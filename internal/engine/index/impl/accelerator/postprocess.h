/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/MetricType.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/distances.h>

#include <cstring>
#include <functional>
#include <vector>

#include "common/gamma_common_data.h"
#include "index/impl/accelerator/filter_utils.h"
#include "index/index_model.h"
#include "util/log.h"
#include "vector/raw_vector.h"

namespace vearch {
namespace accelerator {


template <typename RetrievalParamsType>
inline int ApplyFiltersAndCompute(RetrievalContext *retrieval_context,
                                  RetrievalParamsType *retrieval_params, int n,
                                  int k, const float *xq, int raw_d, int d,
                                  int recall_num, bool rerank,
                                  std::vector<float> &D,
                                  std::vector<long> &I, float *distances,
                                  long *labels, VectorReader *vector_reader) {
  SearchCondition *condition =
      dynamic_cast<SearchCondition *>(retrieval_context);
  if (condition == nullptr) {
    LOG(ERROR) << "ApplyFiltersAndCompute: retrieval_context is not a "
                  "SearchCondition";
    return -1;
  }

  std::vector<enum DataType> range_filter_types(
      condition->range_filters.size());
  std::vector<std::vector<std::string>> all_term_items(
      condition->term_filters.size());

  const bool right_filter =
      (ParseFilters(condition, range_filter_types, all_term_items) == 0);

  // set filter
  auto is_filterable = [&](long vid) -> bool {
    int docid = vid;
    return (retrieval_context->IsValid(vid) == false) ||
           (right_filter &&
            (FilteredByRangeFilter(condition, range_filter_types, docid) ||
             FilteredByTermFilter(condition, all_term_items, docid)));
  };

  using HeapForIP = faiss::CMin<float, faiss::idx_t>;
  using HeapForL2 = faiss::CMax<float, faiss::idx_t>;

  auto init_result = [&](int topk, float *simi, faiss::idx_t *idxi) {
    if (retrieval_params->GetDistanceComputeType() ==
        DistanceComputeType::INNER_PRODUCT) {
      faiss::heap_heapify<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_heapify<HeapForL2>(topk, simi, idxi);
    }
  };

  auto reorder_result = [&](int topk, float *simi, faiss::idx_t *idxi) {
    if (retrieval_params->GetDistanceComputeType() ==
        DistanceComputeType::INNER_PRODUCT) {
      faiss::heap_reorder<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_reorder<HeapForL2>(topk, simi, idxi);
    }
  };

  std::function<void(std::vector<const uint8_t *>)> compute_vec;

  if (rerank == true) {
    compute_vec = [&](std::vector<const uint8_t *> vecs) {
      for (int i = 0; i < n; ++i) {
        const float *xi = xq + i * d;  // query

        float *simi = distances + i * k;
        long *idxi = labels + i * k;
        init_result(k, simi, idxi);

        for (int j = 0; j < recall_num; ++j) {
          long vid = I[i * recall_num + j];
          if (vid < 0) {
            continue;
          }

          if (is_filterable(vid) == true) {
            continue;
          }
          const float *vec =
              reinterpret_cast<const float *>(vecs[i * recall_num + j]);
          float dist = -1;
          if (retrieval_params->GetDistanceComputeType() ==
              DistanceComputeType::INNER_PRODUCT) {
            dist = faiss::fvec_inner_product(xi, vec, raw_d);
          } else {
            dist = faiss::fvec_L2sqr(xi, vec, raw_d);
          }

          if (retrieval_context->IsSimilarScoreValid(dist) == true) {
            if (retrieval_params->GetDistanceComputeType() ==
                DistanceComputeType::INNER_PRODUCT) {
              if (HeapForIP::cmp(simi[0], dist)) {
                faiss::heap_pop<HeapForIP>(k, simi, idxi);
                faiss::heap_push<HeapForIP>(k, simi, idxi, dist, vid);
              }
            } else {
              if (HeapForL2::cmp(simi[0], dist)) {
                faiss::heap_pop<HeapForL2>(k, simi, idxi);
                faiss::heap_push<HeapForL2>(k, simi, idxi, dist, vid);
              }
            }
          }
        }
        reorder_result(k, simi, idxi);
      }  // parallel
    };
  } else {
    compute_vec = [&](std::vector<const uint8_t *> vecs) {
      for (int i = 0; i < n; ++i) {
        float *simi = distances + i * k;
        long *idxi = labels + i * k;
        int idx = 0;
        memset(simi, -1, sizeof(float) * k);
        memset(idxi, -1, sizeof(long) * k);

        for (int j = 0; j < recall_num; ++j) {
          long vid = I[i * recall_num + j];
          if (vid < 0) {
            continue;
          }

          if (is_filterable(vid) == true) {
            continue;
          }

          float dist = D[i * recall_num + j];

          if (retrieval_context->IsSimilarScoreValid(dist) == true) {
            simi[idx] = dist;
            idxi[idx] = vid;
            idx++;
          }
          if (idx >= k) break;
        }
      }
    };
  }

  std::function<int()> compute_dis;

  if (rerank == true) {
    compute_dis = [&]() -> int {
      RawVector *raw_vec = dynamic_cast<RawVector *>(vector_reader);
      if (raw_vec == nullptr) {
        LOG(ERROR) << "ApplyFiltersAndCompute: vector_reader is not a "
                      "RawVector, cannot rerank";
        return -1;
      }
      ScopeVectors scope_vecs;
      if (raw_vec->Gets(I, scope_vecs)) {
        LOG(ERROR) << "get raw vector error!";
        return -1;
      }
      compute_vec(scope_vecs.Get());
      return 0;
    };
  } else {
    compute_dis = [&]() -> int {
      std::vector<const uint8_t *> vecs;
      compute_vec(vecs);
      return 0;
    };
  }

  if (compute_dis() != 0) {
    return -1;
  }

#ifdef PERFORMANCE_TESTING
  if (retrieval_context->GetPerfTool()) {
    retrieval_context->GetPerfTool()->Perf("reorder");
  }
#endif
  return 0;
}

}  // namespace accelerator
}  // namespace vearch
