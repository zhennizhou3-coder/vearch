/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "gamma_gpu_index_base.h"
#include "index/impl/accelerator/postprocess.h"

namespace vearch {
namespace gpu {

/**
 * Common GPU search functionality for different index types
 */
template <typename CPUIndexType, typename GPURetrievalParamsType>
class GammaGPUSearchBase : public GammaGPUIndexBase<CPUIndexType> {
 public:
  using GammaGPUIndexBase<CPUIndexType>::kMaxReqNum;
  using GammaGPUIndexBase<CPUIndexType>::kMaxRecallNum;
  using GammaGPUIndexBase<CPUIndexType>::search_queue_;
  using GammaGPUIndexBase<CPUIndexType>::gpu_threads_;
  using GammaGPUIndexBase<CPUIndexType>::d_;
  using GammaGPUIndexBase<CPUIndexType>::metric_type_;
  using GammaGPUIndexBase<CPUIndexType>::vector_;

 protected:
  /**
   * Common search implementation with filter support
   */
  int CommonSearch(RetrievalContext *retrieval_context, int n, const uint8_t *x,
                   int k, float *distances, long *labels, int default_nprobe,
                   size_t nlist, bool enable_rerank = false) {
    if (gpu_threads_.size() == 0) {
      LOG(ERROR) << "gpu index not indexed!";
      return -1;
    }

    if (n > kMaxReqNum) {
      LOG(ERROR) << "req num [" << n << "] should not larger than ["
                 << kMaxReqNum << "]";
      return -1;
    }

    GPURetrievalParamsType *retrieval_params =
        dynamic_cast<GPURetrievalParamsType *>(
            retrieval_context->RetrievalParams());
    utils::ScopeDeleter1<GPURetrievalParamsType> del_params;
    if (retrieval_params == nullptr) {
      retrieval_params = CreateDefaultRetrievalParams(default_nprobe);
      del_params.set(retrieval_params);
    }

    const float *xq = reinterpret_cast<const float *>(x);
    if (xq == nullptr) {
      LOG(ERROR) << "search feature is null";
      return -1;
    }

    RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
    int raw_d = raw_vec->MetaInfo()->Dimension();
    const float *vec_q = xq;

    // Get recall number and rerank flag
    int recall_num = GetRecallNum(retrieval_params, k, enable_rerank);
    bool rerank = enable_rerank && (recall_num >= k);

    if (recall_num > kMaxRecallNum) {
      LOG(ERROR) << "topK num [" << recall_num << "] should not larger than ["
                 << kMaxRecallNum << "]";
      return -1;
    }

    // Get nprobe
    int nprobe = GetNprobe(retrieval_params, default_nprobe, nlist);

    std::vector<float> D(n * recall_num);
    std::vector<long> I(n * recall_num);

#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("GPUSearch prepare");
    }
#endif

    GPUSearchItem *item =
        new GPUSearchItem(n, vec_q, recall_num, D.data(), I.data(), nprobe);

    search_queue_.enqueue(item);
    item->WaitForDone();
    delete item;

#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("GPU thread");
    }
#endif

    // Apply filters and compute final results
    return accelerator::ApplyFiltersAndCompute(
        retrieval_context, retrieval_params, n, k, xq, raw_d, d_, recall_num,
        rerank, D, I, distances, labels, vector_);
  }

 private:
  // Abstract methods to be implemented by derived classes
  virtual GPURetrievalParamsType *CreateDefaultRetrievalParams(
      int default_nprobe) = 0;
  virtual int GetRecallNum(GPURetrievalParamsType *params, int k,
                           bool enable_rerank) = 0;
  virtual int GetNprobe(GPURetrievalParamsType *params, int default_nprobe,
                        size_t nlist) = 0;
};

}  // namespace gpu
}  // namespace vearch
