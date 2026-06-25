/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "index/index_model.h"

#include "common/gamma_common_data.h"
#include "util/log.h"
#include "vector/raw_vector.h"

size_t IndexModel::ComputeIVFTrainingNum(size_t nlist) const {
  size_t num;
  if ((size_t)training_threshold_ < nlist * vearch::min_points_per_centroid) {
    num = nlist * vearch::min_points_per_centroid;
    LOG(WARNING) << "training_threshold[" << training_threshold_
                 << "] < ncentroids[" << nlist << "] * "
                 << vearch::min_points_per_centroid
                 << ", clamped up to " << num << ".";
  } else if ((size_t)training_threshold_ <=
             nlist * vearch::max_points_per_centroid) {
    num = (size_t)training_threshold_;
  } else {
    num = nlist * vearch::max_points_per_centroid;
    LOG(WARNING) << "training_threshold[" << training_threshold_
                 << "] > ncentroids[" << nlist << "] * "
                 << vearch::max_points_per_centroid
                 << ", clamped down to " << num << ".";
  }
  return num;
}

int IndexModel::GetTrainingVectors(size_t threshold,
                                   std::unique_ptr<const uint8_t[]> &train_data,
                                   size_t &num_got) {
  if (threshold == 0) {
    LOG(ERROR) << "training threshold must be greater than zero";
    return -1;
  }
  vearch::RawVector *raw_vec = dynamic_cast<vearch::RawVector *>(vector_);
  if (raw_vec == nullptr) {
    LOG(ERROR) << "Failed to cast vector_ to RawVector*";
    return -1;
  }
  ScopeVectors scope_vecs;
  size_t valid_count = 0;
  int ret = raw_vec->SampleTrainingVectors(threshold, scope_vecs, num_got,
                                           valid_count);
  if (ret != 0) {
    LOG(ERROR) << "Fail to sample training vectors, ret=" << ret;
    return ret;
  }
  if (valid_count < threshold) {
    LOG(ERROR) << "valid vector count [" << valid_count
               << "] less than training threshold [" << threshold << "]";
    return -1;
  }
  // SampleTrainingVectors must return the whole sample as one contiguous
  // block (a single Add) so that train_data owns all num_got vectors.
  // Guard against a backend handing back multiple chunks: we would
  // otherwise take ownership of only the first one and train() would read
  // past it.
  if (scope_vecs.Size() != 1) {
    LOG(ERROR) << "training vectors must be a single contiguous chunk, got "
               << scope_vecs.Size() << " chunk(s)";
    return -1;
  }
  // Hand the sampled block off to the caller's unique_ptr: clear the local
  // ScopeVectors' delete flag so it does not free the buffer we now own.
  scope_vecs.deletable_[0] = false;
  train_data.reset(scope_vecs.Get(0));
  return 0;
}
