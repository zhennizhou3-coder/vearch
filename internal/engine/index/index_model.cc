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

// Decide how many vectors to sample for IVF training.
//
// The PS/write layer (entity.ValidateIndexes) guarantees NEW spaces satisfy
// training_threshold_ >= max(MinTrainingThreshold, ncentroids*39). This engine
// function must additionally tolerate LEGACY spaces persisted before that check
// existed, whose training_threshold_ may sit below ncentroids*39: it trains on
// the configured threshold (only warning about lower quality) instead of
// clamping the sample count up to ncentroids*39, which would demand more vectors
// than such a table holds and make GetTrainingVectors fail the build. It still
// returns -1 when training_threshold_ < ncentroids (k-means cannot run) and
// clamps down above ncentroids*max_points_per_centroid to avoid oversampling.
int64_t IndexModel::ComputeIVFTrainingNum(size_t nlist) const {
  size_t num;
  if ((size_t)training_threshold_ < nlist) {
    // Fewer training points than centroids: k-means cannot run. Return -1 and
    // let the caller abort the build.
    LOG(ERROR) << "training_threshold[" << training_threshold_
               << "] < ncentroids[" << nlist << "], cannot train index.";
    return -1;
  } else if ((size_t)training_threshold_ <
             nlist * vearch::min_points_per_centroid) {
    num = (size_t)training_threshold_;
    LOG(WARNING) << "training_threshold[" << training_threshold_
                 << "] < ncentroids[" << nlist << "] * "
                 << vearch::min_points_per_centroid << ", training on " << num
                 << " vectors, index quality may be lower.";
  } else if ((size_t)training_threshold_ <=
             nlist * vearch::max_points_per_centroid) {
    num = (size_t)training_threshold_;
  } else {
    num = nlist * vearch::max_points_per_centroid;
    LOG(WARNING) << "training_threshold[" << training_threshold_
                 << "] > ncentroids[" << nlist << "] * "
                 << vearch::max_points_per_centroid << ", clamped down to "
                 << num << ".";
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
