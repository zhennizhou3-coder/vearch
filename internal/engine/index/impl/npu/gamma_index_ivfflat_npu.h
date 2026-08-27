/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "gamma_npu_index_base.h"

namespace vearch {
namespace npu {

class IVFFlatNPURetrievalParameters : public NPURetrievalParametersBase {
 public:
  IVFFlatNPURetrievalParameters()
      : NPURetrievalParametersBase(64, DistanceComputeType::L2) {
    parallel_on_queries_ = true;
  }

  IVFFlatNPURetrievalParameters(int nprobe, enum DistanceComputeType type)
      : NPURetrievalParametersBase(nprobe, type) {
    parallel_on_queries_ = true;
  }

  IVFFlatNPURetrievalParameters(int nprobe, bool parallel_on_queries,
                                enum DistanceComputeType type)
      : NPURetrievalParametersBase(nprobe, type),
        parallel_on_queries_(parallel_on_queries) {}

  virtual ~IVFFlatNPURetrievalParameters() {}

  int Nprobe() { return nprobe_; }
  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

  bool ParallelOnQueries() { return parallel_on_queries_; }
  void SetParallelOnQueries(bool parallel_on_queries) {
    parallel_on_queries_ = parallel_on_queries;
  }

 protected:
  bool parallel_on_queries_;
};

class GammaIVFFlatNPUIndex
    : public GammaNPUIndexBase<IVFFlatNPURetrievalParameters> {
 public:
  GammaIVFFlatNPUIndex();
  virtual ~GammaIVFFlatNPUIndex() override;

  Status Init(const std::string &model_parameters,
              int training_threshold) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;
  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;
  int Delete(const std::vector<int64_t> &ids) override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *labels) override;

  Status Dump(const std::string &path, bool training_only) override;
  Status Load(const std::string &path, bool training_only,
              int64_t &load_num) override;

 protected:
  faiss::Index *CreateNPUIndex() override;
  int CreateSearchThread() override;
  int NPUSearchThread() override;

  std::unique_ptr<IVFFlatNPURetrievalParameters> CreateDefaultRetrievalParams(
      int default_nprobe) override;
  int GetRecallNum(IVFFlatNPURetrievalParameters *params, int k,
                   bool enable_rerank) override;

 private:
  size_t nlist_;
  int nprobe_;
  uint64_t updated_num_;
};

}  // namespace npu
}  // namespace vearch
