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

// npu Ascend IVFRABITQ index only support L2 metric type
class IVFRABITQNPURetrievalParameters : public NPURetrievalParametersBase {
 public:
  IVFRABITQNPURetrievalParameters() : NPURetrievalParametersBase(64, DistanceComputeType::L2) {
    recall_num_ = 0;
    qb_ = 8;
  }

  IVFRABITQNPURetrievalParameters(int recall_num, int qb, int nprobe, enum DistanceComputeType type)
      : NPURetrievalParametersBase(nprobe, type) {
    recall_num_ = recall_num;
    qb_ = qb;
  }

  virtual ~IVFRABITQNPURetrievalParameters() {}

  int RecallNum() { return recall_num_; }
  void SetRecallNum(int recall_num) { recall_num_ = recall_num; }
  int Nprobe() { return nprobe_; }
  void SetNprobe(int nprobe) { nprobe_ = nprobe; }
  int Qb() { return qb_; }
  void SetQb(int qb) { qb_ = qb; }

 protected:
  int recall_num_;
  int qb_;
};

class GammaIVFRABITQNPUIndex : public GammaNPUIndexBase<IVFRABITQNPURetrievalParameters> {
 public:
  GammaIVFRABITQNPUIndex();
  virtual ~GammaIVFRABITQNPUIndex() override;

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
  // Implement abstract methods from GammaNPUIndexBase
  faiss::Index *CreateNPUIndex() override;
  int CreateSearchThread() override;
  int NPUSearchThread() override;

  std::unique_ptr<IVFRABITQNPURetrievalParameters> CreateDefaultRetrievalParams(
      int default_nprobe) override;
  int GetRecallNum(IVFRABITQNPURetrievalParameters *params, int k,
                   bool enable_rerank) override;

 private:
  size_t nlist_;
  int nprobe_;
  uint64_t updated_num_;
};

} // namespace npu
} // namespace vearch