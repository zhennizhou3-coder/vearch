/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/gpu/impl/IndexUtils.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/StandardGpuResources.h>

#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "common/gamma_common_data.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "index/impl/accelerator/filter_utils.h"
#include "index/impl/accelerator/retrieval_params.h"
#include "index/impl/accelerator/search_item.h"
#include "index/impl/gamma_index_flat.h"
#include "index/index_model.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"

namespace vearch {
namespace gpu {

using faiss::gpu::GpuClonerOptions;
using faiss::gpu::GpuMultipleClonerOptions;
using faiss::gpu::StandardGpuResources;

// GPU search item: thin alias over the shared accelerator search item, kept
// for backward compatibility with existing GPU index implementations.
using GPUSearchItem = accelerator::AcceleratorSearchItem;

// Base GPU retrieval parameters. The default `nprobe` value (80) is baked
// in via the template's non-type parameter so derived classes keep their
// historical default-constructor behavior.
using GPURetrievalParametersBase =
    accelerator::AcceleratorRetrievalParams<80>;

/**
 * Base class for GPU index implementations
 */
template <typename CPUIndexType>
class GammaGPUIndexBase : public IndexModel {
 public:
  GammaGPUIndexBase()
      : IndexModel(),
        gpu_index_(nullptr),
        b_exited_(false),
        is_trained_(false),
        d_(0) {}

  virtual ~GammaGPUIndexBase() { Cleanup(); }

  virtual Status Init(const std::string &model_parameters,
                      int training_threshold) override {
    b_exited_ = false;
    gpu_index_ = nullptr;
    return Status::OK();
  }

  virtual int Indexing() override { return 0; }

  virtual int Update(const std::vector<int64_t> &ids,
                     const std::vector<const uint8_t *> &vecs) override {
    return 0;
  }

  virtual int Delete(const std::vector<int64_t> &ids) override { return 0; }

  virtual long GetTotalMemBytes() override { return 0; }

  virtual Status Dump(const std::string &dir) override { return Status::OK(); }

  virtual Status Load(const std::string &index_dir,
                      int64_t &load_num) override {
    return Status::OK();
  }

 protected:
  virtual faiss::Index *CreateGPUIndex() = 0;
  virtual int CreateSearchThread() = 0;
  virtual int GPUThread() = 0;

  void InitGPUResources() {
    int ngpus = faiss::gpu::getNumDevices();
    LOG(INFO) << "number of GPUs available: " << ngpus;

    devices_.clear();
    for (int i = 0; i < ngpus; ++i) {
      devices_.push_back(i);
    }

    std::lock_guard<std::mutex> lock(cpu_mutex_);
    if (resources_.size() == 0) {
      for (int i : devices_) {
        auto res = new StandardGpuResources;
        res->getResources()->initializeForDevice(i);
        res->setTempMemory((size_t)1536 * 1024 * 1024);  // 1.5 GiB
        resources_.push_back(res);
      }
    }
  }

  void Cleanup() {
    std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);
    b_exited_ = true;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    delete gpu_index_;
    gpu_index_ = nullptr;

    for (auto &resource : resources_) {
      delete resource;
      resource = nullptr;
    }
    resources_.clear();
  }

  moodycamel::BlockingConcurrentQueue<GPUSearchItem *> search_queue_;

  // GPU and CPU indices
  faiss::Index *gpu_index_;

  // GPU resources
  std::vector<StandardGpuResources *> resources_;
  std::vector<int> devices_;
  std::vector<std::thread> gpu_threads_;

  // State variables
  bool b_exited_;
  bool is_trained_;
  int d_;
  DistanceComputeType metric_type_;

  // Synchronization
  std::mutex cpu_mutex_;
  std::shared_mutex gpu_index_mutex_;

  int vectors_added_since_last_log_;

  // Constants
  static constexpr int kMaxBatch = 512;
  static constexpr int kMaxReqNum = 512;
  const int kMaxRecallNum = faiss::gpu::getMaxKSelection();
};

}  // namespace gpu
}  // namespace vearch
