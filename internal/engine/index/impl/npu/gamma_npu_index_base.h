/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "common/gamma_common_data.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "index/impl/accelerator/filter_utils.h"
#include "index/impl/accelerator/postprocess.h"
#include "index/impl/accelerator/retrieval_params.h"
#include "index/impl/accelerator/search_item.h"
#include "index/impl/gamma_index_flat.h"
#include "index/index_model.h"
#include "monitor/monitor.h"
#include "monitor/scope_metric.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"

namespace vearch {
namespace npu {

// NPU search item: thin alias over the shared accelerator search item, kept
// for backward compatibility with existing NPU index implementations.
using NPUSearchItem = accelerator::AcceleratorSearchItem;

// Base NPU retrieval parameters. The default `nprobe` value (64) is baked
// in via the template's non-type parameter so derived classes keep their
// historical default-constructor behavior.
using NPURetrievalParametersBase =
    accelerator::AcceleratorRetrievalParams<64>;

class RerankSemaphore {
 public:
  RerankSemaphore() {
    unsigned int cores = std::thread::hardware_concurrency();
    count_ = cores > 0 ? static_cast<int>(cores * 3 / 4) : 24;
    if (count_ < 1) count_ = 1;
    LOG(INFO) << "RerankSemaphore init: hardware_concurrency=" << cores
              << ", max concurrent rerank count_=" << count_;
  }

  void Acquire() {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this] { return count_ > 0; });
    --count_;
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mtx_);
    ++count_;
    cv_.notify_one();
  }

 private:
  std::mutex mtx_;
  std::condition_variable cv_;
  int count_ = 1;
};

inline RerankSemaphore &NpuRerankLimiter() {
  static RerankSemaphore sem;
  return sem;
}

template <typename NPURetrievalParamsType>
class GammaNPUIndexBase : public IndexModel {
 public:
  GammaNPUIndexBase()
      : IndexModel(),
        npu_index_(nullptr),
        b_exited_(false),
        is_trained_(false),
        d_(0) {}

  virtual ~GammaNPUIndexBase() { Cleanup(); }

  bool IsTrained() const override { return is_trained_.load(); }

  virtual Status Init(const std::string &model_parameters,
                      int training_threshold) override {
    b_exited_ = false;
    return Status::OK();
  }

  virtual long GetTotalMemBytes() override { return 0; }

  bool IsNPUIndex() const override { return true; }

 protected:
  virtual faiss::Index *CreateNPUIndex() = 0;
  virtual int CreateSearchThread() = 0;
  virtual int NPUSearchThread() = 0;

  /**
   * Common search implementation with filter support.
   */
  int CommonSearch(RetrievalContext *retrieval_context, int n, const uint8_t *x,
                   int k, float *distances, long *labels, int default_nprobe,
                   size_t nlist, bool enable_rerank = false) {
    if (npu_search_threads_.size() == 0) {
      LOG(ERROR) << "npu index not indexed!";
      return -1;
    }

    if (n > kMaxReqNum) {
      LOG(ERROR) << "req num [" << n << "] should not larger than ["
                 << kMaxReqNum << "]";
      return -1;
    }

    SCOPE_ENGINE_METRIC(
        npu_search_latency,
        (monitor::Labels{{"field", vector_->MetaInfo()->Name()}}),
        npu_search_hist_);

    NPURetrievalParamsType *retrieval_params =
        dynamic_cast<NPURetrievalParamsType *>(
            retrieval_context->RetrievalParams());
    std::unique_ptr<NPURetrievalParamsType> default_params;
    if (retrieval_params == nullptr) {
      default_params = CreateDefaultRetrievalParams(default_nprobe);
      retrieval_params = default_params.get();
    }

    const float *xq = reinterpret_cast<const float *>(x);
    if (xq == nullptr) {
      LOG(ERROR) << "search feature is null";
      return -1;
    }

    RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
    int raw_d = raw_vec->MetaInfo()->Dimension();
    const float *vec_q = xq;

    int recall_num = GetRecallNum(retrieval_params, k, enable_rerank);
    bool rerank = enable_rerank && (recall_num >= k);

    if (recall_num > kMaxRecallNum) {
      LOG(ERROR) << "topK num [" << recall_num << "] should not larger than ["
                 << kMaxRecallNum << "]";
      return -1;
    }

    int nprobe = GetNprobe(retrieval_params, default_nprobe, nlist);

    std::vector<float> dis(n * recall_num);
    std::vector<long> label(n * recall_num);

    std::unique_ptr<NPUSearchItem> item(
        new NPUSearchItem(n, vec_q, recall_num, dis.data(), label.data(),
                          nprobe));

    search_queue_.enqueue(item.get());
    int wait_status = item->WaitForDone();
    if (wait_status != NPUSearchItem::OK) {
      LOG(ERROR) << "NPU search item failed, status=" << wait_status;
      return -1;
    }

    // Cap rerank concurrency so the single NPU worker keeps getting scheduled.
    std::optional<RerankScope> gate;
    if (rerank) gate.emplace();
    int rc = accelerator::ApplyFiltersAndCompute(
        retrieval_context, retrieval_params, n, k, xq, raw_d, d_, recall_num,
        rerank, dis, label, distances, labels, vector_);
    return rc;
  }

  void Cleanup() {
    {
      std::unique_lock<std::shared_mutex> lock(npu_index_mutex_);
      b_exited_ = true;
    }
    for (auto &t : npu_search_threads_) {
      if (t.joinable()) t.join();
    }
    npu_search_threads_.clear();

    std::unique_lock<std::shared_mutex> lock(npu_index_mutex_);
    npu_index_.reset();
  }

 protected:
  moodycamel::BlockingConcurrentQueue<NPUSearchItem *> search_queue_;

  // NPU index
  std::unique_ptr<faiss::Index> npu_index_;

  std::vector<std::thread> npu_search_threads_;

  // State variables
  std::atomic<bool> b_exited_;
  std::atomic<bool> is_trained_;
  int d_;
  DistanceComputeType metric_type_;

  // Synchronization
  std::shared_mutex npu_index_mutex_;

  int vectors_added_since_last_log_;

#ifdef ENGINE_METRICS_ENABLED
  // Cached per-instance histogram; resolved once, then hot path only Observes.
  prometheus::Histogram *npu_search_hist_ = nullptr;
#endif

  static constexpr int kMaxBatchItems = 512;
  static constexpr int kMaxReqNum = 512;
  static constexpr int kMaxRecallNum = 16384;

 private:
  class RerankScope {
   public:
    RerankScope() { NpuRerankLimiter().Acquire(); }
    ~RerankScope() { NpuRerankLimiter().Release(); }
    RerankScope(const RerankScope &) = delete;
    RerankScope &operator=(const RerankScope &) = delete;
  };

  int GetNprobe(NPURetrievalParamsType *params, int default_nprobe,
                size_t nlist) {
    if (params->Nprobe() > 0 && (size_t)params->Nprobe() <= nlist) {
      return params->Nprobe();
    } else {
      LOG(WARNING) << "Error nprobe for search, so using default value: "
                   << default_nprobe;
      return default_nprobe;
    }
  }

  virtual std::unique_ptr<NPURetrievalParamsType> CreateDefaultRetrievalParams(
      int default_nprobe) = 0;
  virtual int GetRecallNum(NPURetrievalParamsType *params, int k,
                           bool enable_rerank) = 0;
};

}  // namespace npu
}  // namespace vearch
