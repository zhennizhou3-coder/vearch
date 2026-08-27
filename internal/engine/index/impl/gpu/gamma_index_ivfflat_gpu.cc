/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_index_ivfflat_gpu.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexShards.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/impl/IndexUtils.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/invlists/InvertedLists.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/utils.h>

#include "index/index_io.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "c_api/gamma_api.h"
#include "common/gamma_common_data.h"
#include "util/bitmap.h"
#include "util/utils.h"

using faiss::idx_t;
using std::string;
using std::vector;

namespace vearch {
namespace gpu {

namespace {
const int kMaxBatch = 512;   // max search batch num (optimized from 200)
const int kMaxReqNum = 512;  // max request num (optimized from 200)
const int kMaxRecallNum = faiss::gpu::getMaxKSelection();// max recall num or max nprobe
}  // namespace

REGISTER_INDEX(GPU_IVFFLAT, GammaIVFFlatGPUIndex)

GammaIVFFlatGPUIndex::GammaIVFFlatGPUIndex()
    : GammaGPUSearchBase<GammaIVFFlatIndex, IVFFlatGPURetrievalParameters>() {
  nlist_ = 2048;
  nprobe_ = 80;
  vectors_added_since_last_log_ = 0;
}

GammaIVFFlatGPUIndex::~GammaIVFFlatGPUIndex() {
  // Base class destructor will handle cleanup
}

Status GammaIVFFlatGPUIndex::Init(const std::string &model_parameters,
                                  int training_threshold) {
  IVFFlatGPUModelParams params;
  if (!model_parameters.empty()) {
    Status status = params.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
  }

  LOG(INFO) << params.ToString();

  int d = vector_->MetaInfo()->Dimension();
  this->d_ = d;
  this->nlist_ = params.ncentroids;
  this->nprobe_ = params.nprobe;
  this->metric_type_ = params.metric_type;

  if (training_threshold) {
    training_threshold_ = training_threshold;
  } else {
    training_threshold_ = nlist_ * max_points_per_centroid;
  }
  // Call base class initialization
  return GammaGPUIndexBase<GammaIVFFlatIndex>::Init(model_parameters,
                                                    training_threshold);
}

RetrievalParameters *GammaIVFFlatGPUIndex::Parse(
    const std::string &parameters) {
  if (parameters.empty()) {
    return new IVFFlatGPURetrievalParameters();
  }

  nlohmann::json j;
  try {
    j = nlohmann::json::parse(parameters);
  } catch (const nlohmann::json::parse_error &e) {
    LOG(ERROR) << "failed to parse IVFFLAT GPU retrieval parameters: "
               << e.what();
    return nullptr;
  }

  std::string metric_type;
  IVFFlatGPURetrievalParameters *retrieval_params =
      new IVFFlatGPURetrievalParameters();

  if (j.contains("metric_type")) {
    metric_type = j.value("metric_type", "");
    if (strcasecmp("L2", metric_type.c_str()) == 0) {
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    } else if (strcasecmp("InnerProduct", metric_type.c_str()) == 0) {
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
    } else if (!metric_type.empty()) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }
  } else {
    retrieval_params->SetDistanceComputeType(metric_type_);
  }

  int nprobe;
  if (j.contains("nprobe")) {
    nprobe = j.value("nprobe", 0);
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }

  return retrieval_params;
}

faiss::Index *GammaIVFFlatGPUIndex::CreateGPUIndex() {
  int num_gpus = faiss::gpu::getNumDevices();
  LOG(INFO) << "number of GPUs available: " << num_gpus;

  vector<int> devs;
  for (int i = 0; i < num_gpus; ++i) {
    devs.push_back(i);
  }

  if (resources_.size() == 0) {
    for (int i : devs) {
      auto res = new faiss::gpu::StandardGpuResources;
      res->getResources()->initializeForDevice(i);
      res->setTempMemory((size_t)1536 * 1024 * 1024);  // 1.5 GiB
      resources_.push_back(res);
    }
  }

  std::vector<faiss::Index *> gpu_indexes;
  for (int i = 0; i < num_gpus; i++) {
    faiss::gpu::GpuIndexIVFFlatConfig config;
    config.device = i;

    auto gpu_index = new faiss::gpu::GpuIndexIVFFlat(
        resources_[i], d_, nlist_, (faiss::MetricType)metric_type_, config);
    gpu_indexes.push_back(gpu_index);
  }

  // faiss::IndexShards *multi_gpu_index = new faiss::IndexShards(d_, true);
  faiss::IndexShards *multi_gpu_index = new faiss::IndexShards(d_);
  multi_gpu_index->successive_ids = false;
  multi_gpu_index->own_indices = true;
  for (auto *idx : gpu_indexes) {
    multi_gpu_index->add_shard(idx);
  }
  faiss::Index *gpu_index = multi_gpu_index;
  return gpu_index;
}

int GammaIVFFlatGPUIndex::CreateSearchThread() {
  auto func_search = std::bind(&GammaIVFFlatGPUIndex::GPUThread, this);
  gpu_threads_.push_back(std::thread(func_search));
  gpu_threads_.back().detach();
  return 0;
}

int GammaIVFFlatGPUIndex::Indexing() {
  std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);

  LOG(INFO) << "GPU indexing";

  if (is_trained_) {
    is_trained_ = false;
    delete gpu_index_;
    indexed_count_ = 0;
  }

  if (!is_trained_) {
    gpu_index_ = CreateGPUIndex();
    {
      int64_t num = ComputeIVFTrainingNum(nlist_);
      if (num <= 0) return num;

      std::unique_ptr<const uint8_t[]> train_data;
      size_t num_got = 0;
      int ret = GetTrainingVectors(num, train_data, num_got);
      if (ret != 0) return ret;
      const uint8_t *train_raw_vec = train_data.get();
      LOG(INFO) << "train vector wanted num=" << num << ", real num=" << num_got;

      gpu_index_->train(num_got, reinterpret_cast<const float *>(train_raw_vec));
    }
    is_trained_ = true;
  }

  if (gpu_threads_.size() == 0) {
    CreateSearchThread();
  }

  LOG(INFO) << "GPU indexed.";
  return 0;
}

bool GammaIVFFlatGPUIndex::Add(int n, const uint8_t *vec) {
  std::vector<long> new_keys;
  std::vector<uint8_t> new_codes;
  size_t code_size = d_ * sizeof(float);
  long vid = indexed_count_;
  int n_add = 0;
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);

  for (int i = 0; i < n; i++) {
    if (raw_vec->Bitmap()->Test(vid + i)) {
      continue;
    }
    uint8_t *code = (uint8_t *)vec + code_size * i;
    new_keys.push_back(vid + i);
    size_t ofs = new_codes.size();
    new_codes.resize(ofs + code_size);
    memcpy((void *)(new_codes.data() + ofs), (void *)code, code_size);
    n_add +=1;
  }

  std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);

  if (start_docid_ != indexed_count_) {
    return false;
  }

  gpu_index_->add_with_ids(n_add, reinterpret_cast<const float *>(new_codes.data()), new_keys.data());
  vectors_added_since_last_log_ += n;
  if (vectors_added_since_last_log_ >= ADD_COUNT_THRESHOLD) {
    LOG(DEBUG) << "GPU indexed count: " << indexed_count_;
    vectors_added_since_last_log_ = 0;
  }
  return true;
}

int GammaIVFFlatGPUIndex::GPUThread() {
  float *xx = new float[kMaxBatch * d_ * kMaxReqNum];
  long *label = new long[kMaxBatch * kMaxRecallNum * kMaxReqNum];
  float *dis = new float[kMaxBatch * kMaxRecallNum * kMaxReqNum];

  thread_local std::vector<int> batch_offsets;
  thread_local std::vector<int> result_offsets;
  batch_offsets.reserve(kMaxBatch);
  result_offsets.reserve(kMaxBatch);

  while (!b_exited_) {
    int size = 0;
    GPUSearchItem *items[kMaxBatch];

    while (size == 0 && !b_exited_) {
      size = search_queue_.wait_dequeue_bulk_timed(items, kMaxBatch, 100);
    }

    if (size > 1) {
      std::unordered_map<int, std::vector<int>> nprobe_map;
      nprobe_map.reserve(8);

      for (int i = 0; i < size; ++i) {
        nprobe_map[items[i]->nprobe_].emplace_back(i);
      }

      for (auto &nprobe_ids : nprobe_map) {
        if (nprobe_ids.second.empty()) continue;

        // Pre-calculate total vectors and max k to reduce redundant computation
        int recallnum = 0, total = 0;
        batch_offsets.clear();
        result_offsets.clear();

        int data_offset = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          recallnum = std::max(recallnum, items[idx]->k_);
          total += items[idx]->n_;
          batch_offsets.push_back(data_offset);
          data_offset += d_ * items[idx]->n_;
        }

        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          const size_t copy_size = d_ * sizeof(float) * items[idx]->n_;
          std::memcpy(xx + batch_offsets[j], items[idx]->x_, copy_size);
        }

        {
          std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
          if (gpu_index_ == nullptr || b_exited_) {
            LOG(WARNING) << "GPU index is null or exiting";
            // Notify all items in this batch
            for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
              items[nprobe_ids.second[j]]->Notify();
            }
            continue;
          }

          auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
          if (indexShards != nullptr) {
            for (int j = 0; j < indexShards->count(); ++j) {
              auto ivfflat = dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(
                  indexShards->at(j));
              if (ivfflat != nullptr) ivfflat->nprobe = nprobe_ids.first;
            }
          }

          try {
            gpu_index_->search(total, xx, recallnum, dis, label);
          } catch (const std::exception &e) {
            LOG(ERROR) << "GPU batch search failed: " << e.what();
            // Notify all items even on failure
            for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
              items[nprobe_ids.second[j]]->Notify();
            }
            continue;
          }
        }

        int result_offset = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          const size_t dis_size = sizeof(float) * items[idx]->n_ * items[idx]->k_;
          const size_t label_size = sizeof(long) * items[idx]->n_ * items[idx]->k_;

          std::memcpy(items[idx]->dis_, dis + result_offset, dis_size);
          std::memcpy(items[idx]->label_, label + result_offset, label_size);
          result_offset += recallnum * items[idx]->n_;

          // Notify immediately after copying results for this item
          items[idx]->Notify();
        }
      }
    } else if (size == 1) {
      try {
        std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
        if (gpu_index_ == nullptr || b_exited_) {
          LOG(WARNING) << "GPU index is null or exiting";
          items[0]->Notify();
          continue;
        }

        auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
        if (indexShards != nullptr) {
          for (int j = 0; j < indexShards->count(); ++j) {
            auto ivfflat = dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(
                indexShards->at(j));
            if (ivfflat != nullptr) ivfflat->nprobe = items[0]->nprobe_;
          }
        }

        gpu_index_->search(items[0]->n_, items[0]->x_, items[0]->k_,
                           items[0]->dis_, items[0]->label_);
      } catch (const std::exception &e) {
        LOG(ERROR) << "GPU search failed: " << e.what();
      }
      items[0]->Notify();
    }
  }

  delete[] xx;
  delete[] label;
  delete[] dis;
  LOG(INFO) << "GPU thread exit";
  return 0;
}

int GammaIVFFlatGPUIndex::Search(RetrievalContext *retrieval_context, int n,
                                 const uint8_t *x, int k, float *distances,
                                 long *labels) {
  return CommonSearch(retrieval_context, n, x, k, distances, labels, nprobe_,
                      nlist_, false);  // IVFFLAT doesn't need rerank
}

IVFFlatGPURetrievalParameters *
GammaIVFFlatGPUIndex::CreateDefaultRetrievalParams(int default_nprobe) {
  return new IVFFlatGPURetrievalParameters(default_nprobe, metric_type_);
}

int GammaIVFFlatGPUIndex::GetRecallNum(IVFFlatGPURetrievalParameters *params,
                                       int k, bool enable_rerank) {
  // For IVFFLAT, we typically don't have a separate recall_num parameter
  // We use k directly as the recall number
  return k;
}

int GammaIVFFlatGPUIndex::GetNprobe(IVFFlatGPURetrievalParameters *params,
                                    int default_nprobe, size_t nlist) {
  if (params->Nprobe() > 0 && (size_t)params->Nprobe() <= nlist &&
      params->Nprobe() <= kMaxRecallNum) {
    return params->Nprobe();
  } else {
    LOG(WARNING) << "Error nprobe for search, so using default value: "
                 << default_nprobe;
    return default_nprobe;
  }
}

// Dump: GPU keeps no full on-disk dump (the base class Dump is a no-op), so the
// full path returns OK; training_only bridges the GPU model to host via shard0's
// copyTo → faiss::IndexIVFFlat and writes only the coarse centroids
// (write_ivf_header); IVFFlat has no codebook. Magic "IgFm".
Status GammaIVFFlatGPUIndex::Dump(const std::string &path, bool training_only) {
  if (!training_only) return Status::OK();
  std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
  if (!is_trained_ || gpu_index_ == nullptr) {
    LOG(INFO) << "gamma index is not trained, skip dumping training artifacts";
    return Status::OK();
  }
  auto shards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
  if (shards == nullptr || shards->count() == 0) {
    return Status::IOError("gpu_index_ is not a non-empty IndexShards");
  }
  auto gpu_ivfflat =
      dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(shards->at(0));
  if (gpu_ivfflat == nullptr) {
    return Status::IOError("shard0 is not a GpuIndexIVFFlat");
  }

  faiss::IndexIVFFlat host;
  try {
    gpu_ivfflat->copyTo(&host);  // GPU→host DMA
  } catch (const std::exception &e) {
    LOG(ERROR) << "GpuIndexIVFFlat copyTo failed: " << e.what();
    return Status::IOError(std::string("dump training artifacts failed: ") + e.what());
  }

  faiss::IOWriter *f = new FileIOWriter(path.c_str());
  utils::ScopeDeleter1<FileIOWriter> del((FileIOWriter *)f);
  uint32_t h = faiss::fourcc("IgFm");
  WRITE1(h);
  vearch::write_ivf_header(&host, f);
  // ← no WriteInvertedLists: rebuilt locally by backfill after swap-in.
  LOG(INFO) << "dump training artifacts: nlist=" << host.nlist << ", d=" << host.d;
  return Status::OK();
}

// Load: full load is a no-op (mirror base); training_only reads centroids into a
// host faiss::IndexIVFFlat, gives it an empty (but valid) inverted-list layout,
// then rebuilds gpu_index_ and copyFrom the trained-but-empty host index into
// every shard. indexed_count_/load_num stay 0; backfill re-adds vectors.
Status GammaIVFFlatGPUIndex::Load(const std::string &path, bool training_only,
                                  int64_t &load_num) {
  if (!training_only) {
    load_num = 0;
    return Status::OK();
  }
  if (!utils::file_exist(path)) {
    return Status::IOError("Load training artifacts: file not found: " + path);
  }
  faiss::IOReader *f = new FileIOReader(path.c_str());
  utils::ScopeDeleter1<FileIOReader> del((FileIOReader *)f);
  uint32_t h;
  READ1(h);
  if (h != faiss::fourcc("IgFm")) {
    return Status::IOError("bad magic for GPU IVFFLAT training artifacts");
  }
  faiss::IndexIVFFlat host;
  vearch::read_ivf_header(&host, f, nullptr);
  // read_ivf_header restores d/nlist/quantizer/metric but not the flat code_size
  // or inverted lists. Finalize an EMPTY, valid IVFFlat so copyFrom sees a
  // trained index with zero vectors.
  host.code_size = host.d * sizeof(float);
  host.ntotal = 0;
  host.own_invlists = true;
  host.replace_invlists(
      new faiss::ArrayInvertedLists(host.nlist, host.code_size), true);
  host.is_trained = true;

  std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);
  delete gpu_index_;
  gpu_index_ = CreateGPUIndex();
  auto shards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
  if (shards == nullptr) {
    return Status::IOError("gpu_index_ is not an IndexShards after create");
  }
  try {
    for (int i = 0; i < shards->count(); ++i) {
      auto gpu_ivfflat =
          dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(shards->at(i));
      if (gpu_ivfflat == nullptr) {
        return Status::IOError("shard is not a GpuIndexIVFFlat");
      }
      gpu_ivfflat->copyFrom(&host);  // host→GPU: trained quantizer, 0 vectors
    }
  } catch (const std::exception &e) {
    LOG(ERROR) << "GpuIndexIVFFlat copyFrom failed: " << e.what();
    return Status::IOError(std::string("load training artifacts failed: ") + e.what());
  }
  is_trained_ = true;
  indexed_count_ = 0;  // no vectors yet; backfill rebuilds them
  load_num = 0;
  if (gpu_threads_.size() == 0) {
    CreateSearchThread();
  }
  LOG(INFO) << "load training artifacts: nlist=" << host.nlist << ", d=" << host.d;
  return Status::OK();
}

}  // namespace gpu
}  // namespace vearch
