/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <faiss/ascend/AscendIndexIVFRaBitQ.h>
#include <faiss/IndexIVFRaBitQ.h>
#include <faiss/impl/IDSelector.h>
#include <acl/acl.h>
#include <malloc.h>
#include <memory>

#include "common/gamma_common_data.h"
#include "gamma_index_ivfrabitq_npu.h"
#include "index/index_io.h"

namespace vearch {
namespace npu {

namespace {
const std::vector<int> kSupportedNcentroids = { 1024, 2048, 4096, 8192, 10048, 16384, 32768 };
const std::vector<int> kSupportedDims = { 64, 128, 256, 384, 512, 768, 1024, 2048 };
const char kIndexFileName[] = "ivfrabitq.index";  // dumped index file name
}

struct IVFRABITQNPUModelParams {
  int ncentroids;     // coarse cluster center number
  int nprobe;
  DistanceComputeType metric_type;
  int training_threshold;

  IVFRABITQNPUModelParams() {
    ncentroids = 2048;
    nprobe = 64;
    metric_type = DistanceComputeType::L2;
    training_threshold = 1000000;
  }

  Status Parse(const char *str) {
    nlohmann::json j;
    try {
      j = nlohmann::json::parse(str);
    } catch (const nlohmann::json::parse_error &e) {
      LOG(ERROR) << "failed to parse IVFRABITQ NPU model parameters: " << e.what();
      return Status::ParamError("failed to parse IVFRABITQ model parameters");
    }

    if (j.contains("ncentroids")) {
      int ncentroids = j.value("ncentroids", 0);
      for (int n : kSupportedNcentroids) {
        if (ncentroids <= n) {
          this->ncentroids = n;
          break;
        }
      }
      if (ncentroids > kSupportedNcentroids.back()) {
        this->ncentroids = kSupportedNcentroids.back();
        LOG(WARNING) << "ncentroids " << ncentroids << " exceeds max supported "
                     << kSupportedNcentroids.back() << ", clamped to "
                     << kSupportedNcentroids.back();
      } else if (this->ncentroids != ncentroids) {
        LOG(INFO) << "ncentroids adjusted from " << ncentroids << " to "
                  << this->ncentroids << " to match supported ncentroids";
      }
    }

    if (j.contains("nprobe")) {
      this->nprobe = j.value("nprobe", 0);
    }

    if (j.contains("metric_type")) {
      std::string metric_type_str = j.value("metric_type", "");
      if (!metric_type_str.empty()) {
        if (!strcasecmp("L2", metric_type_str.c_str())) {
          this->metric_type = DistanceComputeType::L2;
        } else if (!strcasecmp("InnerProduct", metric_type_str.c_str())) {
          this->metric_type = DistanceComputeType::INNER_PRODUCT;
        } else if (!strcasecmp("Cosine", metric_type_str.c_str())) {
          this->metric_type = DistanceComputeType::Cosine;
        } else {
          std::string msg =
              std::string("invalid metric_type = ") + metric_type_str;
          LOG(ERROR) << msg;
          return Status::ParamError(msg);
        }
      }
    }

    if (!Validate()) return Status::ParamError("Invalid parameters");
    return Status::OK();
  }

  bool Validate() {
    if (ncentroids <= 0) {
      LOG(ERROR) << "invalid ncentroids = " << ncentroids;
      return false;
    }
    if (std::find(kSupportedNcentroids.begin(), kSupportedNcentroids.end(),
                  ncentroids) == kSupportedNcentroids.end()) {
      std::stringstream ss;
      for (size_t i = 0; i < kSupportedNcentroids.size(); ++i) {
        if (i) ss << ", ";
        ss << kSupportedNcentroids[i];
      }
      LOG(ERROR) << "invalid ncentroids = " << ncentroids
                 << ", must be one of [" << ss.str() << "]";
      return false;
    }
    if (nprobe <= 0) {
      LOG(ERROR) << "invalid nprobe = " << nprobe;
      return false;
    }
    if (nprobe > ncentroids) {
      LOG(ERROR) << "nprobe should less than ncentroids, nprobe = " << nprobe
                 << ", ncentroids = " << ncentroids;
      return false;
    }
    // Ascend ivf-rabitq index only supports L2 metric type.
    if (metric_type != DistanceComputeType::L2) {
      LOG(ERROR) << "NPU_IVFRABITQ only supports metric_type=L2";
      return false;
    }
    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids=" << ncentroids << ", ";
    ss << "nprobe=" << nprobe << ", ";
    ss << "metric_type=" << (int)metric_type << ", ";
    ss << "training_threshold=" << training_threshold;
    return ss.str();
  }
};

REGISTER_INDEX(NPU_IVFRABITQ, GammaIVFRABITQNPUIndex)

GammaIVFRABITQNPUIndex::GammaIVFRABITQNPUIndex()
  : GammaNPUIndexBase<IVFRABITQNPURetrievalParameters>() {
  nlist_ = 2048;
  nprobe_ = 80;
  updated_num_ = 0;
  vectors_added_since_last_log_ = 0;
}

GammaIVFRABITQNPUIndex::~GammaIVFRABITQNPUIndex() {
  // Base class destructor will handle cleanup
}

Status GammaIVFRABITQNPUIndex::Init(const std::string &model_parameters,
                                int training_threshold) {
  IVFRABITQNPUModelParams ivfrabitq_param;
  if (!model_parameters.empty()) {
    Status status = ivfrabitq_param.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
  }
  LOG(INFO) << ivfrabitq_param.ToString();
  int d = vector_->MetaInfo()->Dimension();
  if (std::find(kSupportedDims.begin(), kSupportedDims.end(), d) == kSupportedDims.end()) {
    std::stringstream ss;
    for (size_t i = 0; i < kSupportedDims.size(); ++i) {
      if (i) ss << ", ";
      ss << kSupportedDims[i];
    }
    std::string msg = std::string("invalid vector dimension = ") +
                      std::to_string(d) + ", must be one of [" + ss.str() + "]";
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }
  this->d_ = d;

  this->nlist_ = ivfrabitq_param.ncentroids;
  this->nprobe_ = ivfrabitq_param.nprobe;

  metric_type_ = ivfrabitq_param.metric_type;
  if (training_threshold) {
    training_threshold_ = training_threshold;
  } else {
    training_threshold_ = nlist_ * max_points_per_centroid;
  }
  npu_index_.reset(CreateNPUIndex());
  if (npu_index_ == nullptr) {
    std::string msg = std::string("create ascend npu index failed ");
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }

  // Call base class initialization
  return GammaNPUIndexBase<IVFRABITQNPURetrievalParameters>::Init(
      model_parameters, training_threshold_);
}

RetrievalParameters *GammaIVFRABITQNPUIndex::Parse(const std::string &parameters) {
  if (parameters.empty()) {
    return new IVFRABITQNPURetrievalParameters();
  }

  nlohmann::json j;
  try {
    j = nlohmann::json::parse(parameters);
  } catch (const nlohmann::json::parse_error &e) {
    LOG(ERROR) << "failed to parse IVFRABITQ NPU retrieval parameters: "
               << e.what();
    return nullptr;
  }

  std::string metric_type;
  IVFRABITQNPURetrievalParameters *retrieval_params = new IVFRABITQNPURetrievalParameters();

  int recall_num;
  int nprobe;

  // NPU IVFRABITQ only supports L2; mirror Init by forcing L2 here so the
  // rerank/heap path stays consistent with the distances returned by the index.
  retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
  if (j.contains("metric_type")) {
    metric_type = j.value("metric_type", "");
    if (!metric_type.empty() && metric_type != "L2") {
      LOG(WARNING) << "NPU IVFRABITQ only supports L2, ignore metric_type="
                   << metric_type << ", use L2.";
    }
  }
  if (j.contains("recall_num")) {
    recall_num = j.value("recall_num", 0);
    if (recall_num > 0) {
      retrieval_params->SetRecallNum(recall_num);
    }
  }
  if (j.contains("nprobe")) {
    nprobe = j.value("nprobe", 0);
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }
  return retrieval_params;
}

faiss::Index *GammaIVFRABITQNPUIndex::CreateNPUIndex() {
  uint32_t num_npus = 0;
  aclError ret = aclrtGetDeviceCount(&num_npus);
  if (ret != ACL_SUCCESS) {
      LOG(ERROR) << "ACL get device count error with code: " << ret;
      return nullptr;
  }

  LOG(INFO) << "get device count " << num_npus;
  std::vector<int> devs;
  for (int i = 0; i < (int)num_npus; ++i) {
    devs.push_back(i);
  }

  faiss::ascend::AscendIndexIVFRaBitQConfig config =
        faiss::ascend::AscendIndexIVFRaBitQConfig(devs, static_cast<int64_t>(2048)*1024*1024);
  config.useKmeansPP = true;
  config.cp.niter = 25;
  config.cp.min_points_per_centroid = 39;
  config.cp.max_points_per_centroid = 256;
  config.cp.seed = 1234;
  config.cp.spherical = false;

  faiss::Index *npu_index = nullptr;
  try {
    npu_index = new faiss::ascend::AscendIndexIVFRaBitQ(
        d_, (faiss::MetricType)metric_type_, nlist_, config);
  } catch (const std::exception &e) {
    std::stringstream dims_ss, ncentroids_ss;
    for (size_t i = 0; i < kSupportedDims.size(); ++i) {
      if (i) dims_ss << ",";
      dims_ss << kSupportedDims[i];
    }
    for (size_t i = 0; i < kSupportedNcentroids.size(); ++i) {
      if (i) ncentroids_ss << ",";
      ncentroids_ss << kSupportedNcentroids[i];
    }
    LOG(ERROR) << "create AscendIndexIVFRaBitQ failed, d=" << d_
               << ", nlist=" << nlist_
               << " (NPU_IVFRABITQ requires d in {" << dims_ss.str()
               << "} and nlist in {" << ncentroids_ss.str()
               << "}), err=" << e.what();
    return nullptr;
  } catch (...) {
    LOG(ERROR) << "create AscendIndexIVFRaBitQ failed with unknown "
                  "exception, d=" << d_ << ", nlist=" << nlist_;
    return nullptr;
  }

  return npu_index;
}

int GammaIVFRABITQNPUIndex::CreateSearchThread() {
  auto func_search = std::bind(&GammaIVFRABITQNPUIndex::NPUSearchThread, this);

  npu_search_threads_.push_back(std::thread(func_search));
  return 0;
}

int GammaIVFRABITQNPUIndex::Indexing() {
  LOG(INFO) << "NPU indexing";

  if (!is_trained_) {
    std::unique_lock<std::shared_mutex> lock(npu_index_mutex_);

    size_t num = ComputeIVFTrainingNum(nlist_);

    std::unique_ptr<const uint8_t[]> train_data;
    size_t num_got = 0;
    int ret = GetTrainingVectors(num, train_data, num_got);
    if (ret != 0) return ret;
    const uint8_t *train_raw_vec = train_data.get();
    LOG(INFO) << "train vector wanted num=" << num << ", real num=" << num_got;

    try {
      npu_index_->train(num_got,
                        reinterpret_cast<const float *>(train_raw_vec));
    } catch (const std::exception &e) {
      LOG(ERROR) << "AscendIndexIVFRaBitQ train failed, d=" << d_
                 << ", nlist=" << nlist_ << ", n_get=" << num_got
                 << ", err=" << e.what();
      return -1;
    } catch (...) {
      LOG(ERROR) << "AscendIndexIVFRaBitQ train failed with unknown "
                    "exception, d=" << d_ << ", nlist=" << nlist_
                 << ", n_get=" << num_got;
      return -1;
    }
    is_trained_ = true;
  } else {
    LOG(INFO) << "gamma GammaIVFRABITQNPUIndex is already trained, skip indexing";
  }

  if (npu_search_threads_.size() == 0) {
    CreateSearchThread();
  }

  LOG(INFO) << "NPU indexed.";
  return 0;
}

bool GammaIVFRABITQNPUIndex::Add(int n, const uint8_t *vec) {
  if (not is_trained_) {
    return false;
  }

  size_t code_size = d_ * sizeof(float);
  std::vector<long> new_keys;
  std::vector<uint8_t> new_codes;
  new_keys.reserve(n);
  new_codes.resize(static_cast<size_t>(n) * code_size);
  long vid = indexed_count_;
  int n_add = 0;
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);

  for (int i = 0; i < n; i++) {
    if (raw_vec->Bitmap()->Test(vid + i)) {
      continue;
    }
    uint8_t *code = (uint8_t *)vec + code_size * i;
    new_keys.push_back(vid + i);
    memcpy((void *)(new_codes.data() + n_add * code_size), (void *)code,
           code_size);
    n_add += 1;
  }

  // Whole batch consists of deleted vectors: nothing to add. Return success so
  // the framework still advances indexed_count_ over this docid range, instead
  // of calling add_with_ids with an empty buffer.
  if (n_add == 0) {
    return true;
  }

  new_codes.resize(n_add * code_size);

  std::unique_lock<std::shared_mutex> lock(npu_index_mutex_);

  if (start_docid_ != indexed_count_) {
    return false;
  }


  try {
    npu_index_->add_with_ids(
        n_add, reinterpret_cast<const float *>(new_codes.data()),
        new_keys.data());
  } catch (const std::exception &e) {
    LOG(ERROR) << "AscendIndexIVFRaBitQ add_with_ids failed, n_add="
               << n_add << ", err=" << e.what();
    return false;
  } catch (...) {
    LOG(ERROR) << "AscendIndexIVFRaBitQ add_with_ids failed with unknown "
                  "exception, n_add=" << n_add;
    return false;
  }
  vectors_added_since_last_log_ += n_add;

  if (vectors_added_since_last_log_ >= ADD_COUNT_THRESHOLD) {
    LOG(DEBUG) << "NPU indexed count: " << indexed_count_;
    vectors_added_since_last_log_ = 0;
  }
  return true;
}

int GammaIVFRABITQNPUIndex::Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) {
  if (not is_trained_) {
    return 0;
  }

  size_t code_size = d_ * sizeof(float);
  std::vector<long> new_keys;
  std::vector<uint8_t> new_codes;
  new_keys.reserve(ids.size());
  new_codes.resize(ids.size() * code_size);

  int n_update = 0;
  for (size_t i = 0; i < ids.size(); i++) {
    if (ids[i] < 0) {
      LOG(WARNING) << "ivfrabitq update invalid id=" << ids[i];
      continue;
    }
    if (vecs[i] == nullptr) {
      continue;
    }
    const float *vec = reinterpret_cast<const float *>(vecs[i]);
    if (vec == nullptr) {
      continue;
    }

    size_t offset = n_update * code_size;
    new_keys.push_back(ids[i]);
    memcpy((void *)(new_codes.data() + offset), (void *)vec, code_size);

    n_update++;
  }

  if (n_update > 0) {
    new_codes.resize(n_update * code_size);
    std::unique_lock<std::shared_mutex> lock(npu_index_mutex_);
    auto ivf_rabitq =
        dynamic_cast<faiss::ascend::AscendIndexIVFRaBitQ *>(npu_index_.get());
    if (ivf_rabitq == nullptr) {
      LOG(ERROR) << "Update: npu_index_ is not an AscendIndexIVFRaBitQ";
      return -1;
    }
    try {
      ivf_rabitq->update(n_update,
                         reinterpret_cast<const float *>(new_codes.data()),
                         new_keys.data());
    } catch (const std::exception &e) {
      LOG(ERROR) << "AscendIndexIVFRaBitQ update failed, n_update="
                 << n_update << ", err=" << e.what();
      return -1;
    } catch (...) {
      LOG(ERROR) << "AscendIndexIVFRaBitQ update failed with unknown "
                    "exception, n_update=" << n_update;
      return -1;
    }

  }

  updated_num_ += n_update;
  LOG(DEBUG) << "update index success! size=" << ids.size()
             << ", n_update=" << n_update << ", updated_num="
             << updated_num_;

  return 0;
}

int GammaIVFRABITQNPUIndex::Delete(const std::vector<int64_t> &ids) {
  // Logical delete only: rely on docids_bitmap_ (set by Engine::Delete)
  (void)ids;
  return 0;
}

int GammaIVFRABITQNPUIndex::Search(RetrievalContext *retrieval_context, int n,
                               const uint8_t *x, int k, float *distances,
                               int64_t *labels) {
  return CommonSearch(retrieval_context, n, x, k, distances, labels, nprobe_,
                      nlist_, true);
}

int GammaIVFRABITQNPUIndex::NPUSearchThread() {
  std::vector<float> xx;
  std::vector<int64_t> label;
  std::vector<float> dis;

  std::vector<int> batch_offsets;
  std::vector<int> result_offsets;
  batch_offsets.reserve(kMaxBatch);
  result_offsets.reserve(kMaxBatch);

  while (!b_exited_) {
    int size = 0;
    NPUSearchItem *items[kMaxBatch];

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
        // Pre-calculate total vectors and max k to reduce redundant computation
        int recallnum = 0, total = 0;
        batch_offsets.clear();
        result_offsets.clear();

        int data_offset = 0;
        for (const auto idx : nprobe_ids.second) {
          recallnum = std::max(recallnum, items[idx]->k_);
          total += items[idx]->n_;
          batch_offsets.push_back(data_offset);
          data_offset += d_ * items[idx]->n_;
        }

        xx.resize(static_cast<size_t>(total) * d_);
        dis.resize(static_cast<size_t>(total) * recallnum);
        label.resize(static_cast<size_t>(total) * recallnum);

        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          const size_t copy_size = d_ * sizeof(float) * items[idx]->n_;
          std::memcpy(xx.data() + batch_offsets[j], items[idx]->x_, copy_size);
        }

        {
          std::shared_lock<std::shared_mutex> lock(npu_index_mutex_);
          if (npu_index_ == nullptr || b_exited_) {
            LOG(WARNING) << "NPU index is null or exiting";
            for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
              items[nprobe_ids.second[j]]->NotifyFailure(
                  NPUSearchItem::INDEX_UNAVAILABLE);
            }
            continue;
          }

          try {
            auto ivfrabitq = dynamic_cast<faiss::ascend::AscendIndexIVFRaBitQ *>(npu_index_.get());
            if (ivfrabitq == nullptr) {
              LOG(ERROR) << "npu_index_ is not AscendIndexIVFRaBitQ";
              for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
                items[nprobe_ids.second[j]]->NotifyFailure(
                    NPUSearchItem::NPU_ERROR);
              }
              continue;
            }
            ivfrabitq->setNumProbes(nprobe_ids.first);
            npu_index_->search(total, xx.data(), recallnum, dis.data(),
                               label.data());
          } catch (const std::exception &e) {
            LOG(ERROR) << "NPU batch search failed: " << e.what();
            for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
              items[nprobe_ids.second[j]]->NotifyFailure(
                  NPUSearchItem::NPU_ERROR);
            }
            continue;
          }
        }

        int result_offset = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          const int item_n = items[idx]->n_;
          const int item_k = items[idx]->k_;

          // dis/label is laid out as [total rows x recallnum cols]; when
          // item_k < recallnum, copy each row separately to skip the per-row
          // padding instead of treating the block as contiguous.
          for (int r = 0; r < item_n; ++r) {
            std::memcpy(items[idx]->dis_ + r * item_k,
                        dis.data() + result_offset + r * recallnum,
                        sizeof(float) * item_k);
            std::memcpy(items[idx]->label_ + r * item_k,
                        label.data() + result_offset + r * recallnum,
                        sizeof(int64_t) * item_k);
          }
          result_offset += recallnum * item_n;

          // Notify immediately after copying results for this item
          items[idx]->Notify();
        }
      }
    } else if (size == 1) {
      bool failed = false;
      bool index_unavailable = false;
      try {
        std::shared_lock<std::shared_mutex> lock(npu_index_mutex_);
        if (npu_index_ == nullptr || b_exited_) {
          LOG(WARNING) << "NPU index is null or exiting";
          index_unavailable = true;
        } else {
          auto ivfrabitq = dynamic_cast<faiss::ascend::AscendIndexIVFRaBitQ *>(npu_index_.get());
          if (ivfrabitq == nullptr) {
            LOG(ERROR) << "npu_index_ is not AscendIndexIVFRaBitQ";
            failed = true;
          } else {
            ivfrabitq->setNumProbes(items[0]->nprobe_);
            // Use a local query buffer to avoid any in-place mutation of request
            // query memory by downstream NPU search implementation.
            std::vector<float> xq_local(items[0]->n_ * d_);
            std::memcpy(xq_local.data(), items[0]->x_,
                        sizeof(float) * items[0]->n_ * d_);
            npu_index_->search(items[0]->n_, xq_local.data(), items[0]->k_,
                               items[0]->dis_, items[0]->label_);
          }
        }
      } catch (const std::exception &e) {
        LOG(ERROR) << "NPU search failed: " << e.what();
        failed = true;
      }
      if (index_unavailable) {
        items[0]->NotifyFailure(NPUSearchItem::INDEX_UNAVAILABLE);
      } else if (failed) {
        items[0]->NotifyFailure(NPUSearchItem::NPU_ERROR);
      } else {
        items[0]->Notify();
      }
    }
  }

  LOG(INFO) << "NPU thread exit";
  return 0;
}

std::unique_ptr<IVFRABITQNPURetrievalParameters> GammaIVFRABITQNPUIndex::CreateDefaultRetrievalParams(
    int default_nprobe) {
  return std::make_unique<IVFRABITQNPURetrievalParameters>(0, 8, default_nprobe, metric_type_);
}

int GammaIVFRABITQNPUIndex::GetRecallNum(IVFRABITQNPURetrievalParameters *params, int k,
                                     bool enable_rerank) {
  int recall_num = params->RecallNum();

  if (recall_num < k) {
    recall_num = k;
  }

  return recall_num;
}

std::string IVFRaBitQToString(const faiss::IndexIVFRaBitQ *ivfrq) {
  std::stringstream ss;
  ss << "d=" << ivfrq->d << ", ntotal=" << ivfrq->ntotal
     << ", is_trained=" << ivfrq->is_trained
     << ", metric_type=" << ivfrq->metric_type << ", nlist=" << ivfrq->nlist
     << ", nprobe=" << ivfrq->nprobe;

  return ss.str();
}

Status GammaIVFRABITQNPUIndex::Dump(const std::string &dir) {
  if (not is_trained_) {
    LOG(INFO) << "gamma index is not trained, skip dumping";
    return Status::OK();
  }
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_dir = dir + "/" + index_name;
  if (utils::make_dir(index_dir.c_str())) {
    std::string msg = std::string("mkdir error, index dir=") + index_dir;
    LOG(ERROR) << msg;
    return Status::PathNotFound(msg);
  }

  std::unique_lock<std::shared_mutex> lock(npu_index_mutex_);
  int64_t indexed_count = indexed_count_;
  auto index = std::make_unique<faiss::IndexIVFRaBitQ>();
  index->own_fields = true;
  index->own_invlists = true;
  auto ivf_rabitq = dynamic_cast<faiss::ascend::AscendIndexIVFRaBitQ *>(npu_index_.get());
  if (ivf_rabitq == nullptr) {
    std::string msg = "npu_index_ is not AscendIndexIVFRaBitQ, skip dump";
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }

  // Wrap the heavy lifting in try/catch: copyTo (NPU→host DMA + buffer alloc)
  // and write_index (filesystem IO) are external library calls that can throw
  // std::bad_alloc / std::ios_base::failure / ascendfaiss errors. Without
  // this guard a thrown exception calls std::terminate() and crashes the
  // whole PS process. Catch and report cleanly so the next flush cycle can
  // retry.
  try {
    ivf_rabitq->copyTo(index.get());
    lock.unlock();

    std::string index_file = index_dir + "/" + kIndexFileName;
    faiss::IOWriter *f = new FileIOWriter(index_file.c_str());
    utils::ScopeDeleter1<FileIOWriter> del((FileIOWriter *)f);

    faiss::write_index(index.get(), f);
    WRITE1(indexed_count);
  } catch (const std::exception &e) {
    LOG(ERROR) << "AscendIndexIVFRaBitQ Dump failed, index_name="
               << index_name << ", err=" << e.what();
    return Status::IOError(std::string("dump failed: ") + e.what());
  } catch (...) {
    LOG(ERROR) << "AscendIndexIVFRaBitQ Dump failed with unknown exception, "
                  "index_name=" << index_name;
    return Status::IOError("dump failed: unknown exception");
  }

  LOG(INFO) << "dump:" << IVFRaBitQToString(index.get())
            << ", indexed count=" << indexed_count;

  index.reset();
  malloc_trim(0);
  return Status::OK();
}

Status GammaIVFRABITQNPUIndex::Load(const std::string &dir, int64_t &load_num) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_file = dir + "/" + index_name + "/" + kIndexFileName;
  if (!utils::file_exist(index_file)) {
    LOG(INFO) << index_file << " is not existing, skip loading";
    load_num = 0;
    return Status::OK();  // it should train again after load
  }

  // Wrap the IO + deserialization + host→NPU copy in try/catch. Any of
  // faiss::read_index / READ1 / AscendIndexIVFRaBitQ::copyFrom can throw on
  // corrupt files, partial writes from a previous crash, or NPU device
  // errors during boot. An uncaught exception here would abort the PS
  // process during partition recovery and prevent the whole node from
  // coming up.
  try {
    faiss::IOReader *f = new FileIOReader(index_file.c_str());
    utils::ScopeDeleter1<FileIOReader> del((FileIOReader *)f);
    std::unique_ptr<faiss::Index> index(faiss::read_index(f));
    if (index == nullptr) {
      std::string msg =
          std::string("read Ascendivfrabitq index error, index name=") + index_name;
      LOG(ERROR) << msg;
      return Status::IOError(msg);
    }

    LOG(INFO) << "read index success, index name=" << index_name;
    faiss::IndexIVFRaBitQ *loaded_index = dynamic_cast<faiss::IndexIVFRaBitQ *>(index.get());
    if (loaded_index == nullptr) {
      std::string msg =
          std::string("read index error, index name=" ) + index_name;
      LOG(ERROR) << msg;
      return Status::IOError(msg);
    }

    int64_t indexed_vec_count = 0;
    READ1(indexed_vec_count);
    indexed_count_ = indexed_vec_count;
    if (indexed_count_ < 0) {
        std::string msg = std::string("invalid indexed count [") +
                          std::to_string(indexed_count_) + "] vector size [" +
                          std::to_string(vector_->MetaInfo()->size_) + "]";
        LOG(ERROR) << msg;
        return Status::IndexError(msg);
    }
    LOG(INFO) << "load: " << IVFRaBitQToString(loaded_index)
              << ", indexed vector count=" << indexed_count_;

    auto ivf_rabitq = dynamic_cast<faiss::ascend::AscendIndexIVFRaBitQ *>(npu_index_.get());
    if (ivf_rabitq == nullptr) {
      std::string msg = "npu_index_ is not AscendIndexIVFRaBitQ, skip load";
      LOG(ERROR) << msg;
      return Status::IOError(msg);
    }
    ivf_rabitq->copyFrom(loaded_index);
    this->is_trained_ = loaded_index->is_trained;
    assert(this->is_trained_);
    load_num = indexed_count_;
  } catch (const std::exception &e) {
    LOG(ERROR) << "AscendIndexIVFRaBitQ Load failed, index_name="
               << index_name << ", err=" << e.what();
    return Status::IOError(std::string("load failed: ") + e.what());
  } catch (...) {
    LOG(ERROR) << "AscendIndexIVFRaBitQ Load failed with unknown exception, "
                  "index_name=" << index_name;
    return Status::IOError("load failed: unknown exception");
  }

  // After a successful load the NPU index is fully usable, but the search
  // worker thread is only started inside Indexing(). Without this hook the
  // first search after a PS restart fails with "npu index not indexed!" until
  // someone manually triggers /index/forcemerge or writes a document. Start
  // the worker eagerly so search becomes available as soon as Load returns.
  if (is_trained_ && npu_search_threads_.size() == 0) {
    CreateSearchThread();
    LOG(INFO) << "NPU search thread started after Load, index name="
              << index_name;
  }
  return Status::OK();
}

} //namespace npu
} //namespace vearch
