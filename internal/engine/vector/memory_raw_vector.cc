/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_raw_vector.h"
#include "memory/memoryManager.h"

#include <unistd.h>

#include <algorithm>
#include <functional>
#include <random>

using std::string;
namespace vearch {

MemoryRawVector::MemoryRawVector(VectorMetaInfo *meta_info,
                                 const StoreParams &store_params,
                                 bitmap::BitmapManager *docids_bitmap,
                                 StorageManager *storage_mgr, int cf_id)
    : RawVector(meta_info, docids_bitmap, store_params) {
  segments_ = nullptr;
  nsegments_ = 0;
  storage_mgr_ = storage_mgr;
  cf_id_ = cf_id;
  segment_size_ = store_params.segment_size;
  vector_byte_size_ = meta_info->DataSize() * meta_info->Dimension();
  curr_idx_in_seg_ = 0;
  compact_if_need_ = true;
  compact_ratio_ = 0.3;
  segment_nums_ = nullptr;
  segment_deleted_nums_ = nullptr;
}

MemoryRawVector::~MemoryRawVector() {
  for (int i = 0; i < nsegments_; i++) {
    CHECK_DELETE_ARRAY(segments_[i]);
  }
  CHECK_DELETE_ARRAY(segments_);
  if (segment_deleted_nums_) {
    CHECK_DELETE_ARRAY(segment_deleted_nums_);
  }
  if (segment_nums_) {
    CHECK_DELETE_ARRAY(segment_nums_);
  }
}

Status MemoryRawVector::Load(int64_t vec_num, int64_t &disk_vec_num) {
  std::unique_ptr<rocksdb::Iterator> it = storage_mgr_->NewIterator(cf_id_);
  string start_key = utils::ToRowKey(0);
  it->Seek(rocksdb::Slice(start_key));
  string end_key = utils::ToRowKey(vec_num);

  int64_t n_load = 0;
  for (; it->Valid() && it->key().compare(end_key) < 0; it->Next()) {
    rocksdb::Slice current_key = it->key();
    int64_t vid = utils::FromRowKey(current_key.ToString());
    if (vid < 0) {
      LOG(ERROR) << desc_ << "parse row key failed, key=" << current_key.ToString();
      continue;
    }
    rocksdb::Slice value = it->value();
    AddToMem(vid, (uint8_t *)value.data_, VectorByteSize());
    n_load++;
  }

  MetaInfo()->size_ = vec_num;
  disk_vec_num = n_load;
  LOG(INFO) << desc_ << "memory raw vector want to load [" << vec_num
            << "], real load [" << n_load << "]";

  return Status::OK();
}

int MemoryRawVector::GetDiskVecNum(int64_t &vec_num) {
  int64_t origin_vec_num = vec_num;
  if (vec_num <= 0) return 0;
  int disk_vec_num = vec_num - 1;
  string key, value;
  for (int64_t i = disk_vec_num; i >= 0; --i) {
    key = utils::ToRowKey(i);
    Status s = storage_mgr_->Get(cf_id_, key, value);
    if (s.ok()) {
      vec_num = i + 1;
      LOG(INFO) << desc_ << "in the disk rocksdb vec_num=" << vec_num
                << ", origin_vec_num=" << origin_vec_num;
      return 0;
    }
  }
  vec_num = 0;
  LOG(INFO) << desc_ << "in the disk rocksdb vec_num=" << vec_num
            << ", origin_vec_num=" << origin_vec_num;
  return 0;
}

int MemoryRawVector::InitStore(std::string &vec_name) {
  segments_ = new (std::nothrow) uint8_t *[kMaxSegments];
  if (segments_ == nullptr) {
    LOG(ERROR) << desc_
               << "malloc new segment failed, segment size=" << segment_size_;
    return -1;
  }
  std::fill_n(segments_, kMaxSegments, nullptr);

  segment_deleted_nums_ =
      new (std::nothrow) std::atomic<uint32_t>[kMaxSegments];
  if (segment_deleted_nums_ == nullptr) {
    LOG(ERROR) << desc_
               << "malloc new segment failed, segment size=" << segment_size_;
    return -2;
  }
  std::fill_n(segment_deleted_nums_, kMaxSegments, 0);

  segment_nums_ = new (std::nothrow) std::atomic<uint32_t>[kMaxSegments];
  if (segment_nums_ == nullptr) {
    LOG(ERROR) << desc_
               << "malloc new segment failed, segment size=" << segment_size_;
    return -3;
  }
  std::fill_n(segment_nums_, kMaxSegments, 0);

  if (ExtendSegments()) return -4;

  LOG(INFO) << desc_ << "init memory raw vector success! vector byte size="
            << vector_byte_size_ << ", " + meta_info_->Name();
  return 0;
}

int MemoryRawVector::AddToStore(uint8_t *v, int len) {
  AddToMem(meta_info_->Size(), v, vector_byte_size_);
  if (WithIO()) {
    storage_mgr_->Add(cf_id_, meta_info_->Size(), v, VectorByteSize());
  }
  return 0;
}

int MemoryRawVector::DeleteFromStore(int64_t vid) {
  if (WithIO()) {
    std::string key = utils::ToRowKey(vid);
    Status s = storage_mgr_->Delete(cf_id_, key);
    if (!s.ok()) {
      LOG(ERROR) << desc_ << "rocksdb delete error:" << s.ToString()
                 << ", key=" << key;
      return -1;
    }
  }
  segment_deleted_nums_[vid / segment_size_] += 1;
  return 0;
}

int MemoryRawVector::AddToMem(int64_t vid, uint8_t *v, int len) {
  assert(len == vector_byte_size_);
  // load will not add consecutive
  while (vid / segment_size_ >= nsegments_) {
    if (ExtendSegments()) return -2;
  }
  memcpy((void *)(segments_[vid / segment_size_] +
                  (vid % segment_size_) * vector_byte_size_),
         (void *)v, vector_byte_size_);
  segment_nums_[vid / segment_size_] += 1;
  return 0;
}

int MemoryRawVector::ExtendSegments() {
  if (nsegments_ >= kMaxSegments) {
    LOG(ERROR) << this->desc_ << "segment number can't be > " << kMaxSegments;
    return -1;
  }

  if (MemoryManager::GetInstance().CheckMemoryUsageExceed(segment_size_ * vector_byte_size_)) {
    LOG(ERROR) << this->desc_ << "current memory usage exceed limit";
    return -1;
  }

  segments_[nsegments_] =
      new (std::nothrow) uint8_t[segment_size_ * vector_byte_size_];
  if (segments_[nsegments_] == nullptr) {
    LOG(ERROR) << this->desc_
               << "malloc new segment failed, segment num=" << nsegments_
               << ", segment size=" << segment_size_;
    return -1;
  }
  curr_idx_in_seg_ = 0;
  ++nsegments_;
  LOG(DEBUG) << desc_ << "extend segment success! nsegments=" << nsegments_;
  return 0;
}

int MemoryRawVector::GetVectorHeader(int64_t start, int n, ScopeVectors &vecs,
                                     std::vector<int> &lens) {
  if (start + n > meta_info_->Size()) return -1;

  while (n) {
    uint8_t *vec = segments_[start / segment_size_] +
                   (size_t)start % segment_size_ * vector_byte_size_;
    int len = segment_size_ - start % segment_size_;
    if (len > n) len = n;

    bool deletable = false;
    vecs.Add(vec, deletable);
    lens.push_back(len);
    start += len;
    n -= len;
  }
  return 0;
}

int MemoryRawVector::GetRandomTrainVectors(int num, ScopeVectors &vecs,
                                            size_t &n_get,
                                            size_t &valid_count) {
  size_t total = meta_info_->Size();

  // Use Reservoir Sampling (Algorithm R) to select up to `num` random
  // non-deleted vectors in a single pass with O(num) memory.
  // This avoids the O(valid_count) memory overhead of collecting all
  // valid IDs first, which would be ~8GB for 1B docs.
  //
  // Correctness: each valid vector has exactly num/valid_count
  // probability of being in the final reservoir, ensuring uniformity.
  std::vector<int64_t> reservoir;
  reservoir.reserve(num);
  std::mt19937 rng(std::random_device{}());
  size_t seen = 0;

  for (int64_t vid = 0; vid < (int64_t)total; ++vid) {
    if (docids_bitmap_->Test(vid)) continue;  // skip deleted

    if (reservoir.size() < (size_t)num) {
      reservoir.push_back(vid);
    } else {
      std::uniform_int_distribution<size_t> dist(0, seen);
      size_t r = dist(rng);
      if (r < (size_t)num) reservoir[r] = vid;
    }
    ++seen;
  }

  valid_count = seen;
  n_get = reservoir.size();
  if (n_get == 0) {
    LOG(ERROR) << desc_ << "no valid vectors for training";
    return -1;
  }

  // Copy selected vectors into a contiguous memory block.
  // For MemoryRawVector the data is already in memory segments,
  // so we can memcpy directly — this is fast and produces the
  // contiguous layout that faiss::Index::train() expects.
  int dimension = meta_info_->Dimension();
  size_t byte_size = (size_t)dimension * data_size_ * n_get;
  uint8_t *train_vecs = new uint8_t[byte_size];
  utils::ScopeDeleter<uint8_t> del_train_vecs(train_vecs);

  for (size_t i = 0; i < n_get; ++i) {
    int64_t vid = reservoir[i];
    const uint8_t *src = GetFromMem(vid);
    if (src == nullptr) {
      LOG(ERROR) << desc_ << "GetFromMem returned null for vid=" << vid;
      return -2;
    }
    memcpy(train_vecs + i * vector_byte_size_, src, vector_byte_size_);
  }

  del_train_vecs.release();
  vecs.Add(train_vecs, true);
  return 0;
}

int MemoryRawVector::UpdateToStore(int64_t vid, uint8_t *v, int len) {
  if (vid >= meta_info_->Size() || vid < 0) {
    return -1;
  }
  if (docids_bitmap_->Test(vid)) {
    return -2;
  }
  memcpy((void *)(segments_[vid / segment_size_] +
                  (size_t)vid % segment_size_ * vector_byte_size_),
         (void *)v, vector_byte_size_);
  if (WithIO()) {
    storage_mgr_->Add(cf_id_, vid, v, VectorByteSize());
  }
  return 0;
}

int MemoryRawVector::GetVector(int64_t vid, const uint8_t *&vec,
                               bool &deletable) const {
  if (vid >= meta_info_->Size() || vid < 0) {
    return -1;
  }
  if (docids_bitmap_->Test(vid)) {
    vec = nullptr;
    deletable = false;
    return -2;
  }
  vec = segments_[vid / segment_size_] +
        (size_t)vid % segment_size_ * vector_byte_size_;

  deletable = false;
  return 0;
}

uint8_t *MemoryRawVector::GetFromMem(int64_t vid) const {
  return segments_[vid / segment_size_] +
         (size_t)vid % segment_size_ * vector_byte_size_;
}

bool MemoryRawVector::Compactable(int segment_no) {
  return (float)segment_deleted_nums_[segment_no] / segment_nums_[segment_no] >=
         compact_ratio_;
}

void FreeOldPtr(uint8_t *temp) {
  delete[] temp;
  temp = nullptr;
}

Status MemoryRawVector::Compact() {
  // only compact sealed segments
  for (int i = 0; i < nsegments_ - 1; i++) {
    if (Compactable(i)) {
      uint8_t *new_segment =
          new (std::nothrow) uint8_t[segment_size_ * vector_byte_size_];
      if (new_segment == nullptr) {
        LOG(ERROR) << desc_ << "malloc new segment failed, segment size="
                   << segment_size_;
        return Status::ParamError(desc_ + "malloc new segment failed");
      }
      int new_idx = 0;
      for (uint32_t j = 0; j < segment_nums_[i]; j++) {
        if (!docids_bitmap_->Test(i * segment_size_ + j)) {
          memcpy(new_segment + j * vector_byte_size_,
                 segments_[i] + j * vector_byte_size_, vector_byte_size_);
          new_idx++;
        }
      }
      uint8_t *old_segment = segments_[i];
      segments_[i] = new_segment;
      uint32_t old_segment_num = segment_nums_[i];
      uint32_t old_segment_deleted_num = segment_deleted_nums_[i];
      segment_deleted_nums_[i] = 0;
      segment_nums_[i] = new_idx;

      LOG(INFO) << desc_ << "compact segment=" << i << ", new_idx=" << new_idx
                << ", old_idx=" << old_segment_num
                << ", deleted_num=" << old_segment_deleted_num;

      // delay free
      std::function<void(uint8_t *)> func_free =
          std::bind(&FreeOldPtr, std::placeholders::_1);
      utils::AsyncWait(10000, func_free, old_segment);
    } else {
      LOG(INFO) << desc_ << "segment=" << i
                << " no need to compact, num=" << segment_nums_[i]
                << ", deleted_num=" << segment_deleted_nums_[i];
    }
  }
  return Status::OK();
}

}  // namespace vearch
