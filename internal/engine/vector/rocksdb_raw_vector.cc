/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "rocksdb_raw_vector.h"

#include <stdio.h>

#include <memory>

#include "rocksdb/table.h"
#include "util/log.h"
#include "util/utils.h"

namespace vearch {

RocksDBRawVector::RocksDBRawVector(VectorMetaInfo *meta_info,
                                   const StoreParams &store_params,
                                   bitmap::BitmapManager *docids_bitmap,
                                   StorageManager *storage_mgr, int cf_id)
    : RawVector(meta_info, docids_bitmap, store_params) {
  storage_mgr_ = storage_mgr;
  cf_id_ = cf_id;
  compact_if_need_ = false;
}

RocksDBRawVector::~RocksDBRawVector() {}

int RocksDBRawVector::GetDiskVecNum(int64_t &vec_num) {
  if (vec_num <= 0) return 0;
  auto max_id_in_disk = vec_num - 1;
  for (auto i = max_id_in_disk; i >= 0; --i) {
    auto result = storage_mgr_->Get(cf_id_, i);
    if (result.first.ok()) {
      vec_num = i + 1;
      LOG(INFO) << desc_ << "in the disk rocksdb vec_num=" << vec_num;
      return 0;
    }
  }
  vec_num = 0;
  LOG(INFO) << desc_ << "in the disk rocksdb vec_num=" << vec_num;
  return 0;
}

Status RocksDBRawVector::Load(int64_t vec_num, int64_t &disk_vec_num) {
  if (vec_num == 0) return Status::OK();
  MetaInfo()->size_ = vec_num;
  disk_vec_num = vec_num;
  LOG(INFO) << desc_ << "rocksdb load success! vec_num=" << vec_num;
  return Status::OK();
}

int RocksDBRawVector::InitStore(std::string &vec_name) {
  block_cache_size_ = (size_t)store_params_.cache_size * 1024 * 1024;

  // std::shared_ptr<rocksdb::Cache> cache =
  //     rocksdb::NewLRUCache(block_cache_size_);
  // table_options_.block_cache = cache;
  // rocksdb::Options options;
  // options.table_factory.reset(NewBlockBasedTableFactory(table_options_));

  // options.IncreaseParallelism();
  // // options.OptimizeLevelStyleCompaction();
  // // create the DB if it's not already present
  // options.create_if_missing = true;

  // std::string db_path = this->root_path_ + "/" + meta_info_->Name();
  // if (!utils::isFolderExist(db_path.c_str())) {
  //   mkdir(db_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  // }

  // // open DB
  // rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db_);
  // if (!s.ok()) {
  //   LOG(ERROR) << "open rocksdb error: " << s.ToString();
  //   return -1;
  // }
  // LOG(INFO) << "rocks raw vector init success! name=" << meta_info_->Name()
  //           << ", block cache size=" << block_cache_size_ << "Bytes";

  return 0;
}

int RocksDBRawVector::GetVector(int64_t vid, const uint8_t *&vec,
                                bool &deletable) const {
  if (vid >= meta_info_->Size() || vid < 0) {
    return 1;
  }
  auto result = storage_mgr_->Get(cf_id_, vid);
  if (!result.first.ok()) {
    LOG(ERROR) << desc_ << "rocksdb get error:" << result.first.ToString()
               << ", vid=" << vid;
    return result.first.code();
  }
  vec = new uint8_t[vector_byte_size_];
  if ((size_t)vector_byte_size_ == result.second.size()) {
    memcpy((void *)vec, result.second.c_str(), vector_byte_size_);
  } else {
    LOG(ERROR) << desc_ << "rocksdb get error: invalid vector size="
               << result.second.size() << ", expect=" << vector_byte_size_;
    return 2;
  }

  deletable = true;
  return 0;
}

int RocksDBRawVector::Gets(const std::vector<int64_t> &vids,
                           ScopeVectors &vecs) const {
  size_t k = vids.size();

  std::vector<std::string> values(k);
  std::vector<rocksdb::Status> statuses =
      storage_mgr_->MultiGet(cf_id_, vids, values);
  if (statuses.size() != k) {
    LOG(ERROR) << desc_
               << "rocksdb multiget error: statuses size=" << statuses.size()
               << ", vids size=" << k;
    return 1;
  }

  for (size_t i = 0; i < k; ++i) {
    if (RequestContext::is_killed()) {
      return 1;
    }

    if (vids[i] < 0) {
      vecs.Add(nullptr, true);
      continue;
    }
    if (!statuses[i].ok()) {
      vecs.Add(nullptr, true);
      continue;
    }
    uint8_t *vector = new uint8_t[vector_byte_size_];
    if ((size_t)vector_byte_size_ != values[i].size()) {
      LOG(ERROR) << desc_ << "rocksdb multiget error: invalid vector size="
                 << values[i].size() << ", expect=" << vector_byte_size_;
      vecs.Add(nullptr, true);
      continue;
    }
    memcpy(vector, values[i].c_str(), vector_byte_size_);
    vecs.Add(vector, true);
  }
  return 0;
}

int RocksDBRawVector::AddToStore(uint8_t *v, int len) {
  return UpdateToStore(meta_info_->Size(), v, len);
}

size_t RocksDBRawVector::GetStoreMemUsage() {
  //   size_t cache_mem = table_options_.block_cache->GetUsage();
  //   std::string index_mem;
  //   db_->GetProperty("rocksdb.estimate-table-readers-mem", &index_mem);
  //   std::string memtable_mem;
  //   db_->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_mem);
  //   size_t pin_mem = table_options_.block_cache->GetPinnedUsage();
  // #ifdef DEBUG
  //   LOG(INFO) << "rocksdb mem usage: block cache=" << cache_mem
  //             << ", index and filter=" << index_mem
  //             << ", memtable=" << memtable_mem
  //             << ", iterators pinned=" << pin_mem;
  // #endif
  //   return cache_mem + pin_mem;
  return 0;
}

int RocksDBRawVector::UpdateToStore(int64_t vid, uint8_t *v, int len) {
  if (v == nullptr || len != meta_info_->Dimension() * meta_info_->DataSize())
    return -1;

  Status s = storage_mgr_->Add(cf_id_, vid, v, this->vector_byte_size_);
  if (!s.ok()) {
    LOG(ERROR) << desc_ << "rocksdb update error:" << s.ToString()
               << ", vid=" << vid;
    return -1;
  }
  return 0;
}

int RocksDBRawVector::DeleteFromStore(int64_t vid) {
  std::string key = utils::ToRowKey(vid);
  Status s = storage_mgr_->Delete(cf_id_, key);
  if (!s.ok()) {
    LOG(ERROR) << desc_ << "rocksdb update error:" << s.ToString()
               << ", vid=" << vid;
    return -1;
  }
  return 0;
}

// get n valid vectors from start
int RocksDBRawVector::SampleTrainingVectors(const size_t num,
                                             ScopeVectors &vecs,
                                             size_t &num_got,
                                             size_t &valid_count) {
  // Reservoir sampling lives in the base class (shared with Memory).
  std::vector<int64_t> reservoir;
  if (SampleTrainingVectorIds(num, reservoir, valid_count) != 0) {
    LOG(ERROR) << desc_ << "no training vectors requested or available";
    num_got = 0;
    return -1;
  }
  num_got = reservoir.size();

  // Batch-fetch from RocksDB.  scope_vecs holds num_got owning buffers
  // until it goes out of scope; the next loop copies into one
  // contiguous block.  Peak overhead is therefore ~2 * vector_byte_size_
  // * num_got, still bounded by the per-index training threshold (typical
  // IVF ~ ncentroids * 256, tens of MB).
  ScopeVectors scope_vecs;
  if (Gets(reservoir, scope_vecs)) {
    LOG(ERROR) << desc_ << "RocksDB MultiGet failed for training vectors";
    return -2;
  }

  size_t byte_size = vector_byte_size_ * num_got;
  std::unique_ptr<uint8_t[]> train_vecs(new (std::nothrow) uint8_t[byte_size]);
  if (!train_vecs) {
    LOG(ERROR) << desc_ << "alloc failed for training block, bytes="
               << byte_size;
    return -2;
  }

  for (size_t i = 0; i < num_got; ++i) {
    if (scope_vecs.Get(i) == nullptr) {
      LOG(ERROR) << desc_ << "Gets returned null for sampled vid="
                 << reservoir[i]
                 << " (bitmap/storage inconsistency)";
      return -2;
    }
    memcpy(train_vecs.get() + i * vector_byte_size_, scope_vecs.Get(i),
           vector_byte_size_);
  }

  vecs.Add(train_vecs.release(), true);
  return 0;
}

}  // namespace vearch
