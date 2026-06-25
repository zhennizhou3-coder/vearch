
/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <string>

#include "common/gamma_common_data.h"
#include "index/index_model.h"
#include "util/bitmap_manager.h"
#include "util/log.h"
#include "util/status.h"
#include "vector/raw_vector.h"

namespace vearch {

class VectorManager {
 public:
  VectorManager(const VectorStorageType &store_type,
                bitmap::BitmapManager *docids_bitmap,
                const std::string &root_path, std::string &desc);
  ~VectorManager();

  Status DetermineVectorStorageType(std::string index_type,
                                    std::string &store_type_str,
                                    VectorStorageType &store_type);

  Status CreateRawVector(struct VectorInfo &vector_info, std::string index_type,
                         TableInfo &table, RawVector **vec, int cf_id,
                         StorageManager *storage_mgr);

  void DestroyRawVectors();

  Status CreateVectorIndex(const std::string &index_type, const std::string &index_params,
                           RawVector *vec, int training_threshold,
                           bool destroy_vec,
                           std::map<std::string, IndexModel *> &vector_indexes);

  void DestroyVectorIndexes();

  /**
   * @brief Remove vector index for a specific field
   *
   * @param field_name  field name to remove index
   * @return Status
   */
  Status RemoveVectorIndex(const std::string &field_name);

  void DescribeVectorIndexes();

  Status CreateVectorIndexes(
      int training_threshold,
      std::map<std::string, IndexModel *> &vector_indexes);

  void ResetVectorIndexes(
      std::map<std::string, IndexModel *> &rebuild_vector_indexes);

  Status ReCreateVectorIndexes(int training_threshold);

  /**
   * @brief Re-create vector index for a specific (field_name, index_type)
   * pair. This is the per-field counterpart of ReCreateVectorIndexes.
   *
   * @param field_name  field name whose vector index should be rebuilt
   * @param index_type  index type (e.g. "HNSW", "IVFFLAT", "IVFPQ", "FLAT")
   * @param training_threshold  training threshold for the new index
   * @return Status
   */
  Status ReCreateVectorIndex(const std::string &field_name,
                             const std::string &index_type,
                             int training_threshold);

  /**
   * @brief Rebuild (in-place) vector index for a specific (field_name,
   * index_type) pair without dropping the old index first.
   *
   * This is the per-field counterpart of the CreateVectorIndexes +
   * TrainIndex + ResetVectorIndexes sequence used by
   * Engine::RebuildIndex(drop_before_rebuild=0). The new index is created,
   * optionally trained, and then swapped in to replace the old one.
   *
   * @param field_name  field name whose vector index should be rebuilt
   * @param index_type  index type (e.g. "HNSW", "IVFFLAT", "IVFPQ", "FLAT")
   * @param training_threshold  training threshold for the new index
   * @param do_train   whether to train the new index before swapping in
   * @return Status
   */
  Status RebuildVectorIndex(const std::string &field_name,
                            const std::string &index_type,
                            int training_threshold, bool do_train);

  Status CreateVectorTable(TableInfo &table, std::vector<int> &vector_cf_ids,
                           StorageManager *storage_mgr);

  int AddToStore(int docid,
                 std::unordered_map<std::string, struct Field> &fields);

  int Update(int docid, std::unordered_map<std::string, struct Field> &fields);

  int TrainIndex(std::map<std::string, IndexModel *> &vector_indexes);

  int AddRTVecsToIndex(bool &index_is_dirty);

  // int Add(int docid, const std::vector<Field *> &field_vecs);
  Status Search(GammaQuery &query, GammaResult *results);

  int GetVector(const std::vector<std::pair<std::string, int>> &fields_ids,
                std::vector<std::string> &vec);

  int GetDocVector(int docid, std::string &field_name,
                   std::vector<uint8_t> &vec);

  void GetTotalMemBytes(long &index_total_mem_bytes,
                        long &vector_total_mem_bytes);

  int Dump(const std::string &path, int64_t dump_docid, int64_t max_docid);
  int Load(const std::vector<std::string> &path, int64_t &doc_num);

  bool Contains(std::string &field_name);

  bool SupportIncrement();

  void VectorNames(std::vector<std::string> &names) {
    for (const auto &it : raw_vectors_) {
      names.push_back(it.first);
    }
  }

  std::map<std::string, IndexModel *> &VectorIndexes() {
    return vector_indexes_;
  }

  int Delete(int64_t docid);

  std::map<std::string, RawVector *> &RawVectors() { return raw_vectors_; }

  std::map<std::string, IndexModel *> &IndexModels() { return vector_indexes_; }

  int MinIndexedNum();

  bitmap::BitmapManager *Bitmap() { return docids_bitmap_; };

  void Close();  // release all resource

  Status CompactVector();

  /**
   * @brief Reset index types and index parameters
   */
  void ResetIndexTypesAndParams();

  /**
   * @brief Add index type and index parameter
   *
   * @param index_type  index type to add
   * @param index_param index parameter to add
   */
  void AddIndexTypeAndParam(const std::string &index_type,
                            const std::string &index_param);

  bool GetEnableRealtime() { return enable_realtime_; }

 private:
  inline std::string IndexName(const std::string &field_name,
                               const std::string &index_type) {
    return field_name + index_name_connector_ + index_type;
  }

  inline void GetVectorNameAndIndexType(const std::string &index_name,
                                        std::string &vec_name,
                                        std::string &index_type) {
    size_t pos = index_name.rfind(index_name_connector_);
    if (pos == std::string::npos) {
      LOG(ERROR) << desc_ << "Invalid index name format: " << index_name;
      return;
    }

    vec_name = index_name.substr(0, pos);
    index_type = index_name.substr(pos + 1);
  }

  /**
   * @brief Resolve (RawVector*, index_param) for a (field_name, index_type)
   * rebuild target. Shared by ReCreateVectorIndex / RebuildVectorIndex.
   *
   * Looks up `raw_vectors_[field_name]`, then scans `index_types_` /
   * `index_params_` for a matching index_type; falls back to the first
   * index_param entry when no exact match exists (legacy behaviour).
   *
   * @param field_name   target field
   * @param index_type   target index type
   * @param vec          [out] RawVector pointer; set on success only
   * @param index_param  [out] resolved index param string
   * @return Status::OK() on success; ParamError when the field has no
   *         RawVector entry.
   */
  Status ResolveRebuildTarget(const std::string &field_name,
                              const std::string &index_type,
                              RawVector *&vec, std::string &index_param);

 private:
  VectorStorageType default_store_type_;
  bitmap::BitmapManager *docids_bitmap_;
  bool table_created_;
  std::string root_path_;
  std::string desc_;

  std::map<std::string, RawVector *> raw_vectors_;
  std::map<std::string, IndexModel *> vector_indexes_;
  // vector memory buffer for realtime
  std::map<std::string, RawVector *> vector_memory_buffers_;
  // FLAT index
  std::map<std::string, IndexModel *> vector_memory_buffer_indexes_;
  bool enable_realtime_;

  std::vector<std::string> index_types_;
  std::vector<std::string> index_params_;
  pthread_rwlock_t index_rwmutex_;
  const std::string index_name_connector_ = "::";
};

}  // namespace vearch
