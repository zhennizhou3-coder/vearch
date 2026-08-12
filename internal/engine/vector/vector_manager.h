
/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/gamma_common_data.h"
#include "index/index_model.h"
#include "index/index_state.h"
#include "util/bitmap_manager.h"
#include "util/log.h"
#include "util/status.h"
#include "vector/raw_vector.h"

namespace vearch {

// Subdirectory under the space's index root that holds dumped vector index
// files: <index_root>/<kDumpSubdirName>/<timestamp>/<AbsoluteName>/<type>.index.
// Single source of truth — Engine builds dump_path_ from it, and
// RemoveVectorIndex uses it to purge a removed field's dumped files. Keeping
// one definition avoids the two drifting apart (a mismatch would silently make
// the removal-time cleanup a no-op and let stale index files resurface).
constexpr const char *kDumpSubdirName = "retrieval_model_index";

class VectorManager {
 public:
  // State of one vector index. Callers receive a copy and do not need to hold
  // VectorManager's rwlock while reading it. Named IndexStatusEntry so it does
  // not shadow the global `enum IndexStatus` (index_state.h) used for `status`.
  // Per-index status is keyed by the same key as vector_indexes_
  // (IndexName(field, index_type)); field-level rebuild flips only the target
  // index's status and can reach the FAILED terminal state.
  struct IndexStatusEntry {
    std::string name;
    IndexStatus status;
    // Vectors actually added to this index so far (the framework's
    // indexed_count_). Read from the same IndexModel the status is keyed by,
    // under the same rdlock. Lets the rebuild monitor gate completion on
    // "backfill caught up" instead of the train-done status flip (which reports
    // INDEXED while indexed_count_ is still climbing from the background
    // AddRTVecsToIndex pass). Named indexed_num to match the EngineStatus
    // surface field (min_indexed_num), not the internal indexed_count_.
    int64_t indexed_num;
    // Two factual basic properties the rebuild monitor combines to classify
    // whether this index will backfill after swap: is_trained (untrained IVF
    // serves via full-recall FLAT, never backfills indexed_num) and
    // support_increment (non-incremental DISKANN is built whole in Indexing(),
    // never backfilled). Both read under the same rdlock as status/indexed_num.
    bool is_trained;
    bool support_increment;
  };

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

  // index_name is the vector_indexes_ key (user-facing index name); callers
  // that only know (field, type) resolve it via field_type_index_name_ first.
  Status CreateVectorIndex(const std::string &index_name,
                           const std::string &index_type,
                           const std::string &index_params, RawVector *vec,
                           int training_threshold, bool destroy_vec,
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

  // Build (allocate + Init, no training/data — that is a separate step) an
  // index object per field into `vector_indexes`, from field_index_params_.
  // Set already_locked=true when the caller already holds vector_indexes_mutex_
  // in write mode (e.g. ReCreateVectorIndexes): pthread_rwlock is not reentrant,
  // so this must NOT take the rdlock again in that case.
  Status CreateVectorIndexes(
      int training_threshold,
      std::map<std::string, IndexModel *> &vector_indexes,
      bool already_locked = false);

  void ResetVectorIndexes(
      std::map<std::string, IndexModel *> &rebuild_vector_indexes);

  Status ReCreateVectorIndexes(int training_threshold);

  // Per-index rebuild (drop-before path): destroy the existing index for
  // (field_name, index_type), re-create + train it, and swap in. The map key
  // in vector_indexes_ is IndexName(field_name, index_type); index_name is the
  // user-facing name used to publish build state to index_name_to_state_
  // (describe API) in addition to the numeric vector_index_status_ the rebuild
  // monitor polls.
  Status ReCreateVectorIndex(const std::string &index_name,
                             const std::string &field_name,
                             const std::string &index_type,
                             int training_threshold);

  // Per-index rebuild (in-place path): build a new index alongside the live
  // one, optionally train it, then swap it in under the write lock. The old
  // index stays queryable until the swap, so search never sees a missing
  // index. index_name is used for build-state publishing (see
  // ReCreateVectorIndex).
  Status RebuildVectorIndex(const std::string &index_name,
                            const std::string &field_name,
                            const std::string &index_type,
                            int training_threshold, bool do_train);

  // Snapshot every vector index's rebuild status under vector_indexes_mutex_
  // rdlock, returned by value. Powers EngineStatus.index_statuses and lets the
  // rebuild monitor track the specific index it triggered.
  std::vector<IndexStatusEntry> IndexStatuses();

  // Set the rebuild status of one specific index (keyed by IndexName).
  void SetIndexStatus(const std::string &index_name, IndexStatus st);

  // Bulk variant that writes `st` for every key in `m`.
  void SetAllStatuses(const std::map<std::string, IndexModel *> &m,
                      IndexStatus st);

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

  bool Contains(const std::string &field_name) const;

  bool RegisterIndexName(const std::string &name,
                         const std::string &field_name);
  void UnregisterIndexName(const std::string &name);
  bool FindFieldByIndexName(const std::string &name,
                            std::string *field_name) const;
  bool HasIndexName(const std::string &name) const;

  // Build state of a dynamically-added vector index, keyed by user-defined
  // index name. Reported through EngineStatus alongside scalar states. Vector
  // search does not gate on this (the index is swapped in atomically), so it is
  // observability only. Guarded by index_name_map_mutex_, mutated only by the
  // Engine-layer add/remove task.
  void SetIndexState(const std::string &name, IndexState state);
  std::map<std::string, std::string> GetAllIndexStates() const;

  bool SupportIncrement();

  // Per-index variant: does the index named index_name support incremental
  // Add? A missing index returns the same default as a fresh IndexModel (true),
  // so callers treat "unknown" as incremental. Used by RebuildIndex to force a
  // full build for non-incremental indexes (e.g. DISKANN) regardless of the
  // training_threshold gate, which is an IVF-only concept.
  bool SupportIncrementOf(const std::string &index_name);

  void VectorNames(std::vector<std::string> &names) {
    for (const auto &it : raw_vectors_) {
      names.push_back(it.first);
    }
  }

  std::map<std::string, IndexModel *> &VectorIndexes() {
    return vector_indexes_;
  }

  // Lock-safe check for any remaining vector index. Reads vector_indexes_ under
  // the rdlock (unlike the VectorIndexes() accessor, which hands out a bare
  // reference). Used after a removal to decide whether index_status_ should be
  // reset once the last vector index is gone.
  bool HasAnyVectorIndex() {
    pthread_rwlock_rdlock(&vector_indexes_mutex_);
    bool any = !vector_indexes_.empty();
    pthread_rwlock_unlock(&vector_indexes_mutex_);
    return any;
  }

  bool HasNPUIndex();

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
   * @brief Add index type and index parameter, plus its user index_name so the
   * forward field->type->index_name map stays consistent.
   *
   * @param index_name  user-facing index name (vector_indexes_ key); empty ->
   *                    synthesized IndexName(field, type) fallback
   * @param field_name  vector field this index is over
   * @param index_type  index type to add
   * @param index_param index parameter to add
   */
  void AddIndexTypeAndParam(const std::string &index_name,
                            const std::string &field_name,
                            const std::string &index_type,
                            const std::string &index_param);

  bool RemoveIndexTypeAndParam(const std::string &field_name,
                               const std::string &index_type,
                               const std::string &index_param);

  bool GetEnableRealtime() { return enable_realtime_; }

  // Batch size used by AddRTVecsToIndex when draining new vectors into indexes.
  // 0 means "use the per-hardware default"; a positive value overrides it for
  // all index types. Set at engine startup from the gamma Table (so it survives
  // restart) and at runtime via Engine::SetConfig.
  void SetIndexBuildBatchSize(int64_t size) { index_build_batch_size_ = size; }
  int64_t GetIndexBuildBatchSize() { return index_build_batch_size_; }

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

  // Directory that contains the space's index root, derived from a field's raw
  // vector storage path: storage root is "<index_root>/data", so its parent is
  // "<index_root>". Returns "" if the field has no raw vector / storage manager.
  // Shared by the removal-time on-disk cleanups (DiskANN runtime dir and dumped
  // index files), which both need <index_root>-relative paths but VectorManager
  // does not hold the index root directly.
  std::string StorageRootParent(const std::string &field_name);

  // Resolve (RawVector*, index_param) for a (field_name, index_type) rebuild
  // target from field_index_params_. Shared by ReCreateVectorIndex /
  // RebuildVectorIndex. Caller must hold vector_indexes_mutex_ (the params map
  // is read under it). Returns ParamError when the field has no RawVector or
  // no index registered for the requested type.
  Status ResolveRebuildTarget(const std::string &field_name,
                              const std::string &index_type, RawVector *&vec,
                              std::string &index_param);

 private:
  VectorStorageType default_store_type_;
  bitmap::BitmapManager *docids_bitmap_;
  bool table_created_;
  std::string root_path_;
  std::string desc_;

  std::map<std::string, RawVector *> raw_vectors_;
  std::map<std::string, IndexModel *> vector_indexes_;
  // Per-index rebuild status, keyed by the SAME key as vector_indexes_
  // (IndexName(field, type)). Written under vector_indexes_mutex_ wrlock
  // alongside vector_indexes_ so the two key sets stay consistent; read
  // (IndexStatuses) under rdlock. Powers EngineStatus.index_statuses and lets
  // the rebuild monitor track the specific index it triggered instead of the
  // coarse engine-wide index_status_.
  std::unordered_map<std::string, IndexStatus> vector_index_status_;
  // vector memory buffer for realtime
  std::map<std::string, RawVector *> vector_memory_buffers_;
  // FLAT index
  std::map<std::string, IndexModel *> vector_memory_buffer_indexes_;
  bool enable_realtime_;

  // See Set/GetIndexBuildBatchSize. 0 = use per-hardware default.
  int64_t index_build_batch_size_ = 0;

  // Per-field vector index types and their params: field_name -> (index_type ->
  // params). Replaces the old parallel index_types_/index_params_ vectors,
  // which conflated all fields' types into two positionally-paired flat lists
  // (fragile on remove, and the "[0] is the default type" logic ignored which
  // field a query targeted). Keyed by field so Search can resolve the default
  // type for the queried field, and remove is an O(log) erase.
  std::map<std::string, std::map<std::string, std::string>> field_index_params_;
  // Forward map field_name -> (index_type -> index_name), so query / rebuild
  // paths that only know (field, type) can resolve the vector_indexes_ key.
  // Kept in lockstep with field_index_params_ under vector_indexes_mutex_.
  std::map<std::string, std::map<std::string, std::string>> field_type_index_name_;
  pthread_rwlock_t vector_indexes_mutex_;
  const std::string index_name_connector_ = "::";

  // Maps user-defined index name -> the vector field the index covers.
  // Engine layer is the only writer; protected by index_name_map_mutex_ so it stays
  // independent of vector_indexes_mutex_ which guards vector_indexes_.
  mutable std::mutex index_name_map_mutex_;
  std::map<std::string, std::string> index_name_to_field_;
  // Build state per index name, same lifetime/lock as index_name_to_field_.
  std::map<std::string, IndexState> index_name_to_state_;
};

}  // namespace vearch
