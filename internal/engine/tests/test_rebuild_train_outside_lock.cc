/**
 * Copyright (c) The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

/**
 * Tests for VectorManager::ReCreateVectorIndex (the drop_before_rebuild path)
 * training OUTSIDE the vector_indexes_mutex_ write lock.
 *
 * The contract under test:
 *   - The freshly-created (untrained) index is published into the live map with
 *     status INDEXING *before* training starts.
 *   - The write lock is released before the (long) training phase, so concurrent
 *     readers taking the rdlock (e.g. IndexStatuses / Search) are NOT blocked for
 *     the whole rebuild. If the lock were held across training (the pre-fix
 *     behavior), a concurrent rdlock caller would block until training finished
 *     and could only ever observe INDEXED — never INDEXING.
 *   - On training success the target ends INDEXED; on failure it ends FAILED, and
 *     in BOTH cases the mutex is left balanced (a subsequent lock-taking call must
 *     not deadlock).
 *
 * Determinism without real training: a stub IndexModel ("TESTBLOCKINGINDEX")
 * blocks inside Indexing() on a latch the test controls, so "training is in
 * progress" is an exact, race-free state rather than a timing guess.
 *
 * NOTE: this test builds against the Gamma engine (link target `gamma`) and must
 * be compiled/run in the engine build (`cd build && ./build.sh -t`, then run
 * ./build/gamma_build/tests/test_rebuild_train_outside_lock). It cannot be built
 * on platforms lacking the engine's native dependencies.
 */

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "c_api/api_data/table.h"
#include "index/index_model.h"
#include "index/index_state.h"
#include "index/reflector.h"
#include "util/bitmap_manager.h"
#include "util/status.h"
#include "vector/vector_manager.h"

namespace {

// Shared latch coordinating the stub index's training with the test thread.
// One process per test executable, so a plain file-scope instance is fine; each
// test calls Reset() first.
struct TrainLatch {
  std::mutex m;
  std::condition_variable cv;
  bool entered = false;  // set by the stub when Indexing() begins
  bool release = false;  // set by the test to let Indexing() return
  int result = 0;        // value Indexing() returns (0 = ok, non-zero = fail)

  void Reset() {
    std::lock_guard<std::mutex> lk(m);
    entered = false;
    release = false;
    result = 0;
  }
};

TrainLatch g_latch;

}  // namespace

// A minimal IndexModel whose Indexing() (the training entry called by
// VectorManager::TrainIndex) blocks on the latch. Nothing here touches vector_,
// so the raw vector created by CreateVectorTable never needs real data.
class BlockingStubIndex : public IndexModel {
 public:
  vearch::Status Init(const std::string &, int) override {
    return vearch::Status::OK();
  }
  RetrievalParameters *Parse(const std::string &) override { return nullptr; }

  int Indexing() override {
    std::unique_lock<std::mutex> lk(g_latch.m);
    g_latch.entered = true;
    g_latch.cv.notify_all();
    g_latch.cv.wait(lk, [] { return g_latch.release; });
    return g_latch.result;
  }

  bool Add(int, const uint8_t *) override { return true; }
  int Update(const std::vector<int64_t> &,
             const std::vector<const uint8_t *> &) override {
    return 0;
  }
  int Delete(const std::vector<int64_t> &) override { return 0; }
  int Search(RetrievalContext *, int, const uint8_t *, int, float *,
             int64_t *) override {
    return 0;
  }
  long GetTotalMemBytes() override { return 0; }
  vearch::Status Dump(const std::string &) override {
    return vearch::Status::OK();
  }
  vearch::Status Load(const std::string &, int64_t &) override {
    return vearch::Status::OK();
  }
};

REGISTER_INDEX(TESTBLOCKINGINDEX, BlockingStubIndex)

namespace {

constexpr const char *kField = "vf";
constexpr const char *kIndexName = "idx_vf";
constexpr const char *kIndexType = "TESTBLOCKINGINDEX";

void FillTable(vearch::TableInfo &table) {
  std::string tname = "t_recreate_nolock";
  table.SetName(tname);
  table.SetTrainingThreshold(1);
  table.SetEnableRealtime(false);

  vearch::VectorInfo vi;
  vi.name = kField;
  vi.data_type = DataType::VECTOR;
  vi.is_index = true;
  vi.dimension = 4;
  vi.store_type = "MemoryOnly";  // keep it in-memory: no StorageManager needed
  vi.store_param = "";
  table.AddVectorInfo(vi);

  vearch::IndexInfo ii;
  ii.name = kIndexName;
  ii.type = kIndexType;
  ii.field_name = kField;
  ii.params = "{}";
  table.AddIndex(ii);
}

// Look up the target index's rebuild status in an IndexStatuses() snapshot.
// Returns -1 if the target is absent.
int StatusOfTarget(
    const std::vector<vearch::VectorManager::IndexStatusEntry> &statuses) {
  for (const auto &e : statuses) {
    if (e.name == kIndexName) return static_cast<int>(e.status);
  }
  return -1;
}

class ReCreateNoLockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    g_latch.Reset();
    mkdir(root_.c_str(), 0755);  // best-effort; MemoryOnly needs no files
    ASSERT_EQ(bm_.Init(1 << 20), 0);
    vm_ = new vearch::VectorManager(vearch::VectorStorageType::MemoryOnly, &bm_,
                                    root_, desc_);
    vearch::TableInfo table;
    FillTable(table);
    std::vector<int> cf_ids = {0};
    vearch::Status st = vm_->CreateVectorTable(table, cf_ids, nullptr);
    ASSERT_TRUE(st.ok()) << st.ToString();
  }

  void TearDown() override {
    delete vm_;
    vm_ = nullptr;
  }

  bitmap::BitmapManager bm_;
  std::string root_ = "/tmp/vearch_recreate_nolock_test";
  std::string desc_ = "recreate_nolock";
  vearch::VectorManager *vm_ = nullptr;
};

// The core regression guard: while training is in progress, a concurrent rdlock
// caller must return promptly AND observe INDEXING. Under the pre-fix behavior
// (write lock held across training) this call would block until training ended.
TEST_F(ReCreateNoLockTest, ReaderNotBlockedAndSeesIndexingDuringTraining) {
  vearch::Status rc_status;
  std::thread rebuild([&] {
    rc_status = vm_->ReCreateVectorIndex(kIndexName, kField, kIndexType,
                                         /*training_threshold=*/1);
  });

  // Wait until the stub is inside Indexing() (training in progress, blocked).
  {
    std::unique_lock<std::mutex> lk(g_latch.m);
    ASSERT_TRUE(g_latch.cv.wait_for(lk, std::chrono::seconds(10),
                                    [] { return g_latch.entered; }))
        << "training never started";
  }

  // The write lock must have been released before training: a rdlock op must
  // complete without waiting for training to finish.
  auto fut =
      std::async(std::launch::async, [&] { return vm_->IndexStatuses(); });
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(10)), std::future_status::ready)
      << "IndexStatuses() blocked while training was in progress — "
         "vector_indexes_mutex_ was held across training (regression)";

  EXPECT_EQ(StatusOfTarget(fut.get()), static_cast<int>(vearch::INDEXING))
      << "untrained index was not published as INDEXING before training";

  // Let training finish.
  {
    std::lock_guard<std::mutex> lk(g_latch.m);
    g_latch.release = true;
  }
  g_latch.cv.notify_all();
  rebuild.join();

  EXPECT_TRUE(rc_status.ok()) << rc_status.ToString();
  EXPECT_EQ(StatusOfTarget(vm_->IndexStatuses()),
            static_cast<int>(vearch::INDEXED));
}

// On training failure the target ends FAILED, and the mutex is left balanced:
// the rdlock probe and a follow-up wrlock rebuild must both proceed (no
// deadlock from the early-unlock + SetIndexStatus re-lock restructuring).
TEST_F(ReCreateNoLockTest, FailedTrainingSetsFailedAndLeavesLockBalanced) {
  // Make training return immediately with a failure.
  {
    std::lock_guard<std::mutex> lk(g_latch.m);
    g_latch.release = true;
    g_latch.result = -1;
  }

  vearch::Status st =
      vm_->ReCreateVectorIndex(kIndexName, kField, kIndexType, 1);
  EXPECT_FALSE(st.ok());

  // Would hang here if the failure path forgot to release the write lock.
  EXPECT_EQ(StatusOfTarget(vm_->IndexStatuses()),
            static_cast<int>(vearch::FAILED));

  // A subsequent successful rebuild must acquire the write lock and complete.
  g_latch.Reset();
  {
    std::lock_guard<std::mutex> lk(g_latch.m);
    g_latch.release = true;  // train returns immediately, ok
  }
  vearch::Status st2 =
      vm_->ReCreateVectorIndex(kIndexName, kField, kIndexType, 1);
  EXPECT_TRUE(st2.ok()) << st2.ToString();
  EXPECT_EQ(StatusOfTarget(vm_->IndexStatuses()),
            static_cast<int>(vearch::INDEXED));
}

}  // namespace
