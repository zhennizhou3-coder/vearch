/**
 * Copyright (c) The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

/**
 * End-to-end test that the index-rebuild OpenMP thread cap is actually applied
 * to the training that runs inside Engine::RebuildIndex.
 *
 * Unlike test_rebuild_train_threads.cc (which tests RebuildTrainThreadCap and
 * OmpThreadScope in isolation), this drives the real path:
 *
 *   Engine::RebuildIndex(drop_before_rebuild=1, limit_cpu)
 *     -> OmpThreadScope(RebuildTrainThreadCap(limit_cpu, cores))
 *        -> VectorManager::ReCreateVectorIndex -> TrainIndex -> IndexModel::Indexing()
 *
 * A probe IndexModel ("TESTCPUINDEX") records omp_get_max_threads() from inside
 * Indexing() — i.e. the exact point where training's parallelism is set. The
 * test asserts that value equals the computed cap (proving the RPC limit_cpu and
 * the max(1, cores*3/4) fallback really reach training), and that the thread
 * count is restored after RebuildIndex returns.
 *
 * NOTE: this builds and links the full Gamma engine (Engine + RocksDB-backed
 * StorageManager/bitmap under a temp dir) and must be compiled/run in the engine
 * test build (`cd build && ./build.sh -t`, then run
 * ./build/gamma_build/tests/test_rebuild_train_cpu_applied). It cannot be built
 * on platforms lacking the engine's native dependencies.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

#include "index/index_model.h"
#include "omp.h"
#include "search/engine.h"

namespace {

// omp_get_max_threads() observed from inside Indexing() (the training entry).
// record-first: any stray later Indexing() call cannot overwrite the value
// captured during the rebuild under test.
struct TrainObs {
  std::mutex m;
  bool has = false;
  int omp_threads = -1;

  void Reset() {
    std::lock_guard<std::mutex> lk(m);
    has = false;
    omp_threads = -1;
  }
  void Record(int n) {
    std::lock_guard<std::mutex> lk(m);
    if (!has) {
      omp_threads = n;
      has = true;
    }
  }
  bool Has() {
    std::lock_guard<std::mutex> lk(m);
    return has;
  }
  int Get() {
    std::lock_guard<std::mutex> lk(m);
    return omp_threads;
  }
};

TrainObs g_obs;

}  // namespace

// Probe index: its Indexing() (called by VectorManager::TrainIndex) records the
// live OMP thread count and returns immediately. Nothing touches vector_, so the
// raw vector never needs real data.
class CpuProbeIndex : public IndexModel {
 public:
  vearch::Status Init(const std::string &, int) override {
    return vearch::Status::OK();
  }
  RetrievalParameters *Parse(const std::string &) override { return nullptr; }
  int Indexing() override {
    g_obs.Record(omp_get_max_threads());
    return 0;
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

REGISTER_INDEX(TESTCPUINDEX, CpuProbeIndex)

namespace {

constexpr const char *kField = "vf";
constexpr const char *kIndexName = "idx_vf";
constexpr const char *kIndexType = "TESTCPUINDEX";

void BuildTable(vearch::TableInfo &table) {
  std::string tname = "cpu_cap_space";
  table.SetName(tname);
  // training_threshold >= 1 so the post-rebuild BuildIndex restart is skipped
  // for the 0-doc table (max_docid_(0) - delete_num_(0) >= threshold is false),
  // leaving exactly one Indexing() call — the one from the rebuild.
  table.SetTrainingThreshold(1);
  table.SetRefreshInterval(-1);
  table.SetEnableRealtime(false);
  table.SetEnableIdCache(false);
  table.SetIndexBuildBatchSize(0);

  // Table::CreateTable requires an "_id" field (it is the key).
  vearch::FieldInfo id_field;
  id_field.name = "_id";
  id_field.data_type = DataType::STRING;
  id_field.is_index = false;
  id_field.index_type = 0;
  table.AddField(id_field);

  vearch::VectorInfo vi;
  vi.name = kField;
  vi.data_type = DataType::VECTOR;
  vi.is_index = true;
  vi.dimension = 4;
  vi.store_type = "MemoryOnly";  // keep the raw vector in memory
  vi.store_param = "";
  table.AddVectorInfo(vi);

  // Vector index type is resolved from Indexes() by matching field_name.
  vearch::IndexInfo ii;
  ii.name = kIndexName;
  ii.type = kIndexType;
  ii.field_name = kField;
  ii.params = "{}";
  table.AddIndex(ii);
}

// Fresh engine over a clean temp dir; caller owns the returned pointer.
vearch::Engine *MakeEngine(const std::string &root) {
  std::string cmd = "rm -rf " + root;
  (void)std::system(cmd.c_str());
  vearch::Engine *engine = vearch::Engine::GetInstance(root, "cpu_cap_space");
  if (engine == nullptr) return nullptr;
  vearch::TableInfo table;
  BuildTable(table);
  vearch::Status st = engine->CreateTable(table);
  if (!st.ok()) {
    delete engine;
    return nullptr;
  }
  return engine;
}

// Explicit limit_cpu (clamped to cores) must be the thread count training runs
// with.
TEST(RebuildTrainCpuApplied, ExplicitLimitCpuIsAppliedToTraining) {
  g_obs.Reset();
  vearch::Engine *engine = MakeEngine("/tmp/vearch_cpu_cap_explicit");
  ASSERT_NE(engine, nullptr);

  // baseline is the pre-rebuild ICV, used only to check restoration below.
  // num_cores mirrors what Engine::RebuildIndex feeds RebuildTrainThreadCap.
  const int baseline = omp_get_max_threads();
  const int num_cores = omp_get_num_procs();
  const int limit_cpu = 2;
  g_obs.Reset();
  int rc = engine->RebuildIndex(kIndexName, kField, kIndexType,
                                /*drop_before_rebuild=*/1, limit_cpu,
                                /*describe=*/0);
  EXPECT_EQ(rc, 0);

  ASSERT_TRUE(g_obs.Has()) << "training (Indexing) was never invoked";
  EXPECT_EQ(g_obs.Get(), vearch::RebuildTrainThreadCap(limit_cpu, num_cores))
      << "training did not run under the RPC-provided limit_cpu";
  EXPECT_EQ(omp_get_max_threads(), baseline)
      << "OMP thread count was not restored after RebuildIndex";

  delete engine;
}

// With limit_cpu <= 0, training must run under the max(1, cores*3/4) fallback,
// not the full core count.
TEST(RebuildTrainCpuApplied, FallbackCapIsAppliedToTraining) {
  g_obs.Reset();
  vearch::Engine *engine = MakeEngine("/tmp/vearch_cpu_cap_fallback");
  ASSERT_NE(engine, nullptr);

  const int baseline = omp_get_max_threads();
  const int num_cores = omp_get_num_procs();
  g_obs.Reset();
  int rc = engine->RebuildIndex(kIndexName, kField, kIndexType,
                                /*drop_before_rebuild=*/1, /*limit_cpu=*/0,
                                /*describe=*/0);
  EXPECT_EQ(rc, 0);

  ASSERT_TRUE(g_obs.Has()) << "training (Indexing) was never invoked";
  EXPECT_EQ(g_obs.Get(), std::max(1, num_cores * 3 / 4))
      << "training did not run under the cores*3/4 fallback cap";
  EXPECT_EQ(g_obs.Get(), vearch::RebuildTrainThreadCap(0, num_cores));
  EXPECT_EQ(omp_get_max_threads(), baseline)
      << "OMP thread count was not restored after RebuildIndex";

  delete engine;
}

}  // namespace
