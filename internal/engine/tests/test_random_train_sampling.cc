/**
 * Copyright (c) The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

/**
 * Tests for RawVector::SampleTrainingVectors — the PR 1 contract:
 *
 *   - Filters out vectors whose bitmap bit is set (deleted).
 *   - Returns valid_count = total non-deleted vectors.
 *   - Returns num_got = min(num, valid_count); never includes deleted ids.
 *   - Returns 0 on success, -1 when no valid vectors exist.
 *   - Sampling is random (reservoir sampling, not "first N").
 *   - Vector data is contiguous and byte-identical to what was written.
 *   - MemoryRawVector and RocksDBRawVector implement the same contract.
 */

#include <gtest/gtest.h>
#include <string.h>

#include <algorithm>
#include <cmath>
#include <set>
#include <string>
#include <vector>

#include "test.h"
#include "util/bitmap_manager.h"
#include "util/utils.h"
#include "vector/memory_raw_vector.h"
#include "vector/raw_vector_factory.h"
#include "vector/rocksdb_raw_vector.h"

using namespace vearch;
using std::string;
using std::vector;

namespace {

constexpr int kDimension = 16;

// Build a deterministic float vector: v[j] = (float)id * kDimension + j.
// This makes it possible to recover the original id from any returned vector
// by inspecting v[0] — we use that to assert no deleted ids leaked through.
float *MakeVector(int64_t id) {
  float *v = new float[kDimension];
  for (int j = 0; j < kDimension; ++j) {
    v[j] = (float)id * kDimension + j;
  }
  return v;
}

Field *MakeVectorField(int64_t id) {
  float *data = MakeVector(id);
  Field *field = new Field();
  field->value = string((char *)data, sizeof(float) * kDimension);
  field->datatype = DataType::VECTOR;
  delete[] data;
  return field;
}

// Recover the id encoded in the first float of a returned vector.
int64_t IdFromVector(const uint8_t *bytes) {
  const float *v = reinterpret_cast<const float *>(bytes);
  return (int64_t)std::lround(v[0] / (float)kDimension);
}

// Convenience: collect the ids returned in a sample (which is one contiguous
// block of num_got * dimension * sizeof(float) bytes inside vecs[0]).
vector<int64_t> CollectReturnedIds(ScopeVectors &vecs, size_t num_got) {
  EXPECT_EQ(1u, vecs.Size())
      << "SampleTrainingVectors must return exactly one contiguous block";
  vector<int64_t> ids;
  ids.reserve(num_got);
  const uint8_t *block = vecs.Get(0);
  size_t stride = (size_t)kDimension * sizeof(float);
  for (size_t i = 0; i < num_got; ++i) {
    ids.push_back(IdFromVector(block + i * stride));
  }
  return ids;
}

// Test fixture. Each test sets up a fresh bitmap, raw vector, and storage
// manager rooted at ./test_random_train_sampling_<TestName>/, then tears
// it all down on exit.
class RandomTrainSamplingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_path_ = "./test_random_train_sampling_" + GetCurrentCaseName();
    utils::remove_dir(root_path_.c_str());
    utils::make_dir(root_path_.c_str());

    bitmap_ = new bitmap::BitmapManager();
    bitmap_->SetDumpFilePath(root_path_ + "/bitmap");
    ASSERT_EQ(0, bitmap_->Init(/*bit_size=*/1 << 20));
  }

  void TearDown() override {
    delete raw_vector_;
    raw_vector_ = nullptr;
    delete storage_mgr_;
    storage_mgr_ = nullptr;
    delete bitmap_;
    bitmap_ = nullptr;
    utils::remove_dir(root_path_.c_str());
  }

  // Creates a raw vector backed by the given storage type, then inserts
  // `n_total` vectors with ids [0, n_total). After this returns, callers
  // can mark deletions via bitmap_->Set(id) and then call
  // raw_vector_->SampleTrainingVectors().
  void CreateAndFill(VectorStorageType store_type, int n_total) {
    StoreParams store_params;
    store_params.cache_size = 1;
    store_params.segment_size = 100;

    auto *meta_info =
        new VectorMetaInfo("vec", kDimension, VectorValueType::FLOAT);
    storage_mgr_ = new StorageManager(root_path_);
    int cf_id = storage_mgr_->CreateColumnFamily("vec");
    raw_vector_ = RawVectorFactory::Create(meta_info, store_type, store_params,
                                           bitmap_, cf_id, storage_mgr_);
    auto status = storage_mgr_->Init(100);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, raw_vector_->Init("vec"));
    for (size_t i = 0; i < n_total; ++i) {
      Field *field = MakeVectorField(i);
      ASSERT_EQ(0, raw_vector_->Add(i, *field));
      delete field;
    }
    ASSERT_EQ(n_total, raw_vector_->GetVectorNum());
  }

  string root_path_;
  bitmap::BitmapManager *bitmap_ = nullptr;
  RawVector *raw_vector_ = nullptr;
  StorageManager *storage_mgr_ = nullptr;
};

// ---------- Memory backend tests ----------

// A zero request returns immediately without scanning live vectors.
TEST_F(RandomTrainSamplingTest, MemoryZeroRequest) {
  const size_t kTotal = 100;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  EXPECT_EQ(-1,
            raw_vector_->SampleTrainingVectors(0, vecs, num_got, valid_count));
  EXPECT_EQ(0u, num_got);
  EXPECT_EQ(0u, valid_count);
}

TEST_F(RandomTrainSamplingTest, MemoryRequestExceedsTotal) {
  const size_t kTotal = 100;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kTotal + 1, vecs, num_got,
                                               valid_count));
  EXPECT_EQ(kTotal, num_got);
  EXPECT_EQ(kTotal, valid_count);
}

TEST_F(RandomTrainSamplingTest, MemoryRequestEqualsValidCount) {
  const size_t kTotal = 100;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kTotal, vecs, num_got,
                                               valid_count));
  EXPECT_EQ(kTotal, num_got);
  EXPECT_EQ(kTotal, valid_count);
}

// 1. Basic: no deletions. valid_count == n_total, num_got == requested num,
//    every returned id is in [0, n_total).
TEST_F(RandomTrainSamplingTest, MemoryNoDeletes) {
  const size_t kTotal = 1000;
  const size_t kRequest = 200;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kRequest, vecs, num_got, valid_count));

  EXPECT_EQ(kRequest, num_got);
  EXPECT_EQ(kTotal, valid_count);

  vector<int64_t> ids = CollectReturnedIds(vecs, num_got);
  std::set<int64_t> unique_ids(ids.begin(), ids.end());
  EXPECT_EQ(unique_ids.size(), ids.size())
      << "reservoir sampling must not return duplicates";
  for (int64_t id : ids) {
    EXPECT_GE(id, 0);
    EXPECT_LT(id, kTotal);
  }
}

// 2. Tombstone filtering: with the first half marked deleted, no returned
//    id may have come from that half, regardless of how many we request.
//    This is the core regression guard against the old GetVectorHeader(0, n)
//    which returned the first n raw vectors (deleted or not).
TEST_F(RandomTrainSamplingTest, MemorySkipsDeleted) {
  const size_t kTotal = 2000;
  const size_t kDeletedUpTo = 1000;       // delete ids [0, 1000)
  const size_t kRequest = 500;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);

  for (size_t i = 0; i < kDeletedUpTo; ++i) {
    ASSERT_EQ(0, bitmap_->Set(i));
  }

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kRequest, vecs, num_got, valid_count));

  EXPECT_EQ(kRequest, num_got);
  EXPECT_EQ((size_t)(kTotal - kDeletedUpTo), valid_count);

  vector<int64_t> ids = CollectReturnedIds(vecs, num_got);
  for (int64_t id : ids) {
    EXPECT_GE(id, kDeletedUpTo)
        << "deleted vector id " << id << " leaked into training set";
    EXPECT_LT(id, kTotal);
  }
}

// 3. Valid count short of request: when fewer non-deleted vectors exist
//    than `num`, num_got == valid_count and the function returns 0.
//    The caller (e.g. GammaIVFPQIndex::Indexing) is expected to compare
//    valid_count vs threshold and bail out, but that decision is not
//    SampleTrainingVectors' job.
TEST_F(RandomTrainSamplingTest, MemoryFewerValidThanRequested) {
  const size_t kTotal = 200;
  const size_t kDeletedUpTo = 150;
  const size_t kRequest = 100;            // > kTotal - kDeletedUpTo == 50
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);
  for (size_t i = 0; i < kDeletedUpTo; ++i) {
    ASSERT_EQ(0, bitmap_->Set(i));
  }

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kRequest, vecs, num_got, valid_count));
  EXPECT_EQ((size_t)(kTotal - kDeletedUpTo), num_got);
  EXPECT_EQ((size_t)(kTotal - kDeletedUpTo), valid_count);
  // every surviving vector should appear exactly once
  vector<int64_t> ids = CollectReturnedIds(vecs, num_got);
  std::sort(ids.begin(), ids.end());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ((int64_t)(kDeletedUpTo + i), ids[i]);
  }
}

// 4. All deleted: no valid vectors → returns -1, num_got == 0,
//    valid_count == 0. This is the "no training set" failure mode.
TEST_F(RandomTrainSamplingTest, MemoryAllDeletedReturnsError) {
  const size_t kTotal = 100;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);
  for (size_t i = 0; i < kTotal; ++i) {
    ASSERT_EQ(0, bitmap_->Set(i));
  }

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(-1,
            raw_vector_->SampleTrainingVectors(50, vecs, num_got, valid_count));
  EXPECT_EQ(0u, num_got);
  EXPECT_EQ(0u, valid_count);
}

// 5. Randomness: two consecutive samples of the same population should
//    differ in at least one position. Equality is theoretically possible
//    but with kRequest=100 from a pool of 1000 the probability is
//    negligible (Birthday-like bound << 1e-100). If this flakes we have
//    bigger problems than this test.
//    This guards against an implementation that reverts to "first N"
//    behavior or seeds the RNG with a constant.
TEST_F(RandomTrainSamplingTest, MemorySamplingIsRandom) {
  const size_t kTotal = 1000;
  const size_t kRequest = 100;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);

  ScopeVectors vecs1;
  size_t n1 = 0, vc1 = 0;
  ASSERT_EQ(0, raw_vector_->SampleTrainingVectors(kRequest, vecs1, n1, vc1));

  ScopeVectors vecs2;
  size_t n2 = 0, vc2 = 0;
  ASSERT_EQ(0, raw_vector_->SampleTrainingVectors(kRequest, vecs2, n2, vc2));

  vector<int64_t> ids1 = CollectReturnedIds(vecs1, n1);
  vector<int64_t> ids2 = CollectReturnedIds(vecs2, n2);
  std::sort(ids1.begin(), ids1.end());
  std::sort(ids2.begin(), ids2.end());
  EXPECT_NE(ids1, ids2)
      << "two independent samples produced identical id sets — "
         "likely deterministic seeding or 'first N' behavior";
}

// 6. Returned vector data is byte-identical to what was originally written.
//    Catches mistakes in the contiguous-block memcpy path.
TEST_F(RandomTrainSamplingTest, MemoryReturnedDataIntegrity) {
  const size_t kTotal = 500;
  const size_t kRequest = 50;
  CreateAndFill(VectorStorageType::MemoryOnly, kTotal);

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kRequest, vecs, num_got, valid_count));
  ASSERT_EQ(kRequest, num_got);

  const uint8_t *block = vecs.Get(0);
  size_t stride = (size_t)kDimension * sizeof(float);
  for (size_t i = 0; i < num_got; ++i) {
    int64_t id = IdFromVector(block + i * stride);
    // Reconstruct expected bytes for that id.
    float *expected = MakeVector(id);
    EXPECT_EQ(0, memcmp(block + i * stride, expected,
                        kDimension * sizeof(float)))
        << "byte mismatch for sampled id=" << id << " at slot " << i;
    delete[] expected;
  }
}

// ---------- Cross-backend parity: same contract, RocksDB-backed ----------

// We re-run the two most important assertions on RocksDBRawVector to ensure
// the per-backend implementation honors the same invariants. The full grid
// is not necessary; reservoir sampling + bitmap test is shared logic, only
// the data-fetch path differs (memcpy vs MultiGet).

TEST_F(RandomTrainSamplingTest, RocksDBSkipsDeleted) {
  const size_t kTotal = 1000;
  const size_t kDeletedUpTo = 600;
  const size_t kRequest = 200;
  CreateAndFill(VectorStorageType::RocksDB, kTotal);
  for (size_t i = 0; i < kDeletedUpTo; ++i) {
    ASSERT_EQ(0, bitmap_->Set(i));
  }

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kRequest, vecs, num_got, valid_count));
  EXPECT_EQ(kRequest, num_got);
  EXPECT_EQ((size_t)(kTotal - kDeletedUpTo), valid_count);

  vector<int64_t> ids = CollectReturnedIds(vecs, num_got);
  for (int64_t id : ids) {
    EXPECT_GE(id, kDeletedUpTo)
        << "deleted vector id " << id << " leaked into training set "
           "(RocksDB backend)";
  }
}

TEST_F(RandomTrainSamplingTest, RocksDBReturnedDataIntegrity) {
  const size_t kTotal = 500;
  const size_t kRequest = 50;
  CreateAndFill(VectorStorageType::RocksDB, kTotal);

  ScopeVectors vecs;
  size_t num_got = 0, valid_count = 0;
  ASSERT_EQ(0,
            raw_vector_->SampleTrainingVectors(kRequest, vecs, num_got, valid_count));
  ASSERT_EQ(kRequest, num_got);

  const uint8_t *block = vecs.Get(0);
  size_t stride = (size_t)kDimension * sizeof(float);
  for (size_t i = 0; i < num_got; ++i) {
    int64_t id = IdFromVector(block + i * stride);
    float *expected = MakeVector(id);
    EXPECT_EQ(0, memcmp(block + i * stride, expected,
                        kDimension * sizeof(float)))
        << "byte mismatch for sampled id=" << id << " at slot " << i
        << " (RocksDB backend)";
    delete[] expected;
  }
}

}  // namespace

int main(int argc, char **argv) {
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
