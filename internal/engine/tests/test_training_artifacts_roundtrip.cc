/**
 * Copyright (c) The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

/**
 * Roundtrip test for GammaIVFPQIndex training-artifacts dump/load
 * (Dump(path, true) / Load(path, true, n); replica index consistency, 方案①A P1).
 *
 * Verifies that dumping ONLY the training artifacts (coarse-quantizer centroids +
 * PQ codebook, no inverted lists) and loading it back into a fresh, empty index
 * reproduces the same centroids and codebook, marks the index trained, and
 * leaves the inverted index empty (indexed_vec_count_ == 0) — the invariant the
 * whole scheme relies on so all replicas share one training artifacts.
 *
 * It exercises the two member methods in isolation by transplanting a trained
 * faiss IVFPQ model into a GammaIVFPQIndex's faiss base subobject, bypassing the
 * RawVector-dependent Init/Indexing path. Links the full Gamma engine; build/run
 * via `cd build && ./build.sh -t`, then run
 * ./build/gamma_build/tests/test_training_artifacts_roundtrip.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <vector>

#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFPQ.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "faiss/IndexBinaryFlat.h"
#include "faiss/IndexBinaryIVF.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/IndexIVFPQFastScan.h"
#include "index/impl/gamma_index_binary_ivf.h"
#include "index/impl/gamma_index_ivfflat.h"
#include "index/impl/gamma_index_ivfpqfs.h"

namespace {

// Copy the training artifacts (coarse centroids + PQ codebook) from a plain faiss
// IVFPQ into the faiss::IndexIVFPQ base subobject of a GammaIVFPQIndex, without
// the RawVector-dependent Init/Indexing path. Only the fields the
// training-artifacts dump serializes are set. The GammaIVFPQIndex owns and deletes `quantizer` in its
// destructor, so own_fields stays false to avoid a double free.
void AdoptTrainingArtifacts(vearch::GammaIVFPQIndex &g, const faiss::IndexIVFPQ &ref) {
  faiss::IndexIVFPQ &base = g;
  base.d = ref.d;
  base.metric_type = ref.metric_type;
  base.nlist = ref.nlist;
  base.nprobe = ref.nprobe;
  base.by_residual = ref.by_residual;
  base.code_size = ref.code_size;
  base.pq = ref.pq;  // deep copy of the ProductQuantizer (codebook)
  base.is_trained = true;

  auto *quant = new faiss::IndexFlat(ref.d, ref.metric_type);
  std::vector<float> centroids((size_t)ref.nlist * ref.d);
  ref.quantizer->reconstruct_n(0, ref.nlist, centroids.data());
  quant->add(ref.nlist, centroids.data());
  base.quantizer = quant;
  base.own_fields = false;
}

TEST(TrainingArtifactsRoundtrip, IVFPQCentroidsAndCodebook) {
  const int d = 16, nlist = 8, M = 4, nbits = 8;
  const int nt = 10000;  // >= 256*39 so PQ (nbits=8) training has enough points, no faiss warnings

  std::mt19937 rng(12345);
  std::normal_distribution<float> nd(0.f, 1.f);
  std::vector<float> xt((size_t)nt * d);
  for (auto &v : xt) v = nd(rng);

  faiss::IndexFlat coarse(d);
  faiss::IndexIVFPQ ref(&coarse, d, nlist, M, nbits);
  ref.train(nt, xt.data());
  ASSERT_TRUE(ref.is_trained);

  // Source: a GammaIVFPQIndex holding ref's training artifacts.
  vearch::GammaIVFPQIndex src;
  AdoptTrainingArtifacts(src, ref);

  // Dump training artifacts to a file (no inverted lists).
  const std::string path = testing::TempDir() + "ta_ivfpq.bin";
  ASSERT_TRUE(src.Dump(path, /*training_only=*/true).ok());

  // Load into a fresh, empty index.
  vearch::GammaIVFPQIndex dst;
  int64_t load_num = 0;
  ASSERT_TRUE(dst.Load(path, /*training_only=*/true, load_num).ok());

  faiss::IndexIVFPQ &dbase = dst;
  EXPECT_EQ(dbase.d, d);
  EXPECT_EQ((int)dbase.nlist, nlist);
  EXPECT_EQ(dbase.by_residual, ref.by_residual);
  EXPECT_EQ((int)dbase.code_size, (int)ref.code_size);
  EXPECT_TRUE(dbase.is_trained);
  // No inverted lists were read; backfill rebuilds them after swap-in.
  EXPECT_EQ(dst.indexed_vec_count_, 0);

  // PQ codebook is byte-identical.
  ASSERT_EQ(dbase.pq.centroids.size(), ref.pq.centroids.size());
  EXPECT_EQ(dbase.pq.centroids, ref.pq.centroids);

  // Coarse-quantizer centroids are identical.
  std::vector<float> ref_cent((size_t)nlist * d), dst_cent((size_t)nlist * d);
  ref.quantizer->reconstruct_n(0, nlist, ref_cent.data());
  dbase.quantizer->reconstruct_n(0, nlist, dst_cent.data());
  EXPECT_EQ(ref_cent, dst_cent);
}

// --- IVFFLAT: training artifacts is just the coarse centroids (no codebook). ------

void AdoptFlat(vearch::GammaIVFFlatIndex &g, const faiss::IndexIVFFlat &ref) {
  faiss::IndexIVFFlat &base = g;
  base.d = ref.d;
  base.metric_type = ref.metric_type;
  base.nlist = ref.nlist;
  base.nprobe = ref.nprobe;
  base.is_trained = true;
  auto *quant = new faiss::IndexFlat(ref.d, ref.metric_type);
  std::vector<float> centroids((size_t)ref.nlist * ref.d);
  ref.quantizer->reconstruct_n(0, ref.nlist, centroids.data());
  quant->add(ref.nlist, centroids.data());
  base.quantizer = quant;
  base.own_fields = false;
}

TEST(TrainingArtifactsRoundtrip, IVFFlatCentroids) {
  const int d = 16, nlist = 8;
  const int nt = 4000;
  std::mt19937 rng(999);
  std::normal_distribution<float> nd(0.f, 1.f);
  std::vector<float> xt((size_t)nt * d);
  for (auto &v : xt) v = nd(rng);

  faiss::IndexFlat coarse(d);
  faiss::IndexIVFFlat ref(&coarse, d, nlist);
  ref.train(nt, xt.data());
  ASSERT_TRUE(ref.is_trained);

  vearch::GammaIVFFlatIndex src;
  AdoptFlat(src, ref);

  const std::string path = testing::TempDir() + "ta_ivfflat.bin";
  ASSERT_TRUE(src.Dump(path, /*training_only=*/true).ok());

  vearch::GammaIVFFlatIndex dst;
  int64_t load_num = 0;
  ASSERT_TRUE(dst.Load(path, /*training_only=*/true, load_num).ok());

  faiss::IndexIVFFlat &dbase = dst;
  EXPECT_EQ(dbase.d, d);
  EXPECT_EQ((int)dbase.nlist, nlist);
  EXPECT_TRUE(dbase.is_trained);
  EXPECT_EQ(dst.GetIndexedVecCount(), 0);

  std::vector<float> ref_cent((size_t)nlist * d), dst_cent((size_t)nlist * d);
  ref.quantizer->reconstruct_n(0, nlist, ref_cent.data());
  dbase.quantizer->reconstruct_n(0, nlist, dst_cent.data());
  EXPECT_EQ(ref_cent, dst_cent);
}

// --- IVFPQFastScan: centroids + PQ codebook + block params (bbs/M2). ---------

void AdoptFastScan(vearch::GammaIVFPQFastScanIndex &g,
                   const faiss::IndexIVFPQFastScan &ref) {
  faiss::IndexIVFPQFastScan &base = g;
  base.d = ref.d;
  base.metric_type = ref.metric_type;
  base.nlist = ref.nlist;
  base.nprobe = ref.nprobe;
  base.by_residual = ref.by_residual;
  base.code_size = ref.code_size;
  base.pq = ref.pq;
  base.bbs = ref.bbs;
  base.M2 = ref.M2;
  base.is_trained = true;
  g.opq_ = nullptr;  // default ctor leaves opq_ unset; the artifacts dump reads it
  auto *quant = new faiss::IndexFlatL2(ref.d);
  std::vector<float> centroids((size_t)ref.nlist * ref.d);
  ref.quantizer->reconstruct_n(0, ref.nlist, centroids.data());
  quant->add(ref.nlist, centroids.data());
  base.quantizer = quant;
  base.own_fields = false;
}

TEST(TrainingArtifactsRoundtrip, IVFPQFastScanCentroidsAndCodebook) {
  const int d = 16, nlist = 8, M = 4, nbits = 4;  // FastScan is 4-bit
  const int nt = 10000;
  std::mt19937 rng(4242);
  std::normal_distribution<float> nd(0.f, 1.f);
  std::vector<float> xt((size_t)nt * d);
  for (auto &v : xt) v = nd(rng);

  faiss::IndexFlatL2 coarse(d);
  faiss::IndexIVFPQFastScan ref(&coarse, d, nlist, M, nbits);
  ref.train(nt, xt.data());
  ASSERT_TRUE(ref.is_trained);

  vearch::GammaIVFPQFastScanIndex src;
  AdoptFastScan(src, ref);

  const std::string path = testing::TempDir() + "ta_ivfpqfs.bin";
  ASSERT_TRUE(src.Dump(path, /*training_only=*/true).ok());

  vearch::GammaIVFPQFastScanIndex dst;
  dst.opq_ = nullptr;
  int64_t load_num = 0;
  ASSERT_TRUE(dst.Load(path, /*training_only=*/true, load_num).ok());

  faiss::IndexIVFPQFastScan &dbase = dst;
  EXPECT_EQ(dbase.d, d);
  EXPECT_EQ((int)dbase.nlist, nlist);
  EXPECT_EQ(dbase.bbs, ref.bbs);
  EXPECT_EQ(dbase.M2, ref.M2);
  EXPECT_TRUE(dbase.is_trained);
  ASSERT_EQ(dbase.pq.centroids.size(), ref.pq.centroids.size());
  EXPECT_EQ(dbase.pq.centroids, ref.pq.centroids);

  std::vector<float> ref_cent((size_t)nlist * d), dst_cent((size_t)nlist * d);
  ref.quantizer->reconstruct_n(0, nlist, ref_cent.data());
  dbase.quantizer->reconstruct_n(0, nlist, dst_cent.data());
  EXPECT_EQ(ref_cent, dst_cent);
}

// --- BINARYIVF: training artifacts is the binary coarse quantizer (no codebook). --

TEST(TrainingArtifactsRoundtrip, BinaryIVFCentroids) {
  const int d = 32;  // bits; code_size = d/8 = 4 bytes
  const int nlist = 8;
  const int nt = 4000;
  const int code_bytes = d / 8;
  std::mt19937 rng(7);
  std::uniform_int_distribution<int> bd(0, 255);
  std::vector<uint8_t> xt((size_t)nt * code_bytes);
  for (auto &v : xt) v = (uint8_t)bd(rng);

  faiss::IndexBinaryFlat coarse(d);
  faiss::IndexBinaryIVF ref(&coarse, d, nlist);
  ref.train(nt, xt.data());
  ASSERT_TRUE(ref.is_trained);

  vearch::GammaIndexBinaryIVF src;
  {
    faiss::IndexBinaryIVF &base = src;
    base.d = ref.d;
    base.code_size = ref.code_size;
    base.metric_type = ref.metric_type;
    base.nlist = ref.nlist;
    base.nprobe = ref.nprobe;
    base.is_trained = true;
    base.quantizer =
        new faiss::IndexBinaryFlat(*dynamic_cast<faiss::IndexBinaryFlat *>(
            ref.quantizer));
    base.own_fields = false;
  }

  const std::string path = testing::TempDir() + "ta_binaryivf.bin";
  ASSERT_TRUE(src.Dump(path, /*training_only=*/true).ok());

  vearch::GammaIndexBinaryIVF dst;
  int64_t load_num = 0;
  ASSERT_TRUE(dst.Load(path, /*training_only=*/true, load_num).ok());

  faiss::IndexBinaryIVF &dbase = dst;
  EXPECT_EQ(dbase.d, d);
  EXPECT_EQ((int)dbase.nlist, nlist);
  EXPECT_TRUE(dbase.is_trained);

  auto *ref_q = dynamic_cast<faiss::IndexBinaryFlat *>(ref.quantizer);
  auto *dst_q = dynamic_cast<faiss::IndexBinaryFlat *>(dbase.quantizer);
  ASSERT_NE(dst_q, nullptr);
  EXPECT_EQ(ref_q->xb, dst_q->xb);  // binary centroids byte-identical
}

}  // namespace
