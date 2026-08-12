/**
 * Copyright (c) The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

/**
 * Tests for the index-rebuild OpenMP thread-cap contract (engine.h):
 *
 *   RebuildTrainThreadCap(limit_cpu, num_cores):
 *     - limit_cpu > 0  -> min(limit_cpu, num_cores): the RPC value is honored but
 *                         clamped to the physical core count, so it can never
 *                         oversubscribe the CPU.
 *     - limit_cpu <= 0 -> falls back to max(1, num_cores * 3 / 4).
 *     - The max(1, ...) floor never yields 0 threads, even for tiny core counts.
 *
 *   OmpThreadScope(limit):
 *     - Caps the calling thread's omp_get_max_threads() to `limit` for the scope.
 *     - Restores the previous value on scope exit (RAII), including nesting.
 *     - A non-positive limit leaves the thread count unchanged.
 *
 * These are the exact behaviors Engine::RebuildIndex relies on to keep a
 * CPU-heavy training phase from saturating every core and starving raft apply.
 */

#include <gtest/gtest.h>

#include "omp.h"
#include "search/engine.h"

namespace test {

using vearch::OmpThreadScope;
using vearch::RebuildTrainThreadCap;

namespace {

// ---- RebuildTrainThreadCap: RPC-provided limit_cpu, clamped to cores ----

TEST(RebuildTrainThreadCap, PositiveLimitCpuHonoredWithinCores) {
  EXPECT_EQ(RebuildTrainThreadCap(1, 8), 1);
  EXPECT_EQ(RebuildTrainThreadCap(2, 8), 2);
  // Up to the core count the RPC value is used as-is.
  EXPECT_EQ(RebuildTrainThreadCap(8, 8), 8);
}

TEST(RebuildTrainThreadCap, PositiveLimitCpuClampedToCores) {
  // An RPC value above the core count is clamped down: asking for more threads
  // than cores only oversubscribes the CPU and worsens raft-apply starvation.
  EXPECT_EQ(RebuildTrainThreadCap(100, 8), 8);
  EXPECT_EQ(RebuildTrainThreadCap(9, 8), 8);
  // Clamped to 1 when only a single core is available.
  EXPECT_EQ(RebuildTrainThreadCap(100, 1), 1);
}

// ---- RebuildTrainThreadCap: fallback to max(1, cores*3/4) ----

TEST(RebuildTrainThreadCap, FallbackIsThreeQuartersOfCores) {
  EXPECT_EQ(RebuildTrainThreadCap(0, 4), 3);
  EXPECT_EQ(RebuildTrainThreadCap(0, 8), 6);
  EXPECT_EQ(RebuildTrainThreadCap(0, 16), 12);
  // 3/4 truncates toward zero.
  EXPECT_EQ(RebuildTrainThreadCap(0, 6), 4);   // 6*3/4 = 4 (4.5 truncated)
}

TEST(RebuildTrainThreadCap, NegativeLimitCpuAlsoFallsBack) {
  EXPECT_EQ(RebuildTrainThreadCap(-1, 8), 6);
  EXPECT_EQ(RebuildTrainThreadCap(-100, 16), 12);
}

TEST(RebuildTrainThreadCap, FloorNeverYieldsZeroThreads) {
  // cores*3/4 is 0 for cores < 2; the max(1, ...) floor must clamp to 1 so we
  // never call omp_set_num_threads(0).
  EXPECT_EQ(RebuildTrainThreadCap(0, 1), 1);
  EXPECT_EQ(RebuildTrainThreadCap(0, 2), 1);   // 2*3/4 = 1
  EXPECT_EQ(RebuildTrainThreadCap(-5, 1), 1);
}

// ---- OmpThreadScope: cap-and-restore ----

TEST(OmpThreadScope, CapsInsideAndRestoresOnExit) {
  const int prev = omp_get_max_threads();
  // Pick a target distinct from the current value; omp_set_num_threads sets the
  // ICV directly and is not clamped to the hardware core count.
  const int target = (prev == 3) ? 5 : 3;
  {
    OmpThreadScope scope(target);
    EXPECT_EQ(omp_get_max_threads(), target);
  }
  EXPECT_EQ(omp_get_max_threads(), prev);
}

TEST(OmpThreadScope, NonPositiveLimitLeavesCountUnchanged) {
  const int prev = omp_get_max_threads();
  {
    OmpThreadScope scope(0);
    EXPECT_EQ(omp_get_max_threads(), prev);
  }
  EXPECT_EQ(omp_get_max_threads(), prev);
  {
    OmpThreadScope scope(-4);
    EXPECT_EQ(omp_get_max_threads(), prev);
  }
  EXPECT_EQ(omp_get_max_threads(), prev);
}

TEST(OmpThreadScope, NestedScopesRestoreLayerByLayer) {
  const int prev = omp_get_max_threads();
  const int outer = (prev == 7) ? 5 : 7;
  const int inner = (outer == 4) ? 2 : 4;
  {
    OmpThreadScope outer_scope(outer);
    EXPECT_EQ(omp_get_max_threads(), outer);
    {
      OmpThreadScope inner_scope(inner);
      EXPECT_EQ(omp_get_max_threads(), inner);
    }
    // Inner scope restores to the outer cap, not to prev.
    EXPECT_EQ(omp_get_max_threads(), outer);
  }
  EXPECT_EQ(omp_get_max_threads(), prev);
}

}  // namespace

}  // namespace test
