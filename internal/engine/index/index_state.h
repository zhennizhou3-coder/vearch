/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace vearch {

// Build state of a dynamically-added index. Shared by the scalar and vector
// index managers so both report through the same EngineStatus surface. A newly
// added index is registered as BUILDING, becomes READY once its build (scalar:
// publish-then-backfill; vector: create+train+swap) completes, or FAILED if the
// build is rolled back. Values are stringified verbatim into the describe API —
// keep in sync with IndexStateToString.
//
// Note on query gating: the scalar manager additionally treats non-READY
// indexes as absent during queries (its publish-then-backfill exposes a
// half-built index). The vector manager does NOT need that — it swaps the fully
// built index in atomically under a write lock, so search never sees a partial
// one. Here the state is observability only for the vector side.
enum class IndexState : int { BUILDING = 0, READY = 1, FAILED = 2 };

inline const char *IndexStateToString(IndexState s) {
  switch (s) {
    case IndexState::BUILDING:
      return "BUILDING";
    case IndexState::READY:
      return "READY";
    case IndexState::FAILED:
      return "FAILED";
    default:
      return "UNKNOWN";
  }
}

// Index lifecycle status. Two consumers share this single definition:
//   - The engine-wide index_status_ (Engine), which only ever reports the first
//     three states (initial build lifecycle).
//   - Per-vector-index tracking in VectorManager, whose rebuild path adds FAILED
//     as a terminal state so the rebuild monitor can distinguish a failed build
//     from one still in flight. Field-level rebuild flips only the target
//     index's status; sibling indexes are untouched.
// Unscoped on purpose: engine.h declares `enum IndexStatus index_status_` and
// tools reference the bare enumerators (e.g. INDEXED). Values are stringified
// into the per-index EngineStatus surface — keep in sync with
// IndexStatusToString.
enum IndexStatus { UNINDEXED = 0, INDEXING = 1, INDEXED = 2, FAILED = 3 };

inline const char *IndexStatusToString(IndexStatus s) {
  switch (s) {
    case UNINDEXED:
      return "UNINDEXED";
    case INDEXING:
      return "INDEXING";
    case INDEXED:
      return "INDEXED";
    case FAILED:
      return "FAILED";
    default:
      return "UNKNOWN";
  }
}

}  // namespace vearch
