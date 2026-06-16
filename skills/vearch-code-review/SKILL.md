---
name: vearch-code-review
description: Use when reviewing Vearch PRs/diffs or preparing code for review. Anchors checks to Vearch-specific invariants — raft write path, anti-affinity, generated `.pb.go`, cgo `-tags="vector"`, etcd key access, replica change ordering — in addition to general correctness, testing, and style.
---

# Vearch Code Review Checklist

Walk through every section. For each box, either tick it or write a one-line note explaining why it does not apply. If a Vearch-specific invariant is violated, that is a blocker — not a nit.

## Vearch Invariants (blockers)

These map 1:1 to the invariants listed in `CLAUDE.md` § "Invariants You Must Never Break". A change that violates any of these must not merge.

- [ ] All data mutations flow through `internal/ps/storage/raftstore` (e.g. `store_writer.go`); the change does not write directly to the Gamma engine.
- [ ] Read paths that bypass raft (using `store_read.go`) are intentional and documented; freshness expectations match `RaftConsistent`.
- [ ] Member/replica changes preserve `ReplicaNum` (add-then-remove ordering in `member_service.go::ChangeReplica`).
- [ ] Migration / placement decisions preserve zone/rack/host anti-affinity (`space_service.go::selectServersForPartition`).
- [ ] No migration crosses `ResourceName` boundaries.
- [ ] Metadata mutations go through service-layer APIs (`internal/master/services/*`), not direct etcd writes — caches and watches must stay consistent.
- [ ] No hand-edits to generated protobuf files (`internal/proto/vearchpb/*.pb.go`); `.proto` was edited and regenerated instead.
- [ ] C++ Gamma engine changes (`internal/engine/`) are accompanied by a full rebuild so cgo links the new shared library.

## Contract Boundaries

- [ ] Each behavior is owned by the right layer — Master controls metadata, Router routes, PS owns data; the change does not blur these.
- [ ] Comments and identifiers describe the local contract, not the rationale of one specific caller. Generic helpers do not name a current use case.
- [ ] New cross-component touchpoints (Master ↔ Router/PS, Router ↔ PS) reuse `internal/client` patterns (`RouterRequest`, `client.Master().Store`) rather than ad-hoc HTTP/RPC.

## Correctness

- [ ] Database semantics preserved: writes are durable through raft commit before ack; reads honor the requested consistency mode.
- [ ] Error cases use `vearchpb.NewError(ErrorEnum_XXX, err)`; HTTP mapping in `cluster_api.go::handleError` covers any new error enums.
- [ ] Concurrency: shared state has explicit synchronization; no goroutine writes data the raft apply path also touches without coordination.
- [ ] No data races, deadlocks, or lock-order inversions introduced (review with `go test -race` where feasible).
- [ ] Context propagation: long-running RPCs honor `ctx` cancellation/deadline.

## Testing

- [ ] Go unit tests cover the changed package (`go test ./<pkg>/... -race`).
- [ ] Integration tests in `test/` exercise the user-visible behavior; the relevant `pytest` file was run against a standalone Vearch.
- [ ] Edge cases covered: empty input, partition-not-found, leader-stepping-down, replica-failure, oversized payloads.
- [ ] CGo / engine changes also exercised via `build/gamma_build && ctest` when applicable.
- [ ] CI matrix (amd64 + arm64) is green; no platform-specific shortcuts.

## Performance

- [ ] Hot paths (search merge, raft apply, partition routing) avoid unnecessary allocations and copies; benchmark numbers attached for sensitive changes.
- [ ] No new per-request goroutines without bounded fan-out or pooling.
- [ ] No metadata-cache bypass that would force a Master round-trip on every request.

## API and Compatibility

- [ ] Public HTTP API change is backwards compatible, OR an explicit deprecation/version note is added under `api/openapi/`.
- [ ] New fields on Space / Partition / DB metadata round-trip safely through the existing etcd entries (older masters do not crash on unknown fields).
- [ ] SDK changes (`sdk/go`, `sdk/python`, etc.) are kept in sync if the wire format changes.
- [ ] Feature flags / config keys live under the right `[section]` in `config/config.toml`.

## Code Quality

- [ ] Apache 2.0 license header present on every new source file.
- [ ] Go is `gofmt`-clean and passes the project's vet/lint.
- [ ] Frameworks match the codebase: `gin-gonic/gin` for HTTP, `smallnest/rpcx` for RPC, `internal/pkg/log` for logging.
- [ ] CGo build remains buildable with `-tags="vector"`.
- [ ] Comments explain non-obvious WHY (raft constraints, anti-affinity reasons, perf workarounds); they do not narrate WHAT the code already says.
