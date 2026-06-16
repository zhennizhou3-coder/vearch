# CLAUDE.md — Vearch Project Navigation

> Vearch is a cloud-native distributed vector database. The control plane is written in Go; the vector index engine (Gamma) is implemented in C++ under `internal/engine/`.
>
> **Purpose**: keep always-loaded guidance short. Use this file for project orientation, invariants, and high-frequency commands. Use `docs/Architecture.md`, `docs/Development.md`, and `docs/DeveloperGuide.md` for deeper references.

---

## 1. Project Snapshot

| Item | Value |
|---|---|
| Version | 3.5.9 |
| Language | Go 1.22+ + C++17 Gamma engine |
| Module path | `github.com/vearch/vearch/v3` |
| Build entry | `cmd/vearch/startup.go` |
| Sample configs | `config/config.toml`, `config/config_cluster.toml` |
| Integration tests | `test/` Python pytest |
| Deployment | Single binary, launched by role: `master` / `ps` / `router` / `all` |
| Metadata store | etcd embedded in master by default; self-managed etcd is optional |
| Consensus | Raft via `cubefs/depends/tiglabs/raft` |
| Default ports | master HTTP `8817`, router HTTP `9001`, ps RPC `8081`, raft `8898/8899` |
| Pprof ports | master `6062`, router `6061`, ps `6060` |

---

## 2. Architecture

```
Legend: ──> data path    - - / ┆ control / metadata path

                    ┌─────────────┐                  ┌─────────────┐
                    │   Client    │ - - - - - - - -> │   Master    │
                    │ HTTP / SDK  │                  │ gin + etcd  │
                    └──────┬──────┘                  └──────▲──────┘
                           │                                ┆
                           ▼                                ┆
                    ┌─────────────┐                        ┆
                    │   Router    │ - - - - - - - - - - - -┆
                    │ gin HTTP    │                        ┆
                    └──┬───┬──┬───┘                        ┆
                       │   │  │                             ┆
            ┌──────────┘   │  └──────────┐                  ┆
            ▼              ▼             ▼                  ┆
        ┌─────────────┐ ┌─────────────┐ ┌─────────────┐     ┆
        │     PS      │ │     PS      │ │     PS      │ - - ┆
        │ raft+gamma  │ │ raft+gamma  │ │ raft+gamma  │
        └─────────────┘ └─────────────┘ └─────────────┘

Client: HTTP API / Go SDK / Python SDK.
Router: Stateless routing layer, metadata cache, partition routing, replica routing.
PS: Partition Server, one raft group per partition, Gamma C++ index engine.
Master: Cluster management, metadata, PS registration, heartbeat, and recovery.
```

**Key facts**:
- Data path is `Client → Router → PS`; Master is not on the document read/write/search data path.
- Client, Router, and PS can all interact directly with Master for control-plane or metadata operations.
- Master embeds etcd (`go.etcd.io/etcd/server/v3/embed`); no need to deploy etcd separately unless `self_manage_etcd = true`.
- Each partition is an independent raft group; replicas synchronize through raft.
- Router is stateless; failover loses no data.
- Gamma is the C++ vector engine, called from Go through cgo bindings.

---

## 3. Where to Change Things

| Area | Start here | Notes |
|---|---|---|
| Process startup / role bootstrap | `cmd/vearch/startup.go` | One binary starts `master`, `ps`, `router`, or `all`. |
| Master API / control plane | `internal/master/cluster_api.go`, `internal/master/services/` | DB, Space, Partition, PS, user, role, backup services. |
| Space creation / PS selection | `internal/master/services/space_service.go` | Scheduling and anti-affinity logic. |
| Raft member changes | `internal/master/services/member_service.go` | Add/remove node, replica changes. |
| Router HTTP document API | `internal/router/document/doc_http.go`, `doc_service.go` | HTTP routes and business logic. |
| Router request routing | `internal/client/client.go` | `RouterRequest`, partitioning, broadcast, replica selection. |
| Metadata cache | `internal/client/master_cache.go` | Router/client cache behavior. |
| PS document RPC | `internal/ps/handler_document.go` | Write, query, search RPC handlers. |
| PS admin RPC | `internal/ps/handler_admin.go` | Partition create/delete and replica config. |
| Raft write path | `internal/ps/storage/raftstore/store_writer.go` | All data mutations must pass through raft. |
| Raft apply path | `internal/ps/storage/raftstore/raft_state_machine.go` | Committed log application. |
| Local read path | `internal/ps/storage/raftstore/store_read.go` | Reads go directly to engine. |
| Gamma Go binding | `internal/ps/engine/gammacb/` | PS-facing engine implementation. |
| Gamma C++ engine | `internal/engine/` | Vector index engine source. See `docs/IndexLayer.md` for the index reference; load the `architecture` skill for engine-internal work. |
| Data model / etcd keys | `internal/entity/` | DB, Space, Partition, Server metadata. |
| Protobuf generated types | `internal/proto/vearchpb/` | Do not hand-edit `.pb.go` files. |

---

## 4. Key Conventions

### etcd Key Space (`internal/entity/meta.go`)

| Prefix | Purpose |
|---|---|
| `/server/<id>` | PS node registration with TTL |
| `/router/<name>/<addr>` | Router heartbeat with 10s TTL |
| `/db/id/<id>` | DB ID → metadata |
| `/db/name/<name>` | DB Name → ID |
| `/space/<dbid>/<spaceid>` | Space metadata |
| `/partition/<id>` | Partition metadata |
| `/lock/...` | Distributed lock |
| `/cluster/clean_job` | Background STM timestamp gating |

### Master Service Pattern

All `internal/master/services/*` service structs hold `client *client.Client`. When modifying master business logic, look first for the matching service file.

```go
type SpaceService struct { client *client.Client }
func NewSpaceService(c *client.Client) *SpaceService { ... }
func (s *SpaceService) CreateSpace(ctx ...) error { ... }
```

### Router Request Pattern

`client.RouterRequest` is the routing-side builder pattern:

```go
request := client.NewRouterRequest(ctx, c)
request.SetMsgID(...).SetMethod(...).SetHead(...).SetSpace().SetDocs(...).PartitionDocs()
items := request.Execute()
```

### Errors and Protobuf

- Error definitions are in `internal/proto/vearchpb/errors.pb.go`.
- Wrap business errors with `vearchpb.NewError(ErrorEnum_XXX, err)`.
- HTTP status mapping is in `internal/master/cluster_api.go::handleError`.
- Regenerate protobuf from `.proto` sources when present; do not hand-edit generated `.pb.go` files.

### Code Style

- Apache 2.0 license header on source files.
- Go code must be `gofmt` formatted.
- HTTP framework: `gin-gonic/gin`.
- RPC framework: `smallnest/rpcx`.
- Logger: `internal/pkg/log`.
- CGo engine builds require `-tags="vector"`.

---

## 5. Invariants You Must Never Break

1. **Writes always go through raft** — data mutation paths must go through `internal/ps/storage/raftstore`; never write directly to the engine.
2. **Reads can be local** — read paths use `store_read.go` and the engine directly; read freshness depends on routing and `RaftConsistent`.
3. **Replica count must not drop below `ReplicaNum`** — for member changes, add first and remove second.
4. **Anti-affinity must be preserved** — migration targets must preserve zone/rack/host isolation.
5. **No cross-ResourceName migration** — migration cannot cross resource pools.
6. **Do not hand-modify etcd keys** — mutate metadata through service-layer APIs so caches and watches stay consistent.
7. **Do not hand-edit generated protobuf files** — edit `.proto` and regenerate.
8. **C++ engine changes require relinking** — run a full build path so cgo links updated Gamma libraries.

---

## 6. Common Commands

```bash
# Full build, including Gamma C++ engine
make all
make all j=8

# Build Go binary only, skipping engine rebuild
cd build && ./build.sh -g OFF

# Standalone local run
./build/bin/vearch -conf=config/config.toml all

# Individual roles
./build/bin/vearch -conf=config/config.toml master
./build/bin/vearch -conf=config/config.toml ps
./build/bin/vearch -conf=config/config.toml router

# Integration tests; requires a running local Vearch instance
make test
cd test && pytest test_document_search.py -x --log-cli-level=INFO

# Go unit tests
go test ./internal/entity/... -v

# Go SDK tests
cd sdk/go/test && go test -v

# Engine tests
cd build && ./build.sh -t
cd build/gamma_build && ctest
```

For full command options, Docker commands, debug endpoints, and CI details, see `docs/Development.md`.

---

## 7. Common Task Entrypoints

| Task | Start here |
|---|---|
| Add a Master HTTP API | `internal/master/cluster_api.go` + matching `internal/master/services/*_service.go` |
| Add a Router document API | `internal/router/document/doc_http.go` + `doc_service.go` |
| Add a PS RPC handler | `internal/ps/handler_document.go` or `handler_admin.go`; client changes in `internal/client/ps.go` and `internal/client/client.go` |
| Change routing strategy | `internal/client/client.go` `GetNodeIdsByClientType` |
| Change PS selection for Space creation | `internal/master/services/space_service.go` |
| Change replica count | `internal/master/services/member_service.go::ChangeReplica` |
| Add an etcd key | `internal/entity/meta.go` + service-layer store access |
| Debug slow PS writes | metrics → PS pprof `:6060` → raft jobs → Gamma writer |
| Modify Gamma C++ engine | `internal/engine/` + cgo bindings under `internal/engine/sdk/go/gamma/`; load the `architecture` skill first |
| Add a new vector index family | `architecture` skill → `adding-an-index.md` (registers via `REGISTER_INDEX` in `internal/engine/index/impl/`); per-family reference in `docs/IndexLayer.md` |

For detailed task recipes and large-file reading order, see `docs/DeveloperGuide.md`.

---

## 8. Deeper References

- `docs/Architecture.md` — architecture, data/control paths, write/search/scheduling flows, call chains, external dependencies.
- `docs/Development.md` — build flags, run modes, Docker, tests, debug endpoints, CI; engine CMake options and third-party deps.
- `docs/DeveloperGuide.md` — code map, modification entrypoints, task recipes, large-file reading order.
- `docs/IndexLayer.md` — Gamma index-layer reference: registered vector indexes, scalar/filter indexes, storage backends.

## 9. Skills (load on demand)

Project-local Claude skills under `.claude/skills/`. **Not** auto-loaded — invoke when the task matches the trigger.

### `architecture` — engine-internal entrypoint

**Trigger**: any work under `internal/engine/` — exploring engine layout, touching a vector index family (FLAT / IVFFLAT / IVFPQ / IVFPQFastScan / IVFRABITQ / BINARYIVF / HNSW / GPU\_\* / SCANN / DISKANN_STATIC), scalar/filter indexes (Bitmap / Inverted / Composite), the `IndexModel` interface, the reflector / `REGISTER_INDEX` flow, raw-vector storage backends, or the realtime inverted index.

**Files**:

- `SKILL.md` — directory layout, core abstractions, storage backends; pointers to `docs/IndexLayer.md` and `docs/Development.md` for reference material.
- `adding-an-index.md` — recipe: implement `IndexModel`, `REGISTER_INDEX(NAME, Class)`, CMake wiring, storage choice, tests, documentation.

**Invariant reminder for the engine**: writes still flow through raft (§5 invariant 1); the skill describes the *engine-internal* index path that runs *after* raft apply.

---

**Document version**: concise Claude navigation for Vearch v3.
