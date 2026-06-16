# Vearch Developer Guide

This guide helps contributors locate code paths and understand modification entrypoints.

## Directory Map

```
vearch/
├── cmd/vearch/                 # Main entry: startup and role bootstrap
├── internal/
│   ├── master/                 # Cluster control plane
│   │   ├── cluster_api.go      # Cluster admin HTTP routes
│   │   ├── services/           # Entity services: DB, Space, Partition, PS, users, roles
│   │   └── store/              # etcd client wrapper and distributed lock
│   ├── ps/                     # Partition Server data node
│   │   ├── handler_admin.go    # Admin RPC handlers
│   │   ├── handler_document.go # Document RPC handlers
│   │   ├── storage/raftstore/  # Raft storage, write, read, apply, snapshot jobs
│   │   └── engine/             # PS-facing engine abstraction and Gamma binding
│   ├── router/                 # Stateless routing layer
│   │   └── document/           # HTTP/gRPC document API, query parsing, response packaging
│   ├── client/                 # Cross-component clients and RouterRequest routing core
│   ├── entity/                 # Metadata model and etcd key constants
│   ├── engine/                 # C++ Gamma engine
│   ├── proto/vearchpb/         # Protobuf definitions and generated Go types
│   ├── config/                 # TOML config loading
│   ├── monitor/                # Metric exporters
│   └── pkg/                    # Common utilities
├── api/openapi/                # OpenAPI spec
├── sdk/                        # Multi-language SDKs
├── test/                       # Python integration tests
├── build/build.sh              # C++ + Go build script
├── cloud/                      # Docker and Kubernetes deployment
├── tools/                      # CLI tools
├── examples/                   # SDK examples
└── scripts/benchmarks/         # Benchmark scripts
```

## Common Modification Entrypoints

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
| Modify Gamma C++ engine | `internal/engine/` + cgo bindings under `internal/engine/sdk/go/gamma/` |

## Routing Strategy

- Main file: `internal/client/client.go`
- Replica selection strategies: `Leader`, `NotLeader`, `Random`, `LeastConnection`, `NearestConnection`
- Round-robin counters are per partition through `replicaRoundRobin sync.Map`.
- Faulty nodes have a 30s TTL in `internal/client/ps.go`.

## Master Scheduling

- Main file: `internal/master/services/space_service.go`
- `selectServersForPartition` chooses PS nodes for each partition.
- `filterAndSortServer` accumulates partition counts; it does not include a data-volume dimension.
- Before changing migration behavior, study `internal/master/services/member_service.go::ChangeMember`.

## etcd Keys

- Add constants in `internal/entity/meta.go`.
- Mutate metadata through service-layer APIs using `client.Master().Store` and the interface in `internal/master/store/store.go`.
- Use `internal/master/store/distlock.go` for cross-node coordination.

## RPC Handlers

- PS document operations live in `internal/ps/handler_document.go`.
- PS admin operations live in `internal/ps/handler_admin.go`.
- Client handler constants live in `internal/client/ps.go`.
- RouterRequest routing helpers live in `internal/client/client.go`.

## HTTP APIs

- Master APIs are registered in `internal/master/cluster_api.go` and implemented in `internal/master/services/`.
- Router document APIs are registered in `internal/router/document/doc_http.go` and implemented in `internal/router/document/doc_service.go`.

## Gamma Engine

- C++ source lives in `internal/engine/`.
- Go bindings live in `internal/engine/sdk/go/gamma/`.
- PS-facing engine contracts live in `internal/ps/engine/engine.go`.
- Engine changes require a full build path so cgo links updated Gamma libraries.

## Replica Changes

- Entry point: `internal/master/services/member_service.go::ChangeReplica`.
- Change replica count by one step at a time.
- Add before remove; replica count must never drop below `ReplicaNum`.

## Large Files Worth Reading Selectively

| File | Importance |
|---|---|
| `internal/client/client.go` | Routing core |
| `internal/router/document/doc_query.go` | Query DSL parsing |
| `internal/client/master_cache.go` | Metadata cache |
| `internal/master/services/space_service.go` | Cluster scheduling core |
| `internal/router/document/doc_http.go` | HTTP entry |
| `internal/client/master.go` | Master client |
