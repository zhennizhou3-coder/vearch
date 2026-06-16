# Architecture of Vearch

![arc](../assets/architecture.excalidraw.png)

Vearch is a cloud-native distributed vector database. The control plane is written in Go, and the vector index engine, Gamma, is implemented in C++.

## Components

Vearch has four main interaction surfaces: Client, Router, Partition Server (PS), and Master.

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
```

### Client

Clients use the HTTP API or SDKs, such as the Go SDK and Python SDK. Clients send document read/write/search traffic through Router, and can also call Master directly for control-plane or metadata operations.

### Router

Router is the stateless request routing layer. It exposes the document API, caches metadata, routes writes by partition key, broadcasts searches to target partitions, selects replicas, and merges search results.

### Partition Server (PS)

Partition Server hosts document partitions. Each partition is an independent raft group. Writes are replicated through raft before being applied to the local Gamma engine. Reads and searches are served by the local engine according to the replica selected by Router.

Gamma is the core vector search engine. It stores, indexes, and retrieves vectors and scalar fields.

### Master

Master manages cluster metadata and embeds etcd by default. It handles database and space metadata, partition metadata, PS registration, heartbeat state, recovery, and scheduling decisions such as selecting PS nodes for new partitions.

## Key Properties

- Data path is `Client → Router → PS`; Master is not on the document read/write/search data path.
- Client, Router, and PS can all interact directly with Master for control-plane or metadata operations.
- Router is stateless; failover loses no data.
- Each partition is an independent raft group; replicas synchronize through raft.
- Master embeds etcd unless self-managed etcd is configured.

## Core Data Flows

### Write Path (Upsert)

```
Client → POST /document/upsert
   ↓
Router (doc_http.go → doc_service.go → client.RouterRequest)
   ↓
[1] Look up Space in metadata cache → master_cache.go
[2] PartitionDocs(): partitionID = murmur3(PKey) → binary-search partition slot range
[3] Group docs by partitionID → sendMap
[4] Find partition leader: GetNodeIdsByClientType(Leader)
   ↓
PS RPC (handler_document.go::Bulk)
   ↓
[5] store.Write wraps DocCmd as RaftCommand_WRITE and calls RaftSubmit
[6] Raft replicates the log inside this partition's raft group and commits it
[7] Each replica applies the committed log via raft_state_machine.go::innerApply
[8] Apply calls Engine.Writer().Write to write the local Gamma engine
   ↓
Leader waits for RaftSubmit / future.Response(), then returns
   ↓
Response → Router → Client
```

### Search Path (Vector Search)

```
Client → POST /document/search
   ↓
Router (doc_service.go::search)
   ↓
[1] Look up Space + Partitions in metadata cache
[2] SearchByPartitions(): broadcast to all partitions
[3] Per partition GetNodeIdsByClientType (default: Random)
[4] Concurrent RPCs
   ↓
PS RPC (handler_document.go::Search) — does NOT go through raft
   ↓
[5] gamma reader Search (HNSW/IVF/...)
[6] Return local topK
   ↓
[7] Router merges results from all partitions (mergeSortedArrays)
   ↓
Response → Client
```

### Cluster Scheduling (Space Creation / PS Selection)

```
POST /space/create
   ↓
Master cluster_api.go → space_service.CreateSpace
   ↓
[1] Validate schema + generate partition IDs
[2] filterAndSortServer: sort PS by partition count ascending
[3] selectServersForPartition: pick ReplicaNum servers per partition
    (with anti-affinity zone/rack/host constraints)
[4] Notify each PS to create the partition
[5] waitForPartitionsReady (poll until replicas ready)
[6] Persist Space metadata to etcd
```

## Call-Chain Quick Reference

| User action | Call chain |
|---|---|
| Create DB | `cluster_api.createDB` → `services.DBService.CreateDB` → `etcdstore.Create` |
| Create Space | `cluster_api.createSpace` → `services.SpaceService.CreateSpace` → `selectServersForPartition` → `client.CreatePartition` (per PS) |
| Write document | `doc_http.upsertHandler` → `doc_service.bulk` → `client.RouterRequest.UpsertByPartitions` → PS `handler_document.Bulk` → `raftstore.store_writer` → `gamma.Write` |
| Search documents | `doc_http.searchHandler` → `doc_service.search` → `client.RouterRequest.SearchByPartitions` → concurrent PS `handler_document.Search` → `gamma.Search` → merge |
| Get doc by ID | `doc_http.getHandler` → `doc_service.getDocs` → `client.RouterRequest.PartitionDocs` (murmur3) → PS `handler_document.GetDocs` |
| Replica change | `cluster_api.changeMember` → `services.MemberService.ChangeMember` → `proto.ConfAddNode/RemoveNode` → PS raft conf change |
| Failed PS recovery | `cluster_api.recoverFailServer` → `services.ServerService.RecoverFailServer` → `MemberService.ChangeMember` (add then remove) |

## External Dependencies

| Dependency | Purpose | Path |
|---|---|---|
| `go.etcd.io/etcd` | Embedded etcd | `internal/master/server.go` |
| `cubefs/depends/tiglabs/raft` | Raft protocol | `internal/ps/storage/raftstore` |
| `gin-gonic/gin` | HTTP framework | `internal/master`, `internal/router` |
| `smallnest/rpcx` | RPC framework | `internal/ps`, `internal/client/ps.go` |
| `spaolacci/murmur3` | Consistent hashing | `internal/client/client.go` |
| `prometheus/client_golang` | Metric collection | `internal/monitor/`, `internal/pkg/metrics/` |
| `patrickmn/go-cache` | In-memory cache | `internal/client/master_cache.go` |
| `BurntSushi/toml` | Config parsing | `internal/config/` |
| `uber/jaeger-client-go` | Distributed tracing | `cmd/vearch/startup.go` |
