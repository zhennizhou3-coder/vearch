---
name: architecture
description: Use when working under `internal/engine/` — exploring the Gamma C++ index engine layout, touching a vector index family (FLAT / IVFFLAT / IVFPQ / IVFPQFastScan / IVFRABITQ / BINARYIVF / HNSW / GPU_IVFFLAT / GPU_IVFPQ / SCANN / DISKANN_STATIC), scalar/filter indexes (Bitmap / Inverted / Composite), the IndexModel interface, the reflector / REGISTER_INDEX flow, raw-vector storage backends, or the realtime inverted index.
---

# Vearch Index Architecture (Gamma Engine)

> Vearch's vector path is implemented by the C++ "Gamma" engine under `internal/engine/`, called from Go via cgo (`internal/engine/c_api/`). This skill is the operational entrypoint for engine-internal work. For the full per-family reference, see [`docs/IndexLayer.md`](../../../docs/IndexLayer.md). For build flags and third-party deps, see [`docs/Development.md`](../../../docs/Development.md).

## Directory Layout

| Directory | Purpose |
|---|---|
| `internal/engine/c_api/` | C ABI exposed to Go (`gamma_api.{h,cc}`); single cgo entrypoint. |
| `internal/engine/c_api/api_data/` | Hand-written C++ data wrappers (`cpp_api.{cc,h}`, `doc.{cc,h}`, `request.{cc,h}`, `response.{cc,h}`). Editable; not generated. |
| `internal/engine/search/` | `Engine` (`engine.{h,cc}`) — top-level façade orchestrating table, vector manager, scalar indexes. |
| `internal/engine/index/` | Index framework: `index_model.h` (abstract `IndexModel`), `reflector.{h,cc}` (registration), `index_io.{h,cc}` (faiss-compatible serializers), `index.{h,cc}` (faiss-like wrappers), `realtime/` (real-time inverted index for IVF). |
| `internal/engine/index/impl/` | Concrete vector index implementations. One subdirectory or `gamma_index_*` file per family. |
| `internal/engine/vector/` | Raw vector storage (`MemoryOnly`, `MemoryBuffer`, `RocksDB`) + `VectorManager` that owns indexes. |
| `internal/engine/storage/` | `StorageManager` — RocksDB-backed columnfamily store used by scalar indexes and persistent vector storage. |
| `internal/engine/table/` | Scalar (table-side) indexes: `BitmapIndex`, `InvertedIndex`, `CompositeIndex`, plus `ScalarIndexManager`. |
| `internal/engine/idl/fbs/` | FlatBuffers IDL (`table.fbs`, `doc.fbs`, …). Generated outputs in `idl/fbs-gen/`. |
| `internal/engine/idl/pb-gen/` | Generated protobuf types used inside the engine. |
| `internal/engine/third_party/` | Vendored deps (faiss, hnswlib, rocksdb, …). |
| `internal/engine/tests/` | C++ engine unit tests; `cd build/gamma_build && ctest`. |
| `internal/engine/sdk/` | Engine-level SDK bindings (Go cgo bindings, Python). |

## Core Abstractions

### `IndexModel` — abstract vector index (`internal/engine/index/index_model.h`)

Every concrete vector index inherits `vearch::IndexModel`. Required virtuals:

- `Init`, `Parse` — parameter handling.
- `Indexing` — train / build.
- `Add`, `Update`, `Delete` — mutation.
- `Search` — must consult `RetrievalContext::IsValid` and `IsSimilarScoreValid`.
- `Dump`, `Load` — persistence.
- `GetTotalMemBytes` — used by metrics / autoscale.

Holds `VectorReader *vector_` (raw vectors), `tbb::concurrent_bounded_queue<int64_t> updated_vids_` for async updates, plus `indexed_count_`, `start_docid_`, `training_threshold_`, `support_increment_`.

Companion classes in the same header: `RetrievalParameters` (per-search knobs, e.g. `efSearch`, `nprobe`), `RetrievalContext` (filters + `PerfTool`), `VectorMetaInfo`, `ScopeVectors`, `VectorReader`.

### Reflector / Registration (`internal/engine/index/reflector.{h,cc}`)

Singleton `Reflector &reflector()` holds `name → IndexFactory*`. Each concrete index registers via the `REGISTER_INDEX(NAME, ClassName)` macro at file scope. Examples:

- `REGISTER_INDEX(IVFPQ, GammaIVFPQIndex)` in `impl/gamma_index_ivfpq.cc`.
- `REGISTER_INDEX(HNSW, GammaIndexHNSWLIB)` in `impl/hnswlib/gamma_index_hnswlib.cc`.

`VectorManager::CreateVectorIndex` calls `reflector().GetNewIndex(index_type)` to instantiate. Misspelled names produce `cannot get model=<name>` at create time.

### Faiss-like Wrappers (`internal/engine/index/index.{h,cc}`)

`vearch::Index` plus `IndexIVFFlat` / `IndexIVFPQ` / `IndexScann` provide a faiss-style API (`train` / `add` / `search` taking `idx_t`, `float*`) on top of the Gamma `IndexModel` classes. Used by tools/benchmarks and the optional standalone `BUILD_FAISSLIKE_INDEX` build.

`index_factory(d, description, metric)` parses faiss-style strings such as `"IVF1024,PQ32x8"`.

These wrappers do **not** participate in `REGISTER_INDEX`; they cannot be selected via `Table.index_type`.

### I/O Helpers (`internal/engine/index/index_io.{h,cc}`)

`WRITEANDCHECK` / `READANDCHECK` macros, `write_index_header`, `read_ivf_header`, `write_hnsw`, `write_product_quantizer`, `write_RaBitQuantizer`, `WriteInvertedLists` / `ReadInvertedLists`. Faiss-binary-compatible where possible.

### Real-time Inverted Index (`internal/engine/index/realtime/`)

`RTInvertIndex` provides lock-free incremental insertion for IVF families. Reference: *Design and Implementation of a Real Time Visual Search System on JD E-commerce Platform*, arXiv:1908.07389.

### Top-level Engine (`internal/engine/search/engine.{h,cc}`)

`Engine` aggregates `Table`, `VectorManager`, `StorageManager`, `ScalarIndexManager`. All cgo entry points in `gamma_api.cc` ultimately call `Engine` methods.

## Vector Storage Backends

`enum VectorStorageType` is selected automatically by `VectorManager::DetermineVectorStorageType` based on index type:

| Backend | When used | Source |
|---|---|---|
| `MemoryOnly` | Default for in-memory indexes (HNSW, FLAT). | `vector/memory_raw_vector.{h,cc}` |
| `MemoryBuffer` | Realtime "buffer" vector kept alongside batch index for incremental writes (FLAT companion to IVF families when `enable_realtime`). | `vector/memory_buffer_raw_vector.{h,cc}` |
| `RocksDB` | Persistent vector storage (large datasets, restart durability). | `vector/rocksdb_raw_vector.{h,cc}` |

All concrete classes inherit `RawVector` (which itself implements `VectorReader`). `RawVectorFactory::Create` (`vector/raw_vector_factory.h`) is the single creation point.

## Operational Sub-skills

- [adding-an-index.md](adding-an-index.md) — recipe for plugging a new vector index family into the engine (IndexModel, REGISTER_INDEX, CMake, storage choice, tests).

## Where to Read More

- Per-family parameters, metrics, data types, and scalar (filter) indexes — [`docs/IndexLayer.md`](../../../docs/IndexLayer.md).
- CMake options and third-party libraries (faiss, hnswlib, RocksDB, DiskANN, ScaNN, OpenBLAS, TBB, roaring, CUDA) — [`docs/Development.md`](../../../docs/Development.md) "Engine Build Options" section.

## Invariant Reminder

Writes still flow through raft (`CLAUDE.md` §5 invariant 1). This skill describes the *engine-internal* index path that runs *after* raft apply.
