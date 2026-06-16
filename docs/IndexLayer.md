# Vearch Index Layer Reference

> Reference for the Gamma C++ engine's index layer under `internal/engine/`.
> Pair this with the operational `architecture` skill at `.claude/skills/architecture/` (entrypoint, recipe for adding a new index family).

## Vector Index Types

Each vector index registers itself with the reflector via `REGISTER_INDEX(NAME, Class)`. The `Registered name` column below is the exact string stored in `Table.index_type` / `IndexInfo.type` (see `internal/engine/idl/fbs/table.fbs`).

### Registered Indexes

| Registered name | Class | Source | Backed by | Best for | Build flag |
|---|---|---|---|---|---|
| `FLAT` | `GammaFLATIndex` | `impl/gamma_index_flat.{h,cc}` | brute-force scan over `RawVector` | small datasets, 100% recall, baseline | always |
| `IVFFLAT` | `GammaIVFFlatIndex` | `impl/gamma_index_ivfflat.{h,cc}` | faiss `IndexIVFFlat` + `RTInvertIndex` | balanced recall/throughput, supports realtime add | always |
| `IVFPQ` | `GammaIVFPQIndex` | `impl/gamma_index_ivfpq.{h,cc}` | faiss `IndexIVFPQ` (or `Relayout` variant) + `RTInvertIndex` | large datasets with memory pressure | always (relayout via `OPT_IVFPQ_RELAYOUT`) |
| `IVFPQFastScan` | `GammaIVFPQFastScanIndex` | `impl/gamma_index_ivfpqfs.{h,cc}` | faiss `IndexIVFPQFastScan` | high-throughput PQ search using SIMD-fast lookup tables | always |
| `IVFRABITQ` | `GammaIVFRABITQIndex` | `impl/gamma_index_ivfrabitq.{h,cc}` | faiss `RaBitQuantizer` over IVF | extreme compression (~32×) | always |
| `BINARYIVF` | `GammaIndexBinaryIVF` | `impl/gamma_index_binary_ivf.{h,cc}` | faiss `IndexBinaryIVF` | Hamming-distance binary vectors | always |
| `HNSW` | `GammaIndexHNSWLIB` | `impl/hnswlib/gamma_index_hnswlib.{h,cc}` | vendored `hnswlib::HierarchicalNSW<float>` | low-latency, high-recall in-memory ANN | always |
| `GPU_IVFFLAT` | `GammaIVFFlatGPUIndex` | `impl/gpu/gamma_index_ivfflat_gpu.{h,cc}` | faiss-GPU IVFFlat | high-throughput GPU search | `BUILD_WITH_GPU=on` |
| `GPU_IVFPQ` | `GammaIVFPQGPUIndex` | `impl/gpu/gamma_index_ivfpq_gpu.{h,cc}` | faiss-GPU IVFPQ | GPU + memory-compressed | `BUILD_WITH_GPU=on` |
| `DISKANN_STATIC` | `GammaIndexDiskANNStatic` | `impl/diskann/gamma_index_diskann_static.{h,cc}` | preinstalled DiskANN (`pq_flash_index.h`) | billion-scale on SSD with limited RAM | `BUILD_WITH_DISKANN=on` (default on) |

### Faiss-Like Wrappers (Not Registered)

A few faiss-style classes exist in `internal/engine/index/index.{h,cc}` and are **not** dispatched through `reflector()`. They are built only for tools and benchmarks.

| Class | Source | Backed by | Build flag |
|---|---|---|---|
| `vearch::IndexScann` / `GammaVearchIndex` | `impl/scann/gamma_index_vearch.{h,cc}`, `index/index.{h,cc}` | Google ScaNN via vendored API | `BUILD_WITH_SCANN=on` (defines `USE_SCANN`) |

If you write `Table.index_type = "Scann"`, `VectorManager::CreateVectorIndex` will fail with `cannot get model=Scann`. ScaNN must be invoked through the faiss-like wrapper API.

### Family Notes

#### FLAT

- No training; `Indexing()` is a no-op. Search iterates `RawVector` and applies bitmap / score filters from `RetrievalContext`.
- Used as the **realtime buffer companion** for IVF families when `Table.enable_realtime` is on. `VectorManager` (in its IVF setup branch) creates a second `FLAT` index over a `MemoryBuffer` raw vector that absorbs new writes until the next batch indexing.

#### IVF Series (IVFFLAT / IVFPQ / IVFPQFastScan / IVFRABITQ / BINARYIVF)

- Training: K-means coarse quantizer (`ncentroids` clusters). `IndexModel::Indexing()` triggers `train` on accumulated vectors once `training_threshold_` is reached.
- Key params (JSON in `index_params`): `metric_type` (`L2` | `InnerProduct`), `ncentroids`, `nsubvector` (PQ M), `nbits_per_idx` (PQ bit width, default 8), `nprobe` (search-time, parsed by `Parse`).
- Real-time inserts go through `realtime::RTInvertIndex` so search and add are concurrent without rebuilds.
- IVFPQ has an optional cache-friendly `OPT_IVFPQ_RELAYOUT` layout (`impl/relayout/`), controlled by the CMake flag and conditionally included in `index/index.h`.

#### HNSW

- Header-only `hnswlib` lives under `impl/hnswlib/{hnswalg.h, hnswlib.h, space_ip.h, space_l2.h, visited_list_pool.h}`.
- `GammaIndexHNSWLIB` inherits from both `GammaFLATIndex` (for the rerank/raw-vector path) and `hnswlib::HierarchicalNSW<float>`.
- Build params: `M` (graph degree), `efConstruction`. Search param: `efSearch` (set per query via `HNSWLIBRetrievalParameters`).

#### SCANN (`USE_SCANN`)

- Bridges faiss IVF coarse quantizer with Google ScaNN reranking. C API in `impl/scann/scann_api.h` (`ScannTraining`, `ScannSearch`, …).
- Requires the proprietary ScaNN library; the build is off by default.

#### DISKANN_STATIC (`BUILD_WITH_DISKANN`)

- Static (read-mostly) on-disk graph index. CMake resolves DiskANN via `DISKANN_ROOT` / `DISKANN_INCLUDE_DIR` / `DISKANN_LIBRARY` (see `internal/engine/CMakeLists.txt`).
- Append/realtime is **not** supported. Set `Table.enable_realtime = false` and rebuild offline.

#### GPU (`BUILD_WITH_GPU`)

- `gamma_gpu_index_base.h` / `gamma_gpu_search_base.h` provide shared CUDA scaffolding.
- Currently only IVFFlat and IVFPQ have GPU variants. CAGRA / cuVS are not integrated in this engine.

### Distance / Metric Types

`enum DistanceComputeType` in `internal/engine/index/index_model.h`:

| Enum value | Wire string | Notes |
|---|---|---|
| `INNER_PRODUCT` | `"InnerProduct"` | Higher is more similar. |
| `L2` | `"L2"` | Squared L2; lower is more similar (default). |
| `Cosine` | `"Cosine"` | Implemented as IP on normalized vectors. |

Binary indexes use Hamming distance internally (faiss `METRIC_Hamming`).

### Vector Data Types

`enum VectorValueType` in `internal/engine/index/index_model.h`:

| Type | `data_size_` | Used by |
|---|---|---|
| `FLOAT` | 4 bytes | All real-valued indexes (FLAT, IVF\*, HNSW, SCANN, DISKANN\_STATIC, GPU\_\*). |
| `BINARY` | 1 byte (8 bits packed) | `BINARYIVF`. |
| `INT8` | 1 byte | Some IVF variants (path through `RawVector`); int8 quantized inputs. |

### Selecting an Index from Wire Configs

The string in `Table.index_type` (FlatBuffers, `idl/fbs/table.fbs:33`) — or per-vector `IndexInfo.type` if multi-index — is passed verbatim to `reflector().GetNewIndex(...)`. Misspellings produce `cannot get model=<name>` at create time, raised by `VectorManager::CreateVectorIndex`.

---

## Vector Storage Backends

`enum VectorStorageType` is selected automatically by `VectorManager::DetermineVectorStorageType` based on index type:

| Backend | When used | Source |
|---|---|---|
| `MemoryOnly` | Default for in-memory indexes (HNSW, FLAT). | `vector/memory_raw_vector.{h,cc}` |
| `MemoryBuffer` | Realtime "buffer" vector kept alongside batch index for incremental writes (FLAT companion to IVF families when `enable_realtime`). | `vector/memory_buffer_raw_vector.{h,cc}` |
| `RocksDB` | Persistent vector storage (large datasets, restart durability). | `vector/rocksdb_raw_vector.{h,cc}` |

All concrete classes inherit `RawVector` (which itself implements `VectorReader`). `RawVectorFactory::Create` (`vector/raw_vector_factory.h`) is the single creation point.

---

## Scalar (Filter) Indexes

Scalar indexes accelerate post-filter and pre-filter on non-vector fields during search. They live in `internal/engine/table/` and are managed by `ScalarIndexManager`, not by the vector reflector.

### Class Hierarchy

`ScalarIndex` (`table/scalar_index.h`) is the abstract base.

| Class | Source | Backing store | Mapping |
|---|---|---|---|
| `BitmapIndex` | `table/bitmap_index.{h,cc}` | In-memory `roaring::Roaring64Map` | `(field, value) → bitset of docids` |
| `InvertedIndex` | `table/inverted_index.{h,cc}` | RocksDB column family (`StorageManager`) | `(field, value, docid) → null` (one-to-one keys, range-scanable) |
| `CompositeIndex` | `table/composite_index.{h,cc}` | RocksDB column family | Composite key over multiple columns |

The header comment in `scalar_index.h` reserves `InvertedListIndex` (`(field, value) → [docids]`) as a future one-to-many variant; not yet implemented.

### Manager

`ScalarIndexManager` (`table/scalar_index_manager.{h,cc}`) owns the per-field `ScalarIndex` instances and is constructed by `Engine` (`search/engine.{h,cc}`). It coordinates:

- Index creation when a `FieldInfo` (`idl/fbs/table.fbs`) has `is_index = true` and an `index_type`.
- Per-doc updates from the realtime write path.
- Query-time lookup, returning a `ScalarIndexResult` (see `table/scalar_index_result.h`) that gets ANDed with the vector candidate set.

### Concurrency

`BitmapIndex` uses `std::shared_mutex` for concurrent reads + exclusive writes on the in-memory roaring map. When extending it, prefer `ScalarIndexResult` move-semantics over deep copies.

### Wire Schema

`FieldInfo` in `idl/fbs/table.fbs`:

```
table FieldInfo {
  name:string;
  data_type:DataType;
  is_index:bool;
  index_type:int = 0;   // 0 = none; values map to ScalarIndex subclass selection
}
```

`IndexInfo` is used for *vector* multi-index configs and also for richer scalar index declarations (`field_names:[string]` for composite indexes).

### Where Filters Are Applied

During vector search, `RetrievalContext::IsValid(int64_t id)` (see `index/index_model.h`) is the integration point: each vector index calls it inside its candidate loop. The implementation in the engine consults the `ScalarIndexResult` produced by `ScalarIndexManager` for the current request, plus the docids deletion bitmap (`util/bitmap_manager.h` / `bitmap::RocksdbBitmapManager`).
