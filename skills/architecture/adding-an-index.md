# Adding a New Vector Index

Recipe for plugging a new vector index family into the Gamma engine. The path is short because `IndexModel` + `REGISTER_INDEX` do most of the work — but the realtime, dump/load, and filter integration points have edges worth knowing.

## 1. Pick a Name and a Source Location

- The registered name is what users put in `Table.index_type` (e.g., `"HNSW"`, `"IVFPQ"`). Convention: `ALL_CAPS`, optionally underscore-prefixed for variants (`GPU_IVFFLAT`, `DISKANN_STATIC`).
- Place the implementation under `internal/engine/index/impl/`. Single-file families use `gamma_index_<name>.{h,cc}`. Bigger families get a subdirectory (`impl/hnswlib/`, `impl/diskann/`, `impl/gpu/`, `impl/scann/`).

## 2. Implement `IndexModel`

Inherit `vearch::IndexModel` (defined in `internal/engine/index/index_model.h`) and implement at minimum:

| Virtual | Purpose |
|---|---|
| `Init(model_parameters, training_threshold)` | Parse params JSON; set `training_threshold_`. |
| `Parse(parameters)` | Build a per-search `RetrievalParameters` subclass (e.g., `efSearch`, `nprobe`). |
| `Indexing()` | Train (if needed) and build the index from `vector_->Gets(...)`. |
| `Add(n, vec)` | Append `n` vectors. Return `false` to signal back-pressure (`IndexModel::support_increment_` controls realtime eligibility). |
| `Update(ids, vecs)` / `Delete(ids)` | In-place mutation; safe to no-op for static indexes (return 0). |
| `Search(retrieval_context, n, x, k, distances, ids)` | Honour `retrieval_context->IsValid(id)` and `IsSimilarScoreValid(score)` while walking candidates. |
| `Dump(dir)` / `Load(dir, load_num)` | Persist / restore. Reuse helpers in `index/index_io.h` for faiss-compatible payloads. |
| `GetTotalMemBytes()` | Used by metrics / autoscale heuristics. |

If you can support concurrent inserts, set `support_increment_ = true` in the constructor; `VectorManager` will then pair you with the `MemoryBuffer` realtime FLAT companion (set up in `VectorManager::CreateVectorIndex` when `enable_realtime_` is on).

## 3. Register

In the `.cc` file, after the class definition, add at file scope:

```cpp
REGISTER_INDEX(MYINDEX, MyGammaIndexClass)
```

This expands (via `internal/engine/index/reflector.h`) to a static `Register_*` constructor that calls `reflector().RegisterFactory("MYINDEX", ...)` — no other wiring needed.

## 4. Wire the Build

- Append your `.cc` to the `gamma` library target in `internal/engine/CMakeLists.txt`. Most existing impls are picked up by glob; verify after `cmake --debug-output`.
- If the index needs an optional dependency, add a `BUILD_WITH_<X>` option and gate the `add_definitions(-DUSE_<X>)` + sources on it (mirror the SCANN / DISKANN / GPU patterns). See `docs/Development.md` "Engine Build Options" for the existing knobs.

## 5. Storage Backend

Decide which `VectorStorageType` your index needs. `VectorManager::DetermineVectorStorageType` picks a default from the index name; if the heuristic is wrong for your family, extend it there.

- `MemoryOnly` — in-memory only (HNSW, FLAT default).
- `MemoryBuffer` — realtime ring buffer companion.
- `RocksDB` — durable / large datasets.

## 6. Tests

Integration tests live under `test/` (Python pytest, requires a running local Vearch — `make test`). For a new index, add or adapt:

1. **Recall baseline** — compare to a faiss / exhaustive ground truth. Reference: `test/test_recall_baseline.py`.
2. **Per-family benchmark** — model after the existing per-index benchmarks (`test/test_vector_index_flat.py`, `test_vector_index_hnsw.py`, `test_vector_index_ivfpq.py`, `test_vector_index_ivfflat.py`, `test_vector_index_ivfrabitq.py`, `test_vector_index_diskann_static.py`). Add `test/test_vector_index_<your_name>.py` following the same pattern.
3. **Space lifecycle** — `test/test_module_space.py` exercises Space create/delete with various `index_type` values; extend it (or its parametrization) to cover yours.
4. **Search semantics** — `test/test_document_search.py` is the end-to-end search suite (filters, realtime updates).

Add a C++ unit test under `internal/engine/tests/` if the index has tricky internal state. Run with `cd build/gamma_build && ctest`.

> Note: `internal/engine/index/README.md` lists `test_vector_index_new_index.py` as a placeholder; that file does not exist. Use the per-family pattern above instead.

## 7. Document

Update [`docs/IndexLayer.md`](../../../docs/IndexLayer.md) (the registered-indexes table at the top, plus a Family Notes paragraph) so future contributors and Claude sessions can find your index.
