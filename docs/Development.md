# Vearch Development

This guide collects build, test, debug, and CI commands for developers working on Vearch.

## Build

### Prerequisites

- Go 1.22+
- CMake 3.17+
- GCC/G++ with C++17 support
- DiskANN library (`libdiskann.so`)
- Intel oneAPI MKL for DiskANN optimized builds
- Boost 1.78, auto-downloaded by the build script

### Commands

```bash
# Full build: C++ Gamma engine + Go binary
make all
make all j=8

# Build Go binary only and skip engine rebuild
cd build && ./build.sh -g OFF

# Debug build
cd build && ./build.sh -d

# Build CLI tools only
make tools

# Show all build options
cd build && ./build.sh -h
```

### build.sh Options

| Option | Purpose |
|---|---|
| `-n <num>` | Compile thread count |
| `-g ON\|OFF` | Build Gamma engine; default is `ON` |
| `-t` | Build engine tests |
| `-d` | Debug build |
| `-o generic\|avx2\|avx512` | SIMD optimization level; `build.sh` defaults to `avx512`, which overrides the CMake option default of `avx2`. ARM64 forces `generic` regardless of either default. |

### Build Output

- `build/bin/vearch` — main binary
- `build/lib/` — C++ shared libraries

### Engine Build Options (CMake)

The CMake options below live in `internal/engine/CMakeLists.txt`. `build/build.sh` exposes the most common ones; advanced tuning requires editing the CMake invocation directly.

| Option | Default | Effect |
|---|---|---|
| `BUILD_WITH_DISKANN` | `on` | Enables `DISKANN_STATIC`. CMake resolves DiskANN via `DISKANN_ROOT` / `DISKANN_INCLUDE_DIR` / `DISKANN_LIBRARY` or system paths (`/usr/local/lib64`, `/usr/lib64`, …). |
| `BUILD_WITH_GPU` | `off` | Builds `GPU_IVFFLAT` / `GPU_IVFPQ` (faiss-GPU). Requires CUDA toolkit. |
| `BUILD_WITH_SCANN` | `off` | Defines `USE_SCANN`; enables ScaNN-backed `IndexScann` wrapper (`impl/scann/`). |
| `BUILD_FAISSLIKE_INDEX` | `off` | Builds the standalone `vearch::Index` faiss-like API (`index/index.{h,cc}`) for tools and benchmarks. |
| `BUILD_RELAYOUT` | `off` | Defines `OPT_IVFPQ_RELAYOUT`; switches IVFPQ to the cache-friendly relayout variant in `impl/relayout/`. |
| `BUILD_GAMMA_OPT_LEVEL` | `avx2` | SIMD tier (`generic` / `avx2` / `avx512`). Forced to `generic` on ARM64. `build.sh -o` overrides; `build.sh` itself defaults to `avx512`. |
| `BUILD_TEST` | `off` | Builds C++ unit tests under `internal/engine/tests/`. |
| `BUILD_TOOLS` | `off` | Builds CLI tools under `internal/engine/tools/`. |
| `BUILD_PYTHON_SDK` | `off` | Builds the Python binding in `internal/engine/sdk/`. |
| `ENABLE_COVERAGE` | `off` | gcov instrumentation. |

The Go control plane invokes the engine build through `build/build.sh` and `make all`; the cgo build tag `vector` is required (`-tags=vector`).

### Vendored Libraries (`internal/engine/third_party/`)

#### faiss

- Path: `third_party/faiss/`.
- Used by: `FLAT`, `IVFFLAT`, `IVFPQ`, `IVFPQFastScan`, `IVFRABITQ`, `BINARYIVF`, the GPU variants, the SCANN coarse-quantizer path, and the faiss-like wrapper in `index/index.{h,cc}`.
- Vearch consumes faiss headers extensively: `faiss/IndexIVFFlat.h`, `faiss/IndexIVFPQ.h`, `faiss/impl/HNSW.h`, `faiss/impl/RaBitQuantizer.h`, `faiss/index_io.h`, etc.
- `index_io.h/cc` provides faiss-binary-compatible (de)serializers extended for Vearch's per-index tail (e.g., realtime inverted lists in `WriteInvertedLists` / `ReadInvertedLists`).

#### hnswlib

- Path: `internal/engine/index/impl/hnswlib/` (header-only: `hnswalg.h`, `hnswlib.h`, `space_l2.h`, `space_ip.h`, `visited_list_pool.h`).
- Used by: `HNSW` (`GammaIndexHNSWLIB` inherits `hnswlib::HierarchicalNSW<float>`).

#### RocksDB

- Used by: persistent vector storage (`vector/rocksdb_raw_vector.{h,cc}`), `StorageManager` (`storage/storage_manager.{h,cc}`), `InvertedIndex` / `CompositeIndex` scalar indexes, `bitmap::RocksdbBitmapManager` for deletion masks.
- One column family per logical store; `cf_id` plumbed through `RawVectorFactory::Create`, `Add(cf_id, id, value, len)` etc.

#### DiskANN

- Treated as a preinstalled system library (no longer vendored as a boost-dependent fork). The CMake searches for `pq_flash_index.h` and `libdiskann.so`. Old hooks `DISKANN_BOOST_ROOT` / `DISKANN_BOOST_NO_SYSTEM_PATHS` remain as deprecated aliases.
- Used only by `DISKANN_STATIC` (`impl/diskann/`).

#### ScaNN (optional)

- Not vendored. Linked via the C API in `impl/scann/scann_api.h` when `BUILD_WITH_SCANN=on`.

### System Dependencies

| Dependency | Required for | Notes |
|---|---|---|
| OpenBLAS | Always | faiss numerical primitives. macOS uses Homebrew (`/usr/local/opt/openblas`). |
| OpenMP | Always | `-fopenmp` on every build (`CMAKE_CXX_FLAGS_RELEASE`). On macOS uses LLVM's libomp. |
| RocksDB | Always | Persistent storage / scalar indexes. |
| TBB | Always | `tbb::concurrent_bounded_queue` used for `IndexModel::updated_vids_`. |
| roaring | Always | `BitmapIndex` (`<roaring/roaring64map.hh>`). |
| CUDA + faiss-GPU | `BUILD_WITH_GPU=on` | GPU IVF variants only. |
| libaio | DiskANN runtime | required by DiskANN's SSD I/O backend. |

### Generated Code

- Engine IDL lives in `internal/engine/idl/`. `internal/engine/idl/build.sh` regenerates FlatBuffers (`fbs/` → `fbs-gen/`) and protobuf (`pb-gen/`). Hand-edit `.fbs` / `.proto`, then run.
- `internal/engine/c_api/api_data/` is the **hand-written** C++ data layer used by the cgo bridge (`cpp_api.cc/h`, `doc.cc/h`, `request.cc/h`, `response.cc/h`, …). Do not confuse it with generated files; it is editable.
- The Go protobuf generated under `internal/proto/vearchpb/*.pb.go` must not be hand-edited. That invariant lives in `CLAUDE.md` and applies to the Go control plane only.

## Run

```bash
# Standalone: all roles in one process for local development
./build/bin/vearch -conf=config/config.toml all

# Individual roles
./build/bin/vearch -conf=config/config.toml master
./build/bin/vearch -conf=config/config.toml ps
./build/bin/vearch -conf=config/config.toml router

# Multi-master: specify identity
./build/bin/vearch -conf=config/config_cluster.toml -master=m1 master
```

## Docker

```bash
# Standalone
cd cloud && docker compose --profile standalone up

# Cluster: 3 masters, 2 routers, 3 PS
cd cloud && docker compose --profile cluster up
```

## Tests

Integration tests require a running Vearch instance in standalone mode on localhost.

```bash
# Full test suite
make test

# Integration test categories
cd test
pytest test_vearch.py -x --log-cli-level=INFO
pytest test_document_* -k "not test_vearch_document_upsert_benchmark" -x --log-cli-level=INFO
pytest test_module_* -x --log-cli-level=INFO

# Single integration test file
cd test && pytest test_document_search.py -x --log-cli-level=INFO

# Go unit tests
go test ./internal/entity/... -v

# Go SDK tests
cd sdk/go/test && go test -v

# Engine unit tests
cd build && ./build.sh -t
cd build/gamma_build && ctest
```

## Debug Endpoints

| Port | Purpose |
|---|---|
| `:6060` | PS pprof |
| `:6061` | Router pprof |
| `:6062` | Master pprof |
| `:8818` | PS / Master Prometheus metrics |

Access pprof Web UI through `localhost:606x/debug/pprof/`.

## Debugging Slow PS Writes

1. Check Prometheus metrics in `internal/pkg/metrics/`.
2. Capture PS pprof at `localhost:6060/debug/pprof/profile`.
3. Inspect raft state in `internal/ps/storage/raftstore/store_raft_job.go`.
4. Inspect engine writes in `internal/ps/engine/gammacb/writer.go`.

## CI

GitHub Actions runs on push and pull requests to `master`.

- `CI.yml` — main build, pytest suite, and SDK tests for amd64 and arm64
- `CI_cluster.yml`, `CI_document.yml`, `CI_index.yml` — focused test suites
- `docker-image.yml` — Docker image builds
