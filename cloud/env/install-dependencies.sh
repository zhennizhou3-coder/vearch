#!/usr/bin/env bash

set -euo pipefail

ARCH=$(arch)
DOWNLOAD_BASE_URL=${DOWNLOAD_BASE_URL:-http://storage.jd.local/storages/dependency}

if [ ! -d "/env/app" ]; then
    mkdir -p /env/app
fi
cd /env/app/

verify_sha256() {
    local file=$1
    local expected=$2

    if [ -z "${expected}" ]; then
        echo "Missing SHA256 for ${file}" >&2
        return 1
    fi

    local actual
    actual=$(sha256sum "${file}" | awk '{print $1}')
    if [ "${actual}" = "${expected}" ]; then
        return 0
    fi

    echo "Checksum mismatch for ${file}: expected ${expected}, got ${actual}" >&2
    return 1
}

download() {
    local output=$1
    local sha256=$2
    shift 2

    if [ -s "${output}" ] && verify_sha256 "${output}" "${sha256}"; then
        return
    fi

    rm -f "${output}"
    for url in "$@"; do
        if [ -z "${url}" ]; then
            continue
        fi
        echo "Downloading ${output} from ${url}"
        if wget -q "${url}" -O "${output}" && [ -s "${output}" ] && verify_sha256 "${output}" "${sha256}"; then
            return
        fi
        rm -f "${output}"
    done

    echo "Failed to download ${output}" >&2
    return 1
}

PROTOBUF_ARCHIVE=protobuf-cpp-3.21.0.tar.gz
PROTOBUF_URL=${DOWNLOAD_BASE_URL}/${PROTOBUF_ARCHIVE}
PROTOBUF_SHA256=28e74a8a241a91c0a6aec997d0d6efda9bc0baa0ed7acfaf392555b88bca8b7c
download "${PROTOBUF_ARCHIVE}" "${PROTOBUF_SHA256}" \
    "${PROTOBUF_URL}" \
    "https://github.com/protocolbuffers/protobuf/releases/download/v21.0/${PROTOBUF_ARCHIVE}"
tar xf "${PROTOBUF_ARCHIVE}"
cd protobuf-3.21.0
./configure --prefix=/usr/local && make -j4 && make install
cd /env/app
rm -rf "${PROTOBUF_ARCHIVE}" protobuf-3.21.0

cd /env/app
ROCKSDB_ARCHIVE=rocksdb-9.2.1.tar.gz
ROCKSDB_URL=${DOWNLOAD_BASE_URL}/${ROCKSDB_ARCHIVE}
ROCKSDB_SHA256=bb20fd9a07624e0dc1849a8e65833e5421960184f9c469d508b58ed8f40a780f
download "${ROCKSDB_ARCHIVE}" "${ROCKSDB_SHA256}" \
    "${ROCKSDB_URL}" \
    "https://github.com/facebook/rocksdb/archive/refs/tags/v9.2.1.tar.gz"
tar xf "${ROCKSDB_ARCHIVE}"
cd /env/app/rocksdb-9.2.1
sed -i '/CFLAGS += -g/d' Makefile
sed -i '/CXXFLAGS += -g/d' Makefile
CFLAGS="-O3 -fPIC" CXXFLAGS="-O3 -fPIC" ROCKSDB_DISABLE_BZIP=1 make static_lib -j4 && make install PREFIX=/usr/local
cd /env/app
rm -rf "${ROCKSDB_ARCHIVE}" rocksdb-9.2.1

cd /env/app

if [[ ! -f "/usr/local/lib64/libroaring.a" ]]; then
    CROARING_ARCHIVE=CRoaring-4.2.1.tar.gz
    CROARING_URL=${DOWNLOAD_BASE_URL}/${CROARING_ARCHIVE}
    CROARING_SHA256=3514728e9eb8c90dbc00a9e337302eb458c65be2f9501a3e882d051599c4a74c
    download "${CROARING_ARCHIVE}" "${CROARING_SHA256}" \
        "${CROARING_URL}" \
        "https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.2.1.tar.gz"
    tar xf "${CROARING_ARCHIVE}"
    pushd CRoaring-4.2.1
    CPM_CMAKE_URL=${DOWNLOAD_BASE_URL}/CPM.cmake
    CPM_CMAKE_SHA256=11c3fa5f1ba14f15d31c2fb63dbc8628ee133d81c8d764caad9a8db9e0bacb07
    rm -f cmake/CPM.cmake
    download "cmake/CPM.cmake" "${CPM_CMAKE_SHA256}" \
        "${CPM_CMAKE_URL}" \
        "https://github.com/cpm-cmake/CPM.cmake/releases/download/v0.38.6/CPM.cmake"
    mkdir build && pushd build
    cmake ../ -B ./ \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DENABLE_ROARING_TESTS=OFF
    make -j4 && make install
    popd && popd
    rm -rf "${CROARING_ARCHIVE}" CRoaring-4.2.1
fi

if [[ ! -f "/usr/local/lib64/libfaiss.a" ]]; then
    FAISS_ARCHIVE=faiss-1.14.1.tar.gz
    FAISS_URL=${DOWNLOAD_BASE_URL}/${FAISS_ARCHIVE}
    FAISS_SHA256=0216d38d8c5c460433815b72d3cde6725eaa5d5770576277a2abc01ffc414d20
    download "${FAISS_ARCHIVE}" "${FAISS_SHA256}" \
        "${FAISS_URL}" \
        "https://github.com/facebookresearch/faiss/archive/refs/tags/v1.14.1.tar.gz"
    tar xf "${FAISS_ARCHIVE}"
    pushd faiss-1.14.1
    if [ -z "${MKLROOT:-}" ]; then
        OS_NAME=$(uname)
        ARCH=$(arch)
        if [ "${OS_NAME}" == "Darwin" ]; then
            cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DFAISS_ENABLE_GPU=OFF -DOpenMP_CXX_FLAGS="-Xpreprocessor -fopenmp -I/usr/local/opt/libomp/include" -DOpenMP_CXX_LIB_NAMES="libomp" -DOpenMP_libomp_LIBRARY="/usr/local/opt/libomp/lib" -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -B build .
        elif [ "${ARCH}" == "aarch64" ] || [ "${ARCH}" == "AARCH64" ]; then
            cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -B build .
        else
            cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx512 -B build .
        fi
    else
        cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx512 -DBLA_VENDOR=Intel10_64_dyn -DMKL_LIBRARIES=$MKLROOT/lib/intel64 -B build .
    fi

    make -C build faiss -j4 && make -C build install
    popd
    rm -rf "${FAISS_ARCHIVE}" faiss-1.14.1
fi


if [[ "${ARCH}" != "aarch64" && "${ARCH}" != "AARCH64" ]]; then
    if [[ ! -f "/usr/local/lib64/libdiskann.so" && ! -f "/usr/local/lib/libdiskann.so" ]]; then
        DISKANN_BOOST_VERSION="1.78.0"
        DISKANN_BOOST_ROOT="/env/app/.deps/boost-${DISKANN_BOOST_VERSION}"
        if [[ ! -f "${DISKANN_BOOST_ROOT}/include/boost/dynamic_bitset.hpp" ]] || [[ ! -f "${DISKANN_BOOST_ROOT}/lib/libboost_program_options.so" && ! -f "${DISKANN_BOOST_ROOT}/lib/libboost_program_options.a" ]]; then
            mkdir -p /env/app/.deps
            pushd /env/app/.deps
            BOOST_TAG=${DISKANN_BOOST_VERSION//./_}
            BOOST_ARCHIVE="boost_${BOOST_TAG}.tar.gz"
            BOOST_SOURCE_DIR="boost_${BOOST_TAG}"
            BOOST_DOWNLOAD_URL=${DOWNLOAD_BASE_URL}/${BOOST_ARCHIVE}
            BOOST_SHA256=94ced8b72956591c4775ae2207a9763d3600b30d9d7446562c552f0a14a63be7
            download "${BOOST_ARCHIVE}" "${BOOST_SHA256}" \
                "${BOOST_DOWNLOAD_URL}" \
                "https://archives.boost.io/release/${DISKANN_BOOST_VERSION}/source/${BOOST_ARCHIVE}"
            if [[ ! -d "${BOOST_SOURCE_DIR}" ]]; then
                tar xf "${BOOST_ARCHIVE}"
            fi
            pushd "${BOOST_SOURCE_DIR}"
            ./bootstrap.sh --prefix="${DISKANN_BOOST_ROOT}"
            ./b2 install -j4 --with-program_options link=shared,static cxxflags=-fPIC
            popd
            rm -rf "${BOOST_ARCHIVE}" "${BOOST_SOURCE_DIR}"
            popd
        fi

        oneapi_root="/opt/intel/oneapi"
        MKL_ROOT="${oneapi_root}/mkl/latest"
        OMP_LIB_PATH="${oneapi_root}/compiler/latest/lib"
        INTEL_LIB_PATHS="${MKL_ROOT}/lib/intel64:${OMP_LIB_PATH}"
        export MKLROOT="${MKL_ROOT}"
        export LIBRARY_PATH=${INTEL_LIB_PATHS}:${LIBRARY_PATH:-}
        export LD_LIBRARY_PATH=${INTEL_LIB_PATHS}:${LD_LIBRARY_PATH:-}

        cd /env/app
        DISKANN_COMMIT=78256bbab4685e1774e78d331e081a153be26823
        DISKANN_ARCHIVE=diskann-${DISKANN_COMMIT}.tar.gz
        DISKANN_ARCHIVE_URL=${DOWNLOAD_BASE_URL}/${DISKANN_ARCHIVE}
        DISKANN_SHA256=0c8946d3f8c6c73b48c06d14f142ebcd52cb3718a306f9d65adac92167263d51
        download "${DISKANN_ARCHIVE}" "${DISKANN_SHA256}" \
            "${DISKANN_ARCHIVE_URL}" \
            "https://github.com/microsoft/DiskANN/archive/${DISKANN_COMMIT}.tar.gz"
        DISKANN_SRC_DIR=$(tar tf "${DISKANN_ARCHIVE}" | awk -F/ 'NR==1{print $1}')
        tar xf "${DISKANN_ARCHIVE}"
        pushd "${DISKANN_SRC_DIR}"
        cmake -B build \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            -DCMAKE_INSTALL_LIBDIR=lib64 \
            -DDISKANN_BUILD_APPS=OFF \
            -DDISKANN_BUILD_TESTS=OFF \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
            -DBOOST_ROOT="${DISKANN_BOOST_ROOT}" \
            -DOMP_PATH="${OMP_LIB_PATH}" \
            -DBoost_NO_SYSTEM_PATHS=ON \
            .
        make -C build diskann -j4 && make -C build install
        cp -r "${PWD}/include" /usr/local/include/diskann
        popd
        rm -rf "${DISKANN_ARCHIVE}" "${DISKANN_SRC_DIR}"
    fi
else
    echo "DiskANN is not supported on arm64, skipping DiskANN and Boost build."
fi

cd /env/app
if [[ ! -f "/usr/local/lib64/libprometheus-cpp-core.a" && ! -f "/usr/local/lib/libprometheus-cpp-core.a" ]]; then
    if [ ! -f "prometheus-cpp-1.2.4.tar.gz" ]; then
        wget -q https://github.com/jupp0r/prometheus-cpp/releases/download/v1.2.4/prometheus-cpp-with-submodules.tar.gz -O prometheus-cpp-1.2.4.tar.gz
    fi
    mkdir -p prometheus-cpp-1.2.4
    tar xf prometheus-cpp-1.2.4.tar.gz -C prometheus-cpp-1.2.4 --strip-components=1
    pushd prometheus-cpp-1.2.4
    cmake -B build -S . \
        -DENABLE_PULL=OFF -DENABLE_PUSH=OFF -DENABLE_COMPRESSION=OFF \
        -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release
    cmake --build build --parallel 4
    cmake --install build
    popd
fi

GO_VERSION=1.22.5
cd /env/app
if [ "${ARCH}" = "x86_64" ]; then
    GO_ARCHIVE=go${GO_VERSION}.linux-amd64.tar.gz
    GO_URL=${DOWNLOAD_BASE_URL}/${GO_ARCHIVE}
    GO_SHA256=904b924d435eaea086515bc63235b192ea441bd8c9b198c507e85009e6e4c7f0
    download "${GO_ARCHIVE}" "${GO_SHA256}" \
        "${GO_URL}" \
        "https://go.dev/dl/${GO_ARCHIVE}"
    tar xf "${GO_ARCHIVE}"
    rm -rf "${GO_ARCHIVE}"
elif [ "${ARCH}" = "aarch64" ]; then
    GO_ARCHIVE=go${GO_VERSION}.linux-arm64.tar.gz
    GO_URL=${DOWNLOAD_BASE_URL}/${GO_ARCHIVE}
    GO_SHA256=8d21325bfcf431be3660527c1a39d3d9ad71535fabdf5041c826e44e31642b5a
    download "${GO_ARCHIVE}" "${GO_SHA256}" \
        "${GO_URL}" \
        "https://go.dev/dl/${GO_ARCHIVE}"
    tar xf "${GO_ARCHIVE}"
    rm -rf "${GO_ARCHIVE}"
fi
cp -r /env/app/go /usr/local/bin/go
rm -rf /env/app/go
