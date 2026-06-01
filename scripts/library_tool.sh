#!/bin/bash -e
# This script builds and installs the required 3rd party C++ libraries.
#
# Usage:
#    	./scripts/library_tool.sh [capnp|libuv|openssl] [download|compile|install] [--prefix=PREFIX]

# Remember:
#	Update the usage string when you are add/remove dependency
#	Bump version should be done through variables, not hard coded strs.

CAPNP_VERSION="1.0.1"
UV_VERSION="1.51.0"
OPENSSL_VERSION="4.0.0"

THIRD_PARTY_DIRECTORY="./thirdparties"

THIRD_PARTY_DOWNLOADED="${THIRD_PARTY_DIRECTORY}/downloaded"
THIRD_PARTY_COMPILED="${THIRD_PARTY_DIRECTORY}/compiled"

PREFIX="/usr/local"

# Parse the optional --prefix= argument
for arg in "$@"; do
	if [[ "$arg" == --prefix=* ]]; then
		PREFIX="${arg#--prefix=}"
	fi
done

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    NUM_CORES=$(nproc)
elif [[ "$OSTYPE" == "darwin"* ]]; then
    NUM_CORES=$(sysctl -n hw.ncpu)
else
    NUM_CORES=1
fi

PREFIX=$(mkdir -p "${PREFIX}" && cd "${PREFIX}" && pwd)

show_help() {
    echo "Usage: ./library_tool.sh [capnp|libuv|openssl] [download|compile|install] [--prefix=DIR]"
    exit 1
}

# Usage: download_tar_gz <url> <folder_name>
download_tar_gz() {
    local url="$1"
    local folder_name="$2"

    curl --retry 100 --retry-max-time 3600 \
        -L "${url}" \
        -o "${THIRD_PARTY_DOWNLOADED}/${folder_name}.tar.gz"
    echo "Downloaded ${folder_name} into ${THIRD_PARTY_DOWNLOADED}/${folder_name}.tar.gz"
}

# Usage: extract_tar_gz <folder_name>
extract_tar_gz() {
    local folder_name="$1"

    rm -rf "${THIRD_PARTY_COMPILED}/${folder_name}"
    tar -xzf "${THIRD_PARTY_DOWNLOADED}/${folder_name}.tar.gz" -C "${THIRD_PARTY_COMPILED}"
}

if [ "$2" == "download" ]; then
    mkdir -p "${THIRD_PARTY_DOWNLOADED}"
elif [ "$2" == "compile" ]; then
    mkdir -p "${THIRD_PARTY_COMPILED}"
fi

if [ "$1" == "capnp" ]; then
    CAPNP_FOLDER_NAME="capnproto-c++-${CAPNP_VERSION}"
    CAPNP_URL="https://capnproto.org/${CAPNP_FOLDER_NAME}.tar.gz"

    if [ "$2" == "download" ]; then
        download_tar_gz "${CAPNP_URL}" "${CAPNP_FOLDER_NAME}"

    elif [ "$2" == "compile" ]; then
        extract_tar_gz "${CAPNP_FOLDER_NAME}"

        cd "${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"
        ./configure --prefix="${PREFIX}" CXXFLAGS="${CXXFLAGS} -I${PREFIX}/include" LDFLAGS="${LDFLAGS} -L${PREFIX}/lib -Wl,-rpath,${PREFIX}/lib"
        make -j "${NUM_CORES}"
        echo "Compiled capnp to ${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"

    elif [ "$2" == "install" ]; then
        cd "${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"
        make install
        echo "Installed capnp into ${PREFIX}"

    else
        show_help
    fi
elif [ "$1" == "libuv" ]; then
    UV_FOLDER_NAME="libuv-${UV_VERSION}"
    UV_URL="https://github.com/libuv/libuv/archive/refs/tags/v${UV_VERSION}.tar.gz"

    if [ "$2" == "download" ]; then
        download_tar_gz "${UV_URL}" "${UV_FOLDER_NAME}"

    elif [ "$2" == "compile" ]; then
        extract_tar_gz "${UV_FOLDER_NAME}"

        cd "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"
        cmake -B build -DCMAKE_INSTALL_PREFIX="${PREFIX}" -DBUILD_TESTING=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        cmake --build build --config Release
        echo "Compiled libuv to ${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"

    elif [ "$2" == "install" ]; then
        cd "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"
        cmake --install build
        echo "Installed libuv into ${PREFIX}"

    else
        show_help
    fi
elif [ "$1" == "openssl" ]; then
    OPENSSL_FOLDER_NAME="openssl-${OPENSSL_VERSION}"
    OPENSSL_URL="https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/${OPENSSL_FOLDER_NAME}.tar.gz"

    if [ "$2" == "download" ]; then
        download_tar_gz "${OPENSSL_URL}" "${OPENSSL_FOLDER_NAME}"

    elif [ "$2" == "compile" ]; then
        extract_tar_gz "${OPENSSL_FOLDER_NAME}"

        cd "${THIRD_PARTY_COMPILED}/${OPENSSL_FOLDER_NAME}"
        ./config --prefix="${PREFIX}" --libdir=lib no-tests
        make -j "${NUM_CORES}"
        echo "Compiled OpenSSL to ${THIRD_PARTY_COMPILED}/${OPENSSL_FOLDER_NAME}"

    elif [ "$2" == "install" ]; then
        cd "${THIRD_PARTY_COMPILED}/${OPENSSL_FOLDER_NAME}"
        make install_sw
        echo "Installed OpenSSL into ${PREFIX}"

    else
        show_help
    fi

else
    show_help
fi
