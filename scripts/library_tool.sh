#!/bin/bash -e
# This script builds and installs the required 3rd party C++ libraries.
#
# Usage:
#       ./scripts/library_tool.sh [boost|capnp|libuv] [download|compile|install] [--prefix=PREFIX] [--target=native|wasm]
#
# --target=wasm cross-compiles capnp/libuv against the Emscripten toolchain.
# It expects emcc/emcmake on PATH (source thirdparties/emsdk/emsdk_env.sh first)
# and that the host `capnp` tool is already installed (capnp code generation
# can't run inside wasm). Output defaults to ./thirdparties/wasm/install.

# Remember:
#       Update the usage string when you add/remove a dependency or target.
#       Bump versions through variables, not hard coded strings.

BOOST_VERSION="1.88.0"
CAPNP_VERSION="1.0.1"
UV_VERSION="1.51.0"

THIRD_PARTY_DIRECTORY="./thirdparties"
PATCHES_DIRECTORY="./scripts/patches"

THIRD_PARTY_DOWNLOADED="${THIRD_PARTY_DIRECTORY}/downloaded"

TARGET="native"
PREFIX=""

# Parse optional flags. Positional args ($1, $2) are the library name and step.
for arg in "$@"; do
    if [[ "$arg" == --prefix=* ]]; then
        PREFIX="${arg#--prefix=}"
    elif [[ "$arg" == --target=* ]]; then
        TARGET="${arg#--target=}"
    fi
done

if [[ "$TARGET" != "native" && "$TARGET" != "wasm" ]]; then
    echo "Unknown --target=${TARGET}; expected 'native' or 'wasm'."
    exit 1
fi

if [[ "$TARGET" == "wasm" ]]; then
    THIRD_PARTY_COMPILED="${THIRD_PARTY_DIRECTORY}/wasm/src"
    DEFAULT_PREFIX="${THIRD_PARTY_DIRECTORY}/wasm/install"
else
    THIRD_PARTY_COMPILED="${THIRD_PARTY_DIRECTORY}/compiled"
    DEFAULT_PREFIX="/usr/local"
fi

if [[ -z "${PREFIX}" ]]; then
    PREFIX="${DEFAULT_PREFIX}"
fi

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    NUM_CORES=$(nproc)
elif [[ "$OSTYPE" == "darwin"* ]]; then
    NUM_CORES=$(sysctl -n hw.ncpu)
else
    NUM_CORES=1
fi

PREFIX=$(mkdir -p "${PREFIX}" && cd "${PREFIX}" && pwd)

show_help() {
    echo "Usage: ./library_tool.sh [boost|capnp|libuv] [download|compile|install] [--prefix=DIR] [--target=native|wasm]"
    exit 1
}

require_emscripten() {
    if ! command -v emcmake >/dev/null 2>&1; then
        echo "emcmake not found; activate emsdk first (source thirdparties/emsdk/emsdk_env.sh)."
        exit 1
    fi
}

apply_patch_if_present() {
    # apply_patch_if_present <source-dir> <patch-file>
    local src_dir="$1"
    local patch_file="$2"
    if [[ ! -f "${patch_file}" ]]; then
        return 0
    fi
    # Skip if already applied (reverse dry-run succeeds means the patch is
    # already in the source tree).
    if patch -d "${src_dir}" -p1 -R --dry-run --silent < "${patch_file}" >/dev/null 2>&1; then
        echo "Patch already applied: ${patch_file}"
        return 0
    fi
    echo "Applying patch ${patch_file}"
    patch -d "${src_dir}" -p1 < "${patch_file}"
}

if [ "$1" == "boost" ]; then
    if [[ "$TARGET" == "wasm" ]]; then
        echo "boost is not used by the wasm client build; nothing to do."
        exit 0
    fi

    BOOST_FOLDER_NAME="boost_$(echo $BOOST_VERSION | tr '.' '_')"

    if [ "$2" == "download" ]; then
        mkdir -p ${THIRD_PARTY_DOWNLOADED}
        curl --retry 100 --retry-max-time 3600 \
          -L "https://archives.boost.io/release/${BOOST_VERSION}/source/${BOOST_FOLDER_NAME}.tar.gz" \
          -o "${THIRD_PARTY_DOWNLOADED}/${BOOST_FOLDER_NAME}.tar.gz"
        echo "Downloaded Boost to ${THIRD_PARTY_DOWNLOADED}/${BOOST_FOLDER_NAME}.tar.gz"

    elif [ "$2" == "compile" ]; then
        mkdir -p ${THIRD_PARTY_COMPILED}
        tar -xzvf "${THIRD_PARTY_DOWNLOADED}/${BOOST_FOLDER_NAME}.tar.gz" -C "${THIRD_PARTY_COMPILED}"
        echo "Compiled Boost to ${THIRD_PARTY_COMPILED}/${BOOST_FOLDER_NAME}"

    elif [ "$2" == "install" ]; then
        cp -r "${THIRD_PARTY_COMPILED}/${BOOST_FOLDER_NAME}/boost" "${PREFIX}/include/."
        echo "Installed Boost into ${PREFIX}/include/boost"

    else
        show_help
    fi

elif [ "$1" == "capnp" ]; then
    CAPNP_FOLDER_NAME="capnproto-c++-$(echo $CAPNP_VERSION)"

    if [ "$2" == "download" ]; then
        mkdir -p "${THIRD_PARTY_DOWNLOADED}"
        curl --retry 100 --retry-max-time 3600 \
            -L "https://capnproto.org/${CAPNP_FOLDER_NAME}.tar.gz" \
            -o "${THIRD_PARTY_DOWNLOADED}/${CAPNP_FOLDER_NAME}.tar.gz"
        echo "Downloaded capnp into ${THIRD_PARTY_DOWNLOADED}/${CAPNP_FOLDER_NAME}.tar.gz"

    elif [ "$2" == "compile" ]; then
        mkdir -p "${THIRD_PARTY_COMPILED}"
        rm -rf "${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"
        tar -xzf "${THIRD_PARTY_DOWNLOADED}/${CAPNP_FOLDER_NAME}.tar.gz" -C "${THIRD_PARTY_COMPILED}"

        if [[ "$TARGET" == "wasm" ]]; then
            require_emscripten
            apply_patch_if_present \
                "${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}" \
                "${PATCHES_DIRECTORY}/capnproto-${CAPNP_VERSION}-emscripten.patch"
            cd "${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"
            # The patch above skips building the capnp/capnpc-c++/capnpc-capnp
            # tools under EMSCRIPTEN; we use the host capnp tools (symlinked
            # under <prefix>/bin/<tool>.js by the install step) for codegen.
            emcmake cmake -B build-wasm \
                -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
                -DCMAKE_BUILD_TYPE=Release \
                -DBUILD_TESTING=OFF \
                -DEXTERNAL_CAPNP=ON \
                -DWITH_OPENSSL=OFF \
                -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
                -DCMAKE_C_FLAGS="-fPIC" \
                -DCMAKE_CXX_FLAGS="-fPIC"
            cmake --build build-wasm --config Release -j "${NUM_CORES}"
        else
            cd "${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"
            ./configure --prefix="${PREFIX}" CXXFLAGS="${CXXFLAGS} -I${PREFIX}/include" LDFLAGS="${LDFLAGS} -L${PREFIX}/lib -Wl,-rpath,${PREFIX}/lib"
            make -j "${NUM_CORES}"
        fi
        echo "Compiled capnp to ${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"

    elif [ "$2" == "install" ]; then
        cd "${THIRD_PARTY_COMPILED}/${CAPNP_FOLDER_NAME}"
        if [[ "$TARGET" == "wasm" ]]; then
            cmake --install build-wasm
            # CapnProtoTargets.cmake imports CapnProto::capnp_tool as
            # ${prefix}/bin/<tool>.js (Pyodide convention). The tools can't run
            # in wasm, so symlink the host-installed binaries instead.
            mkdir -p "${PREFIX}/bin"
            for tool in capnp capnpc-c++ capnpc-capnp; do
                host_tool=$(command -v "${tool}" || true)
                if [[ -z "${host_tool}" ]]; then
                    echo "Host capnp tool '${tool}' not found on PATH; install native capnp first."
                    exit 1
                fi
                ln -sf "${host_tool}" "${PREFIX}/bin/${tool}.js"
            done
        else
            make install
        fi
        echo "Installed capnp into ${PREFIX}"

    else
        show_help
    fi
elif [ "$1" == "libuv" ]; then
    UV_FOLDER_NAME="libuv-${UV_VERSION}"

    if [ "$2" == "download" ]; then
        mkdir -p "${THIRD_PARTY_DOWNLOADED}"
        curl --retry 100 --retry-max-time 3600 \
            -L "https://github.com/libuv/libuv/archive/refs/tags/v${UV_VERSION}.tar.gz" \
            -o "${THIRD_PARTY_DOWNLOADED}/${UV_FOLDER_NAME}.tar.gz"
        echo "Downloaded libuv into ${THIRD_PARTY_DOWNLOADED}/${UV_FOLDER_NAME}.tar.gz"

    elif [ "$2" == "compile" ]; then
        mkdir -p "${THIRD_PARTY_COMPILED}"
        rm -rf "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"
        tar -xzf "${THIRD_PARTY_DOWNLOADED}/${UV_FOLDER_NAME}.tar.gz" -C "${THIRD_PARTY_COMPILED}"

        if [[ "$TARGET" == "wasm" ]]; then
            require_emscripten
            apply_patch_if_present \
                "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}" \
                "${PATCHES_DIRECTORY}/libuv-${UV_VERSION}-emscripten.patch"
            cd "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"
            emcmake cmake -B build \
                -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
                -DCMAKE_BUILD_TYPE=Release \
                -DBUILD_TESTING=OFF \
                -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
                -DCMAKE_C_FLAGS="-fPIC" \
                -DCMAKE_CXX_FLAGS="-fPIC"
        else
            cd "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"
            cmake -B build -DCMAKE_INSTALL_PREFIX="${PREFIX}" -DBUILD_TESTING=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        fi
        cmake --build build --config Release -j "${NUM_CORES}"
        echo "Compiled libuv to ${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"

    elif [ "$2" == "install" ]; then
        cd "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"
        cmake --install build
        echo "Installed libuv into ${PREFIX}"

    else
        show_help
    fi
else
    show_help
fi
