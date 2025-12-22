#!/bin/bash -e
# This script builds and installs the required 3rd party C++ libraries.
#
# Usage:
#    	./scripts/library_tool.sh [boost|capnp|uv] [compile|install] [--prefix=PREFIX]

# Remember:
#	Update the usage string when you are add/remove dependency
#	Bump version should be done through variables, not hard coded strs.

BOOST_VERSION="1.88.0"
CAPNP_VERSION="1.0.1"
UV_VERSION="1.51.0"

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

PREFIX=$(readlink -f "${PREFIX}")
mkdir -p "${PREFIX}/include/"

show_help() {
    echo "Usage: ./library_tool.sh [boost|capnp|libuv] [download|compile|install] [--prefix=DIR]"
    exit 1
}

if [ "$1" == "boost" ]; then
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

        cd "${THIRD_PARTY_COMPILED}/${UV_FOLDER_NAME}"
        cmake -B build -DCMAKE_INSTALL_PREFIX="${PREFIX}" -DBUILD_TESTING=OFF
        cmake --build build --config Release
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
