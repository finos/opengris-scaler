#!/bin/bash -e
# This script builds and installs the required 3rd party C++ libraries.
#
# Usage:
#    	./scripts/library_tool.sh [boost|capnp] [compile|install] [--prefix=PREFIX]

# Remember:
#	Update the usage string when you are add/remove dependency
#	Bump version should be done through variables, not hard coded strs.

set -x

BOOST_VERSION="1.88.0"
CAPNP_VERSION="1.1.0"

DOWNLOAD_DIR="./downloaded"
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
    echo "Usage: ./library_tool.sh [boost|capnp] [download|compile|install] [--prefix=DIR]"
    exit 1
}

if [ "$1" == "boost" ]; then
    BOOST_FOLDER_NAME="boost_$(echo $BOOST_VERSION | tr '.' '_')"

    if [ "$2" == "download" ]; then
        mkdir -p ${DOWNLOAD_DIR}
        curl --retry 100 --retry-max-time 3600 \
          -L "https://archives.boost.io/release/${BOOST_VERSION}/source/${BOOST_FOLDER_NAME}.tar.gz" \
          -o "${DOWNLOAD_DIR}/${BOOST_FOLDER_NAME}.tar.gz"
        echo "Downloaded Boost to ${DOWNLOAD_DIR}/${BOOST_FOLDER_NAME}.tar.gz"

    elif [ "$2" == "compile" ]; then
        tar -xzvf "${DOWNLOAD_DIR}/${BOOST_FOLDER_NAME}.tar.gz" -C "./"

    elif [ "$2" == "install" ]; then
        cp -r "${BOOST_FOLDER_NAME}/boost" "${PREFIX}/include/."
        echo "Installed Boost into ${PREFIX}/include/boost"

    else
        show_help
    fi
elif [ "$1" == "capnp" ]; then
    CAPNP_FOLDER_NAME="capnproto-c++-$(echo $CAPNP_VERSION)"

    if [ "$2" == "download" ]; then
        mkdir -p ${DOWNLOAD_DIR}
        curl --retry 100 --retry-max-time 3600 \
            -L "https://capnproto.org/${CAPNP_FOLDER_NAME}.tar.gz" \
            -o "${DOWNLOAD_DIR}/${CAPNP_FOLDER_NAME}.tar.gz"
        echo "Downloaded capnp into ${DOWNLOAD_DIR}/${CAPNP_FOLDER_NAME}.tar.gz"

    elif [ "$2" == "compile" ]; then
        rm -rf "${CAPNP_FOLDER_NAME}"
        tar -xzf "${DOWNLOAD_DIR}/${CAPNP_FOLDER_NAME}.tar.gz" -C "./"

        cd "${CAPNP_FOLDER_NAME}"
        ./configure --prefix="${PREFIX}" CXXFLAGS="${CXXFLAGS} -I${PREFIX}/include" LDFLAGS="${LDFLAGS} -L${PREFIX}/lib -Wl,-rpath,${PREFIX}/lib"
        make -j "${NUM_CORES}"

    elif [ "$2" == "install" ]; then
        cd "${CAPNP_FOLDER_NAME}"
        make install
        echo "Installed capnp into ${PREFIX}"

    else
        show_help
    fi
else
    show_help
fi
