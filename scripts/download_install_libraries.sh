#!/bin/bash -e
# This script builds and installs the required 3rd party C++ libraries.
#
# Usage:
#    	./scripts/download_install_libraries.sh [boost|capnp] [compile|install] [--prefix=PREFIX]

# Remember:
#	Update the usage string when you are add/remove dependency
#	Bump version should be done through variables, not hard coded strs.

BOOST_VERSION="1.88.0"
CAPNP_VERSION="1.1.0"

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

PREFIX=`readlink -f $PREFIX`
mkdir -p ${PREFIX}/include/

show_help() {
    echo "Usage: ./download_install_libraries.sh [boost|capnp] [compile|install] [--prefix=DIR]"
    exit 1
}

if [ "$1" == "boost" ]; then
    BOOST_FOLDER_NAME="boost_$(echo $BOOST_VERSION | tr '.' '_')"
    if [ "$2" == "compile" ]; then
        BOOST_PACKAGE_NAME=${BOOST_FOLDER_NAME}.tar.gz
        curl -O https://archives.boost.io/release/${BOOST_VERSION}/source/${BOOST_PACKAGE_NAME} --retry 100 --retry-max-time 3600
        tar -xzf ${BOOST_PACKAGE_NAME}

    elif [ "$2" == "install" ]; then
        cp -r ${BOOST_FOLDER_NAME}/boost ${PREFIX}/include/.
        echo "Installed Boost into ${PREFIX}/include/boost"

    else
        show_help
    fi
elif [ "$1" == "capnp" ]; then
    CAPNP_FOLDER_NAME="capnproto-c++-$(echo $CAPNP_VERSION)"
    if [ "$2" == "compile" ]; then
        CAPNP_PACKAGE_NAME=${CAPNP_FOLDER_NAME}.tar.gz
        curl -O https://capnproto.org/${CAPNP_PACKAGE_NAME} --retry 100 --retry-max-time 3600
        tar -xzf ${CAPNP_PACKAGE_NAME}

        cd ${CAPNP_FOLDER_NAME}
        ./configure --prefix=${PREFIX} CXXFLAGS="${CXXFLAGS} -I${PREFIX}/include" LDFLAGS="${LDFLAGS} -L${PREFIX}/lib -Wl,-rpath,${PREFIX}/lib"
        make -j${NUM_CORES}

    elif [ "$2" == "install" ]; then
        cd ${CAPNP_FOLDER_NAME}
        make install
        echo "Installed capnp into ${PREFIX}"

    else
        show_help
    fi
else
    show_help
fi
