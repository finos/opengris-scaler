#!/bin/bash -e
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

if [ "$1" == "boost" ]; then
	if [ "$2" == "compile" ]; then
		BOOST_FOLDER_NAME="boost_$(echo $BOOST_VERSION | tr '.' '_')"
		BOOST_PACKAGE_NAME=${BOOST_FOLDER_NAME}.tar.gz
		curl -O https://archives.boost.io/release/${BOOST_VERSION}/source/${BOOST_PACKAGE_NAME} --retry 100 --retry-max-time 3600
		tar -xzf ${BOOST_PACKAGE_NAME}
		mv ${BOOST_FOLDER_NAME} boost
	elif [ "$2" == "install" ]; then
        cp -r boost/boost ${PREFIX}/include/.
        echo "Installed Boost into ${PREFIX}/include/boost"
	else
		echo "Argument needs to be either compile or install"
		exit 1
	fi

elif [ "$1" == "capnp" ]; then
	if [ "$2" == "compile" ]; then
		CAPNP_FOLDER_NAME="capnproto-c++-$(echo $CAPNP_VERSION)"
		CAPNP_PACKAGE_NAME=${CAPNP_FOLDER_NAME}.tar.gz
		curl -O https://capnproto.org/${CAPNP_PACKAGE_NAME} --retry 100 --retry-max-time 3600
		tar -xzf ${CAPNP_PACKAGE_NAME}
		mv ${CAPNP_FOLDER_NAME} capnp
		cd capnp
		./configure --prefix=${PREFIX} \
			CXXFLAGS="${CXXFLAGS} -I${PREFIX}/include" \
			LDFLAGS="${LDFLAGS} -L${PREFIX}/lib -Wl,-rpath,${PREFIX}/lib"
		make -j$(nproc)
	elif [ "$2" == "install" ]; then
		cd capnp
		make install
		echo "Installed capnp into ${PREFIX}"
	else
		echo "Argument needs to be either compile or install"
		exit 1
	fi

else
    echo "Usage: ./download_install_dependencies.sh [boost|capnp] [compile|install] [--prefix=DIR]"
    exit 1
fi

