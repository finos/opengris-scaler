#!/bin/bash -e
# Remember:
#	Update the usage string when you are add/remove dependency
#	Bump version should be done through variables, not hard coded strs.

BOOST_VERSION="1.88.0"
CAPNP_VERSION="1.1.0"

if [ "$1" == "boost" ]; then
	BOOST_FOLDER_NAME="boost_$(echo $BOOST_VERSION | tr '.' '_')"
	BOOST_PACKAGE_NAME=${BOOST_FOLDER_NAME}.tar.gz
	curl -O https://archives.boost.io/release/${BOOST_VERSION}/source/${BOOST_PACKAGE_NAME} --retry 100 --retry-max-time 3600
	tar -xzf ${BOOST_PACKAGE_NAME}
	sudo mkdir -p /usr/include/boost
	sudo mv ${BOOST_FOLDER_NAME}/boost /usr/include/.
	echo "Installed Boost into /usr/include/boost"
	sudo rm -rf ${BOOST_FOLDER_NAME} ${BOOST_PACKAGE_NAME}

elif [ "$1" == "capnp" ]; then
	CAPNP_FOLDER_NAME="capnproto-c++-$(echo $CAPNP_VERSION)"
	CAPNP_PACKAGE_NAME=${CAPNP_FOLDER_NAME}.tar.gz
	curl -O https://capnproto.org/${CAPNP_PACKAGE_NAME} --retry 100 --retry-max-time 3600
	tar -xzf ${CAPNP_PACKAGE_NAME}
	cd ${CAPNP_FOLDER_NAME}
	./configure --prefix=/usr/
	make -j6 check
	sudo make install
	echo "Installed capnp into /usr"
	sudo rm -rf ${CAPNP_FOLDER_NAME} ${CAPNP_PACKAGE_NAME}

else 
    echo "Usage: ./download_install_dependencies.sh [boost|capnp]"
    exit 1
fi

