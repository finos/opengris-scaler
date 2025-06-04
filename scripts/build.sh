#!/bin/bash -e

cmake -S . -B build -DCMAKE_CXX_COMPILER=$(which clang++) -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=.
cd build && make
make install
ctest
