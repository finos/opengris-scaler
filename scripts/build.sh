#!/bin/bash -e

cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=.
cd build && make
make install
ctest
