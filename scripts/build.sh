#!/bin/bash -e
#
# This script builds and installs in-place the C++ components.
#
# Usage:
#      ./scripts/build.sh
#
# You can specify the C and C++ compilers by setting the CMAKE_C_COMPILER and CMAKE_CXX_COMPILER environment variables.
# For example:
#      CMAKE_C_COMPILER=gcc CMAKE_CXX_COMPILER=g++ ./scripts/build.sh

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # e.g. linux or darwin
ARCH="$(uname -m)"                              # e.g. x86_64 or arm64

BUILD_DIR="build_${OS}_${ARCH}"
BUILD_PRESET="${OS}-${ARCH}"

echo "Build directory: $BUILD_DIR"
echo "Build preset: $BUILD_PRESET"

# Configure

CMAKE_ARGS=()
if [ -n "$CMAKE_C_COMPILER" ]; then
  CMAKE_ARGS+=(-DCMAKE_C_COMPILER="$CMAKE_C_COMPILER")
fi
if [ -n "$CMAKE_CXX_COMPILER" ]; then
  CMAKE_ARGS+=(-DCMAKE_CXX_COMPILER="$CMAKE_CXX_COMPILER")
fi

cmake --preset $BUILD_PRESET "${CMAKE_ARGS[@]}"

# Build
cmake --build --preset $BUILD_PRESET

# Install
cmake --install $BUILD_DIR

# Tests
ctest --preset $BUILD_PRESET
