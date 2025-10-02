#!/bin/bash -e
#
# This script tests the C++ components.
#
# Usage:
#      ./scripts/test.sh

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # e.g. linux or darwin
ARCH="$(uname -m)"                              # e.g. x86_64 or arm64

BUILD_DIR="build_${OS}_${ARCH}"
BUILD_PRESET="${OS}-${ARCH}"

# Run tests
ctest --preset $BUILD_PRESET -VV
