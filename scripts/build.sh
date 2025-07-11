#!/bin/bash -e
#
# This script builds and installs in-place the C++ components.
#
# Usage:
#      ./scripts/build.sh

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # e.g. linux or darwin
ARCH="$(uname -m)"                              # e.g. x86_64 or arm64

BUILD_DIR="build_${OS}_${ARCH}"

echo "Build directory: $BUILD_DIR"

# YMQ only supported on Linux for now
TARGETS=("object_storage_server" "cc_object_storage_server")
TEST_TARGETS=("test_update_record" "test_scaler_object")
if [[ "$OS" == "linux" ]]; then
  TARGETS+=("ymq")
fi

# Configure
cmake -S . -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_INSTALL_PREFIX=.

# Build
cmake --build "$BUILD_DIR" --target "${TARGETS[@]}" "${TEST_TARGETS[@]}"

# Install
for TARGET in "${TARGETS[@]}"; do
  cmake --install "$BUILD_DIR" --component "$TARGET"
done

# Tests
ctest --test-dir "$BUILD_DIR" --output-on-failure
