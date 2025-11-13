#
# This script tests the C++ components.
#
# Usage:
#      ./scripts/test.ps1

$OS="windows"
$ARCH="x64"
$BUILD_DIR="build_${OS}_${ARCH}"
$BUILD_PRESET="${OS}-${ARCH}"

# Run tests
ctest --preset $BUILD_PRESET -VV @args
