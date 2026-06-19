#
# This script tests the C++ components.
#
# Usage:
#      ./scripts/test.ps1

$ErrorActionPreference="Stop"

$OS="windows"
$ARCH="x64"
$BUILD_DIR="build_${OS}_${ARCH}"
$BUILD_PRESET="${OS}-${ARCH}"

function exitOnError {
    param([scriptblock]$command)
    & $command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $command"
    }
}

# Run tests
exitOnError { ctest --preset $BUILD_PRESET -VV @args }
