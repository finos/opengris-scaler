# This script builds and installs in-place the C++ components.
#
# If `--clean` is specified, remove any existing cached build files before building.
#
# Usage:
#      ./scripts/build.ps1 [--clean]

$ErrorActionPreference = "Stop"

$OS = "windows"

$ARCH_NAME = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
$ARCH = switch ($ARCH_NAME) {
    "AMD64" { "x64" }
    "ARM64" { "arm64" }
}

$BUILD_DIR = "build_${OS}_${ARCH}"
$BUILD_PRESET = "${OS}-${ARCH}"

function exitOnError {
    param([scriptblock]$command)
    & $command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $command"
    }
}

if ($args[0] -eq "--clean") {
    # Clean up previous build artifacts
    if (Test-Path $BUILD_DIR) {
        Remove-Item -Recurse -Force $BUILD_DIR
    }
}

Write-Host "Build directory: $BUILD_DIR"
Write-Host "Build preset: $BUILD_PRESET"

# Configure
exitOnError { cmake --preset $BUILD_PRESET @args }

# Build
exitOnError { cmake --build --preset $BUILD_PRESET }

# Install
exitOnError { cmake --install $BUILD_DIR }
