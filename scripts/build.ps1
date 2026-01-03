# This script builds and installs in-place the C++ components.
#
# Usage:
#      ./scripts/build.ps1

$ErrorActionPreference = "Stop"
$OS = "windows"
$ARCH = "x64"
$BUILD_DIR = "build_${OS}_${ARCH}"
$BUILD_PRESET = "${OS}-${ARCH}"

# Clean up previous build artifacts
if (Test-Path $BUILD_DIR) {
    Remove-Item -Recurse -Force $BUILD_DIR
}
Get-ChildItem "scaler/protocol/capnp" -Include *.c++, *.h -ErrorAction SilentlyContinue | Remove-Item -Force

Write-Host "Build directory: $BUILD_DIR"
Write-Host "Build preset: $BUILD_PRESET"

# Configure
cmake --preset $BUILD_PRESET @args

# Build
cmake --build --preset $BUILD_PRESET

# Install
# Simply copy from src to dest here. CMake Install on Windows generally doesn't work
$srcDebug  = @(".\build_windows_x64\src\cpp\scaler\utility\Debug\one_to_many_dict.pyd")
$srcRelease = @(".\build_windows_x64\src\cpp\scaler\utility\Release\one_to_many_dict.pyd")
$dest = @(".\src\scaler\utility\one_to_many_dict.pyd")

for ($i = 0; $i -lt $src.Length; $i++) {
    if (Test-Path $srcRelease[$i]) {
        Copy-Item -Path $srcRelease[$i] -Destination $dest[$i] -Force
    } else {
        Copy-Item -Path $srcDebug[$i] -Destination $dest[$i] -Force
    }
}
