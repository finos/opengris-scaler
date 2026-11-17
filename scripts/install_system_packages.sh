#!/bin/bash -e
#
# This script installs the apt packages that building and testing Scaler needs on Linux.
# CI and the devcontainer both run it, so their toolchains match. Run it as root.
#
# Usage:
#      ./scripts/install_system_packages.sh

LLVM_VERSION="21"

export DEBIAN_FRONTEND=noninteractive

apt-get update -y
# lsb-release, wget, gnupg, and software-properties-common are what llvm.sh needs
apt-get install -y build-essential tzdata cmake clang curl pkg-config g++-14 \
    lsb-release wget gnupg software-properties-common

# Ubuntu 24.04 defaults to gcc 13
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100
update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100

# clang-format-21 is the version the lint gate checks against
llvm_script="$(mktemp)"
curl -fsSL https://apt.llvm.org/llvm.sh -o "${llvm_script}"
bash "${llvm_script}" "${LLVM_VERSION}"
rm "${llvm_script}"
apt-get install -y "clang-format-${LLVM_VERSION}"
