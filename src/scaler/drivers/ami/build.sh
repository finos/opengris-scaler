#!/bin/bash
set -e
set -x

# This script builds the AMI for opengris-scaler using Packer
# It reads the version from the version.txt file and passes it as a variable

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="$SCRIPT_DIR/../../version.txt"

if [ ! -f "$VERSION_FILE" ]; then
    echo "Error: Version file not found at $VERSION_FILE"
    exit 1
fi

VERSION=$(cat "$VERSION_FILE" | tr -d '[:space:]')

echo "Building AMI for version: $VERSION"

cd "$SCRIPT_DIR"
packer build -var "version=$VERSION" opengris-scaler.pkr.hcl
