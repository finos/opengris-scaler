#!/bin/bash
set -e

uv python install "${PYTHON_VERSION}"
uv venv --python "${PYTHON_VERSION}" /opt/opengris-scaler

printf '%s\n' "${PYTHON_REQUIREMENTS}" > /tmp/requirements.txt

# If any requirement is a VCS source URL, install C++ build dependencies first.
# This mirrors what the EC2 user data script does for source builds of scaler.
if grep -qE 'git\+|@ git\+' /tmp/requirements.txt; then
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates git cmake gcc g++ pkg-config \
        libcapnp-dev capnproto \
        libuv1-dev libssl-dev
    rm -rf /var/lib/apt/lists/*
fi

uv pip install --no-cache -q --python /opt/opengris-scaler -r /tmp/requirements.txt

ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/

if [ -z "${COMMAND}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

echo "Executing: ${COMMAND}"
exec bash -c "${COMMAND}"
