#!/bin/sh
set -e

if [ -z "${PYTHON_VERSION}" ]; then
    echo "ERROR: PYTHON_VERSION environment variable is not set." >&2
    exit 1
fi

if [ -z "${PYTHON_REQUIREMENTS}" ]; then
    echo "ERROR: PYTHON_REQUIREMENTS environment variable is not set." >&2
    exit 1
fi

if [ -z "${COMMAND}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

uv python install "${PYTHON_VERSION}"
uv venv --python "${PYTHON_VERSION}" /opt/opengris-scaler

printf '%s\n' "${PYTHON_REQUIREMENTS}" > /tmp/requirements.txt

# Source installs need C++ build dependencies for Scaler's native extensions.
if grep -qE 'git\+|@ git\+' /tmp/requirements.txt; then
    apk add --no-cache \
        ca-certificates git cmake gcc g++ make pkgconf \
        capnproto capnproto-dev \
        libuv-dev openssl-dev
fi

uv pip install --no-cache -q --python /opt/opengris-scaler -r /tmp/requirements.txt

ln -sf /opt/opengris-scaler/bin/scaler /usr/local/bin/scaler
ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/

echo "Executing: ${COMMAND}"
exec sh -c "${COMMAND}"
