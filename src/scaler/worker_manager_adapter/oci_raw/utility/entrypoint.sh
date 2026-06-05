#!/bin/bash
set -e

uv python install "${PYTHON_VERSION}"
uv venv --python "${PYTHON_VERSION}" /opt/opengris-scaler

printf '%s\n' "${PYTHON_REQUIREMENTS}" > /tmp/requirements.txt
uv pip install --no-cache -q --python /opt/opengris-scaler -r /tmp/requirements.txt

ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/

if [ -z "${COMMAND}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

echo "Executing: ${COMMAND}"
exec bash -c "${COMMAND}"
