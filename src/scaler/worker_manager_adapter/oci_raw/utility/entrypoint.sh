#!/bin/bash
set -e

if [ -n "${PYTHON_REQUIREMENTS}" ]; then
    echo "Installing Python requirements..."
    printf '%s\n' "${PYTHON_REQUIREMENTS}" > /tmp/requirements.txt
    pip install --no-cache-dir -q -r /tmp/requirements.txt
fi

if [ -z "${COMMAND}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

echo "Executing: ${COMMAND}"
exec bash -c "${COMMAND}"
