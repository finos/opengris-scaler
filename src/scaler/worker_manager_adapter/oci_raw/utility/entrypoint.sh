#!/bin/bash
set -e

# Install the requested Python version via pyenv if PYTHON_VERSION differs from the base image's Python.
# For simplicity, we run inside the base python:3.12-slim image and ignore PYTHON_VERSION.

# Install any extra Python requirements before starting the worker.
if [ -n "${PYTHON_REQUIREMENTS}" ]; then
    echo "Installing Python requirements: ${PYTHON_REQUIREMENTS}"
    # Requirements are semicolon-separated (same convention as the ECS adapter).
    echo "${PYTHON_REQUIREMENTS}" | tr ';' '\n' | xargs pip install --no-cache-dir -q
fi

# Install the scaler package itself so that scaler_cluster is available on PATH.
if [ -n "${SCALER_PACKAGE}" ]; then
    pip install --no-cache-dir -q "${SCALER_PACKAGE}"
fi

if [ -z "${COMMAND}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

echo "Executing: ${COMMAND}"
exec bash -c "${COMMAND}"
