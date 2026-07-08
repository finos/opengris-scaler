#!/bin/bash
#
# Run the container-scaling e2e locally: build the self-contained worker image from the current build,
# then run the test. A real scheduler + object storage come up and the harness starts the web GUI wired
# to the scheduler monitor -- it prints a "web GUI: http://localhost:PORT" line you can open to watch the
# scaling happen while the test runs.
#
# Usage:
#       ./scripts/run_container_e2e.sh
#       DOCKER="sudo docker" ./scripts/run_container_e2e.sh      # if the docker socket is root-only
#
# Notes:
#   * Requires the package installed with the gui extras and a built wheel under dist/. If no wheel is
#     present one is built (this recompiles the C++ extensions and can take a few minutes). After changing
#     PRODUCT code, rebuild the wheel (`python -m build --wheel`) so the container runs the new build.
#   * The worker image is always rebuilt (cheap when the wheel is unchanged -- Docker caches the layers).
#
set -euo pipefail

DOCKER="${DOCKER:-sudo docker}"
PYTHON="${PYTHON:-python}"

if ! ls dist/*.whl >/dev/null 2>&1; then
    echo "No wheel under dist/; building one (recompiles the C++ extensions, may take a few minutes)..."
    "${PYTHON}" -m build --wheel
fi

export RUN_CONTAINER_E2E=1
export SCALER_IT_CONTAINER_CLI="${SCALER_IT_CONTAINER_CLI:-${DOCKER}}"
# Rebuild the worker image so a wheel change is never silently run against a stale image.
export SCALER_IT_REBUILD=1

exec "${PYTHON}" -m unittest tests.integration.test_container_scaling_e2e -v "$@"
