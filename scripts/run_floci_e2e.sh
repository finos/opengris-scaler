#!/bin/bash
#
# Run the floci-backed ECS scaling e2e locally: build the ECS task image from the current build, start a
# floci AWS emulator, then run the test. The SHIPPED ECS worker manager provisions real task containers on
# floci and a real scheduler + object storage + web GUI come up -- the harness prints a
# "web GUI: http://localhost:PORT" line you can open to watch the scaling happen while the test runs.
#
# Usage:
#       ./scripts/run_floci_e2e.sh
#       DOCKER="sudo docker" ./scripts/run_floci_e2e.sh      # if the docker socket is root-only
#
# Notes:
#   * Requires the package installed with the gui extras and a built wheel under dist/. If no wheel is
#     present one is built (recompiles the C++ extensions, may take a few minutes). After changing PRODUCT
#     code, rebuild the wheel (`python -m build --wheel`) so the task containers run the new build.
#   * The ECS task image is always rebuilt (cheap when the wheel is unchanged -- Docker caches the layers).
#   * floci runs the ECS RunTask containers on the host Docker daemon via the mounted socket, so it needs
#     Docker; the image (floci/floci:latest) is pulled on first use.
set -euo pipefail

DOCKER="${DOCKER:-sudo docker}"
PYTHON="${PYTHON:-python}"

if ! ls dist/*.whl >/dev/null 2>&1; then
    echo "No wheel under dist/; building one (recompiles the C++ extensions, may take a few minutes)..."
    "${PYTHON}" -m build --wheel
fi

export RUN_FLOCI_E2E=1
export SCALER_IT_CONTAINER_CLI="${SCALER_IT_CONTAINER_CLI:-${DOCKER}}"
# Rebuild the task image so a wheel change is never silently run against a stale image.
export SCALER_IT_REBUILD=1

exec "${PYTHON}" -m unittest tests.integration.test_ecs_scaling_e2e -v "$@"
