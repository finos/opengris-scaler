#!/bin/bash
#
# Run the floci-backed EC2 scaling e2e locally: build a manylinux wheel of the CURRENT source (so the
# instance runs this branch, not a PyPI release), start a floci emulator, and drive the shipped ORB/EC2
# worker manager through the scale curve on real Amazon Linux 2023 instances. A real scheduler + object
# storage + web GUI come up; the harness prints a "web GUI: http://localhost:PORT" line to watch scaling.
#
# Usage:
#       ./scripts/run_ec2_e2e.sh
#       DOCKER="sudo docker" ./scripts/run_ec2_e2e.sh      # if the docker socket is root-only
#
# Notes:
#   * The manylinux wheel is built with cibuildwheel (compiles the C++ ext + thirdparties in a manylinux
#     container -- several minutes) only if one is not already under dist_manylinux/. After changing PRODUCT
#     code, delete dist_manylinux/*.whl to force a rebuild so the instance runs the new source.
#   * The host also needs scaler installed with the [orb] extra (the manager process runs the ORB SDK).
set -euo pipefail

DOCKER="${DOCKER:-sudo docker}"
PYTHON="${PYTHON:-python}"
WHEEL_DIR="${SCALER_IT_MANYLINUX_WHEEL_DIR:-dist_manylinux}"

if ! ls "${WHEEL_DIR}"/*manylinux*.whl >/dev/null 2>&1; then
    cpython_tag="cp$(${PYTHON} -c 'import sys; print(f"{sys.version_info.major}{sys.version_info.minor}")')"
    echo "No manylinux wheel under ${WHEEL_DIR}; building ${cpython_tag} with cibuildwheel (several minutes)..."
    DOCKER="${DOCKER}" PYTHON="${PYTHON}" CIBW_OUTPUT_DIR="${WHEEL_DIR}" \
        ./scripts/build_cibuildwheel.sh --only "${cpython_tag}-manylinux_x86_64"
fi

export RUN_EC2_E2E=1
export SCALER_IT_CONTAINER_CLI="${SCALER_IT_CONTAINER_CLI:-${DOCKER}}"
export SCALER_IT_MANYLINUX_WHEEL_DIR="${WHEEL_DIR}"

exec "${PYTHON}" -m unittest tests.integration.e2e.test_scaling -v "$@"
