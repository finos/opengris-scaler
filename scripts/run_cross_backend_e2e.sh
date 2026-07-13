#!/bin/bash
#
# Run the floci-backed cross-backend waterfall e2e locally: build a manylinux wheel of the CURRENT source
# (so EC2 instances run this branch, not a PyPI release), start a floci emulator, and drive the shipped ECS
# worker manager (priority 1) and the shipped ORB/EC2 worker manager (priority 2) behind ONE scheduler
# running the waterfall policy. Work fills the fast ECS pool first and spills onto real Amazon Linux 2023
# instances once ECS saturates. A real scheduler + object storage + web GUI come up; the harness prints a
# "web GUI: http://localhost:PORT" line to watch the spill happen.
#
# Usage:
#       ./scripts/run_cross_backend_e2e.sh
#       DOCKER="sudo docker" ./scripts/run_cross_backend_e2e.sh      # if the docker socket is root-only
#
# Tunables (same knobs as the workflow_dispatch inputs; each is inherited by the test process):
#       SCALER_IT_WATERFALL_POLICY  raw waterfall_v1 policy, one rule/line "priority,worker_manager_id[,cap]"
#                                   (ids: wm-ecs-p1, wm-ec2-p2); blank = default
#       SCALER_IT_NUM_TASKS         tasks per burst wave
#       SCALER_IT_TASK_SECONDS      sleep per task, seconds
#   e.g. SCALER_IT_NUM_TASKS=60 SCALER_IT_TASK_SECONDS=0.3 ./scripts/run_cross_backend_e2e.sh
#
# Notes:
#   * The manylinux wheel is built with cibuildwheel (compiles the C++ ext + thirdparties in a manylinux
#     container -- several minutes) only if one is not already under dist_manylinux/. After changing PRODUCT
#     code, delete dist_manylinux/*.whl to force a rebuild so the instances run the new source.
#   * The host also needs scaler installed with the [orb] extra (the EC2 manager process runs the ORB SDK).
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

export RUN_CROSS_BACKEND_E2E=1
export SCALER_IT_CONTAINER_CLI="${SCALER_IT_CONTAINER_CLI:-${DOCKER}}"
export SCALER_IT_MANYLINUX_WHEEL_DIR="${WHEEL_DIR}"

exec "${PYTHON}" -m unittest tests.integration.e2e.test_scaling -v "$@"
