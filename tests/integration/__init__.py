"""End-to-end integration test skeleton (see README.md).

Opt-in: these full-stack tests are skipped by the default ``unittest discover`` run so they do
not add process-spawning / cloud-mock overhead to the standard CI build. Enable them with::

    RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v
"""

import os

RUN_INTEGRATION_TESTS = os.environ.get("RUN_INTEGRATION_TESTS") == "1"
INTEGRATION_SKIP_REASON = "set RUN_INTEGRATION_TESTS=1 to enable the end-to-end integration tests"

# A separate, heavier gate for the process scaling test (spawns many processes). Kept out of the
# default integration run so it only executes when explicitly requested (e.g. the manually-triggered
# CI workflow), independent of RUN_INTEGRATION_TESTS.
RUN_PROCESS_SCALING_TEST = os.environ.get("RUN_PROCESS_SCALING_TEST") == "1"
PROCESS_SCALING_SKIP_REASON = "set RUN_PROCESS_SCALING_TEST=1 to enable the process scaling test"

# A separate, heavier gate for the container-scaling e2e: it launches real Docker containers (each
# a fixed worker manager) and bind-mounts the host's built scaler into them, so it needs a Docker daemon
# and cannot run in the standard CI lanes. Independent of RUN_INTEGRATION_TESTS; opt in explicitly.
RUN_CONTAINER_E2E = os.environ.get("RUN_CONTAINER_E2E") == "1"
CONTAINER_E2E_SKIP_REASON = "set RUN_CONTAINER_E2E=1 to enable the container-scaling e2e"
