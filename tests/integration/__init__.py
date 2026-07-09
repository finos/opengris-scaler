"""End-to-end integration tests (see README.md).

Opt-in: these full-stack tests are skipped by the default ``unittest discover`` run so they do
not add process-spawning / cloud-mock overhead to the standard CI build. Enable them with::

    RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v
"""

import os

RUN_INTEGRATION_TESTS = os.environ.get("RUN_INTEGRATION_TESTS") == "1"
INTEGRATION_SKIP_REASON = "set RUN_INTEGRATION_TESTS=1 to enable the end-to-end integration tests"

# A separate, heavier gate for the container-scaling e2e: it launches real Docker containers (each a
# fixed worker manager) that run the self-contained worker image, so it needs a Docker daemon and cannot
# run in the standard CI lanes. Independent of RUN_INTEGRATION_TESTS; opt in explicitly.
RUN_CONTAINER_E2E = os.environ.get("RUN_CONTAINER_E2E") == "1"
CONTAINER_E2E_SKIP_REASON = "set RUN_CONTAINER_E2E=1 to enable the container-scaling e2e"

# The floci-backed ECS e2e drives the shipped ECS worker manager against a local floci emulator that
# launches real task containers on the host Docker daemon. Like the container e2e it needs Docker and runs
# on demand only, so it has its own explicit gate.
RUN_FLOCI_E2E = os.environ.get("RUN_FLOCI_E2E") == "1"
FLOCI_E2E_SKIP_REASON = "set RUN_FLOCI_E2E=1 to enable the floci-backed ECS scaling e2e"
