"""End-to-end integration tests (see README.md).

Opt-in: these full-stack tests are skipped by the default ``unittest discover`` run so they do
not add process-spawning / cloud-mock overhead to the standard CI build. Enable them with::

    RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v
"""

import os
from typing import List

RUN_INTEGRATION_TESTS = os.environ.get("RUN_INTEGRATION_TESTS") == "1"
INTEGRATION_SKIP_REASON = "set RUN_INTEGRATION_TESTS=1 to enable the end-to-end integration tests"


def container_cli() -> List[str]:
    """The container CLI every Docker-backed e2e shells out to, e.g. ``["sudo", "docker"]`` or ``["docker"]``.
    Override with ``SCALER_IT_CONTAINER_CLI`` (a rootless docker or a podman shim). One definition so the
    default cannot drift across the harness, floci, image builder, and runtime."""
    return os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()


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

# The floci-backed EC2 e2e drives the shipped ORB/EC2 worker manager against floci, which launches real
# Amazon Linux 2023 instances that install a manylinux wheel of the current source and boot a worker. It
# needs Docker plus a prebuilt manylinux wheel, so it runs on demand only behind its own gate.
RUN_EC2_E2E = os.environ.get("RUN_EC2_E2E") == "1"
EC2_E2E_SKIP_REASON = "set RUN_EC2_E2E=1 to enable the floci-backed EC2 scaling e2e"

# The cross-backend waterfall e2e puts an ECS and an EC2 worker manager at different waterfall priorities on
# ONE scheduler and proves work fills the fast ECS pool first and spills to EC2. It needs everything both
# floci e2es do (Docker, both task images, and a prebuilt manylinux wheel), so it is the heaviest job and
# runs on demand only behind its own gate.
RUN_CROSS_BACKEND_E2E = os.environ.get("RUN_CROSS_BACKEND_E2E") == "1"
CROSS_BACKEND_E2E_SKIP_REASON = "set RUN_CROSS_BACKEND_E2E=1 to enable the floci-backed cross-backend waterfall e2e"
