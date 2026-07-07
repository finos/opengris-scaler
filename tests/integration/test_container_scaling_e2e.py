"""Container-scaling end-to-end skeleton: real workers in real containers, no cloud and no AWS mock.

A real client bursts work at a real scheduler; the scheduler's scaling policy drives the
``ContainerWorkerProvisioner`` to launch container "machines" (each a fixed ``baremetal_native`` worker
manager with its own IP), which run the tasks and return real results. This is the free, local base for
richer e2e scenarios -- scale up/down under load, multi-machine spread, and nested clients with distinct
IPs -- that no in-process or cloud-mock test can exercise.

Opt-in (``RUN_CONTAINER_E2E=1``) and Docker-only; each machine runs the host's built scaler via
read-only bind-mounts, so it is the exact version the scheduler speaks (see ``_container_backend``).
"""

from __future__ import annotations

import os
import subprocess
import unittest
from multiprocessing import get_context

from scaler import Client
from scaler.utility.logging.utility import setup_logger
from tests.integration import CONTAINER_E2E_SKIP_REASON, RUN_CONTAINER_E2E
from tests.integration._container_backend import run_container_worker_manager
from tests.integration._container_runtime import DockerRuntime
from tests.integration._harness import SchedulerHarness
from tests.integration._tasks import square
from tests.utility.utility import logging_test_name, terminate_process

_WORKERS_PER_MACHINE = 2
_MAX_MACHINES = 2
_MACHINE_PREFIX = "scaler-it-machine"
_CLI = os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()


@unittest.skipUnless(RUN_CONTAINER_E2E, CONTAINER_E2E_SKIP_REASON)
@unittest.skipUnless(DockerRuntime.is_available(), "Docker is required for the container-scaling e2e")
class TestContainerScalingE2E(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.runtime = DockerRuntime()
        self.addCleanup(self._remove_machines)  # last resort: reap any container the manager left behind
        self.harness = SchedulerHarness(gateway=self.runtime.host_gateway())
        self.addCleanup(self.harness.shutdown)
        self.manager = get_context("spawn").Process(
            target=run_container_worker_manager,
            args=(
                self.harness.scheduler_address,
                self.harness.worker_scheduler_address,
                _WORKERS_PER_MACHINE,
                _MAX_MACHINES,
            ),
        )
        self.manager.start()
        self.addCleanup(terminate_process, self.manager)

    def _remove_machines(self) -> None:
        ids = subprocess.run(
            [*_CLI, "ps", "-aq", "--filter", f"name={_MACHINE_PREFIX}"], capture_output=True, text=True
        ).stdout.split()
        for container in ids:
            subprocess.run([*_CLI, "rm", "-f", container], capture_output=True)

    def test_burst_scales_up_container_machines_and_runs_tasks(self) -> None:
        """From zero machines, a burst forces the scheduler to provision container machines that compute
        the results -- correct results are themselves proof the containers ran the work."""
        inputs = list(range(20))
        with Client(self.harness.scheduler_address) as client:
            results = client.map(square, inputs)
        self.assertEqual(results, [value * value for value in inputs])


if __name__ == "__main__":
    unittest.main()
