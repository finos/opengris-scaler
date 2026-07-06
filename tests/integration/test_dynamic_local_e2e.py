"""End-to-end data-plane test: dynamic provisioning that actually executes real tasks.

Neither moto nor community LocalStack boots the containers they "provision", so the cloud
control-plane test (test_ecs_provisioning_moto.py) cannot verify real task execution. This
test closes that gap using the local ``NativeWorkerProvisioner`` -- which provisions real
worker *processes* the same way the ECS/ORB managers provision cloud instances (both are
``DeclarativeWorkerProvisioner`` driven by the same scheduler command over the same wire).

It proves the whole dynamic-provisioning control loop with real components:

    client submits tasks -> scheduler's scaling policy emits setDesiredTaskConcurrency
      -> WorkerManagerRunner (real network) -> provisioner scales up real workers
        -> workers register and execute -> results flow back to the client

Getting correct results back is itself proof that provisioning happened: the scheduler
starts with zero workers, so nothing can compute until the manager scales up.
"""

import sys
import unittest
from multiprocessing import get_context

from scaler import Client
from scaler.utility.logging.utility import setup_logger
from tests.integration import INTEGRATION_SKIP_REASON, RUN_INTEGRATION_TESTS
from tests.integration._harness import SchedulerHarness, run_native_worker_manager
from tests.utility.utility import logging_test_name, terminate_process


def _square(value: int) -> int:
    return value * value


@unittest.skipUnless(RUN_INTEGRATION_TESTS, INTEGRATION_SKIP_REASON)
@unittest.skipIf(
    sys.platform == "win32",
    "Declarative scale-down calls stop_units mid-test, which has no graceful SIGINT path on "
    "Windows (see tests/scheduler/test_scaling.py).",
)
class TestDynamicLocalProvisioningE2E(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.harness = SchedulerHarness(scaling_policy="allocate=even_load; scaling=vanilla")
        self.addCleanup(self.harness.shutdown)

    def _start_worker_manager(self, max_task_concurrency: int = 4):
        # NOT daemon=True: the DYNAMIC provisioner spawns its own worker subprocesses, and daemonic
        # processes are not allowed to have children. addCleanup guarantees teardown.
        process = get_context("spawn").Process(
            target=run_native_worker_manager, args=(self.harness.scheduler_address, max_task_concurrency)
        )
        process.start()
        self.addCleanup(terminate_process, process)
        return process

    def test_dynamic_provisioning_executes_tasks(self) -> None:
        """With zero pre-provisioned workers, submitted work forces the manager to scale up
        real workers that compute the results."""
        self._start_worker_manager(max_task_concurrency=4)

        inputs = list(range(24))
        with Client(self.harness.scheduler_address) as client:
            results = client.map(_square, inputs)

        self.assertEqual(results, [value * value for value in inputs])

    def test_single_submit_scales_from_zero(self) -> None:
        """A single blocking submit still returns, proving at least one worker was provisioned."""
        self._start_worker_manager(max_task_concurrency=2)

        with Client(self.harness.scheduler_address) as client:
            future = client.submit(_square, 7)
            self.assertEqual(future.result(), 49)


if __name__ == "__main__":
    unittest.main()
