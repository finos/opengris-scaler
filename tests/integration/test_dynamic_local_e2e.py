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
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from tests.integration import INTEGRATION_SKIP_REASON, RUN_INTEGRATION_TESTS
from tests.integration._harness import SchedulerHarness
from tests.utility.utility import logging_test_name


def _square(value: int) -> int:
    return value * value


def _run_native_worker_manager(scheduler_address: str, max_task_concurrency: int) -> None:
    """Entry point for the dynamic (auto-scaling) native worker manager subprocess."""
    from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

    manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_manager_id="wm-dynamic-it",
                object_storage_address=None,
                max_task_concurrency=max_task_concurrency,
            ),
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({}),
                per_worker_task_queue_size=10,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
                garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
                io_threads=DEFAULT_IO_THREADS,
                event_loop="builtin",
            ),
            logging_config=LoggingConfig(paths=("/dev/stdout",), level="INFO", config_file=None),
        )
    )
    manager.run()


@unittest.skipUnless(RUN_INTEGRATION_TESTS, INTEGRATION_SKIP_REASON)
@unittest.skipIf(
    sys.platform == "win32",
    "Declarative scale-down calls stop_units mid-test, which has no graceful SIGINT path on "
    "Windows (see tests/scheduler/test_scaling.py). The provisioning control loop itself is "
    "cross-platform; only the teardown timing differs.",
)
class TestDynamicLocalProvisioningE2E(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.harness = SchedulerHarness(scaling_policy="allocate=even_load; scaling=vanilla")
        self.addCleanup(self.harness.shutdown)

    def _start_worker_manager(self, max_task_concurrency: int = 4):
        # NOT daemon=True: the DYNAMIC provisioner spawns its own worker subprocesses, and
        # daemonic processes are not allowed to have children. addCleanup guarantees teardown.
        process = get_context("spawn").Process(
            target=_run_native_worker_manager, args=(self.harness.scheduler_address, max_task_concurrency)
        )
        process.start()
        self.addCleanup(self._stop_process, process)
        return process

    @staticmethod
    def _stop_process(process) -> None:
        if process.is_alive():
            process.terminate()
            process.join(timeout=15)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)

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
