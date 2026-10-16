import multiprocessing
import os
import unittest

import psutil

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.utility import logging_test_name

# Short, so the scheduler forgets the killed worker instead of assigning the next task to it.
WORKER_TIMEOUT_SECONDS = 5

# Short, so the manager declares the killed worker lost well within the test's budget.
UNIT_TIMEOUT_SECONDS = 5

REPLACEMENT_TIMEOUT_SECONDS = 60


def _processor_pid() -> int:
    return os.getpid()


class TestNativeWorkerSupervision(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_a_killed_worker_is_replaced(self) -> None:
        """A worker killed without notice sends no heartbeat; its manager declares it lost and replaces it."""
        combo = SchedulerClusterCombo(n_workers=0, worker_timeout_seconds=WORKER_TIMEOUT_SECONDS)
        manager_process = multiprocessing.get_context("spawn").Process(
            target=NativeWorkerManager(
                NativeWorkerManagerConfig(
                    worker_manager_config=WorkerManagerConfig(
                        scheduler_address=AddressConfig.from_string(combo.get_address()),
                        worker_manager_id="supervision-test",
                        max_task_concurrency=1,
                        unit_timeout_seconds=UNIT_TIMEOUT_SECONDS,
                    ),
                    worker_config=WorkerConfig(heartbeat_interval_seconds=1),
                )
            ).run
        )
        try:
            manager_process.start()
            with Client(address=combo.get_address()) as client:
                processor_pid = client.submit(_processor_pid).result(timeout=REPLACEMENT_TIMEOUT_SECONDS)
                agent = psutil.Process(processor_pid).parent()
                assert agent is not None
                agent.kill()

                replacement_pid = client.submit(_processor_pid).result(timeout=REPLACEMENT_TIMEOUT_SECONDS)
                self.assertNotEqual(replacement_pid, processor_pid)
        finally:
            manager_process.terminate()
            manager_process.join()
            combo.shutdown()
