import os
import unittest

import psutil

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

# Short, so the scheduler forgets the killed worker instead of assigning the next task to it.
WORKER_TIMEOUT_SECONDS = 5

REPLACEMENT_TIMEOUT_SECONDS = 60


def _processor_pid() -> int:
    return os.getpid()


class TestNativeWorkerSupervision(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_a_killed_worker_is_replaced(self) -> None:
        """The native manager replaces a worker that dies without being asked to stop."""
        combo = SchedulerClusterCombo(n_workers=1, worker_timeout_seconds=WORKER_TIMEOUT_SECONDS)
        try:
            with Client(address=combo.get_address()) as client:
                processor_pid = client.submit(_processor_pid).result()
                agent = psutil.Process(processor_pid).parent()
                assert agent is not None
                agent.kill()

                replacement_pid = client.submit(_processor_pid).result(timeout=REPLACEMENT_TIMEOUT_SECONDS)
                self.assertNotEqual(replacement_pid, processor_pid)
        finally:
            combo.shutdown()
