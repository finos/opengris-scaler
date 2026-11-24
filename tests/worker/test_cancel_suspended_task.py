import os
import tempfile
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

CHILD_SECONDS = 3
POLL_SECONDS = 0.1
START_TIMEOUT_SECONDS = 30
SETTLE_SECONDS = CHILD_SECONDS + 5  # the child finishes, and the freed worker would start a stale task by then
PARENT_LINE = "parent\n"
CHILD_LINE = "child\n"


def _record(log_path: str, line: str) -> None:
    with open(log_path, "a") as log:
        log.write(line)


def child_task(log_path: str, seconds: float) -> float:
    _record(log_path, CHILD_LINE)
    time.sleep(seconds)
    return seconds


def parent_task(log_path: str) -> float:
    """Submits a child, whose higher priority suspends this task on a single worker."""
    _record(log_path, PARENT_LINE)
    with Client() as client:
        return client.submit(child_task, log_path, CHILD_SECONDS).result()


class TestCancelSuspendedTask(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.cluster = SchedulerClusterCombo(n_workers=1)
        self.addCleanup(self.cluster.shutdown)

        log_fd, self.log_path = tempfile.mkstemp(prefix="scaler_cancel_suspended_", suffix=".log")
        os.close(log_fd)
        self.addCleanup(os.unlink, self.log_path)

    def __lines(self) -> list:
        with open(self.log_path) as log:
            return log.readlines()

    def test_a_cancelled_suspended_task_never_starts_again(self) -> None:
        with Client(address=self.cluster.get_address()) as client:
            parent = client.submit(parent_task, self.log_path)

            deadline = time.monotonic() + START_TIMEOUT_SECONDS
            while CHILD_LINE not in self.__lines():
                self.assertLess(time.monotonic(), deadline, "the child task did not start")
                time.sleep(POLL_SECONDS)

            self.assertTrue(parent.cancel(), "the suspended parent task was not cancelled")
            time.sleep(SETTLE_SECONDS)

        self.assertEqual(self.__lines().count(PARENT_LINE), 1, "the cancelled parent task ran again")
