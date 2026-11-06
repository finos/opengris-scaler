import os
import tempfile
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

SCHEDULER_EXIT_TIMEOUT_SECONDS = 30


def noop_sleep(seconds: float) -> float:
    # a Python function: Python 3.10 cannot read the signature of the builtin time.sleep, which submit needs
    time.sleep(seconds)
    return seconds


class TestSchedulerShutdown(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_a_client_shutdown_logs_no_scheduler_error(self) -> None:
        """A requested shutdown is not a failure: the scheduler logs no error while it stops."""
        log_directory = tempfile.TemporaryDirectory()
        self.addCleanup(log_directory.cleanup)
        log_path = os.path.join(log_directory.name, "cluster.log")
        cluster = SchedulerClusterCombo(n_workers=1, protected=False, logging_paths=(log_path,))
        address = cluster.get_address()
        self.addCleanup(cluster.shutdown)

        with Client(address=address) as client:
            client.submit(noop_sleep, 0).result()  # the cluster is up
            client.shutdown()

        cluster._scheduler.join(SCHEDULER_EXIT_TIMEOUT_SECONDS)
        self.assertFalse(cluster._scheduler.is_alive(), "the scheduler did not exit")

        # the cluster processes share the log file; each line carries the pid of the process that wrote it
        scheduler_tag = f"[{cluster._scheduler.pid}]"
        with open(log_path, encoding="utf-8", errors="replace") as cluster_log:
            error_lines = [line for line in cluster_log if scheduler_tag in line and " ERROR " in line]
        self.assertEqual(error_lines, [])
