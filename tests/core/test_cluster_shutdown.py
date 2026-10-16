import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

QUEUED_TASK_COUNT = 4
TASK_SECONDS = 2
DISPATCH_SECONDS = 1  # long enough for the scheduler to queue the tasks on the worker
LONG_TASK_SECONDS = 600  # a forced shutdown must not wait for it

# Well under the worker death timeout and the manager drain timeout, 300s each by default: a worker that waits for
# either has missed the scheduler's cancels.
SHUTDOWN_DEADLINE_SECONDS = 30


def noop_sleep(seconds: float) -> float:
    # a Python function: Python 3.10 cannot read the signature of the builtin time.sleep, which submit needs
    time.sleep(seconds)
    return seconds


class TestClusterShutdown(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_scheduler_outlives_the_draining_fleet(self) -> None:
        """A client shutdown drains workers with queued tasks promptly, and the scheduler exits after its managers."""
        cluster = SchedulerClusterCombo(n_workers=1, per_worker_task_queue_size=QUEUED_TASK_COUNT, protected=False)
        self.addCleanup(cluster.shutdown)
        address = cluster.get_address()

        # The tasks belong to a client that stays connected: a shutting-down client cancels its own tasks first.
        with Client(address=address) as task_client:
            task_client.submit(noop_sleep, 0).result()  # the worker is up

            for _ in range(QUEUED_TASK_COUNT):
                task_client.submit(noop_sleep, TASK_SECONDS)
            time.sleep(DISPATCH_SECONDS)

            with Client(address=address) as shutdown_client:
                shutdown_client.shutdown()

            manager_process = cluster._worker_manager_process
            scheduler_process = cluster._scheduler

            deadline = time.monotonic() + SHUTDOWN_DEADLINE_SECONDS
            while manager_process.is_alive() and time.monotonic() < deadline:
                self.assertTrue(scheduler_process.is_alive(), "the scheduler exited before its worker manager")
                time.sleep(0.1)

            self.assertFalse(
                manager_process.is_alive(), f"the worker manager did not exit in {SHUTDOWN_DEADLINE_SECONDS}s"
            )

            scheduler_process.join(SHUTDOWN_DEADLINE_SECONDS)
            self.assertFalse(scheduler_process.is_alive(), "the scheduler did not exit after its worker manager")

    def test_combo_shutdown_drops_running_tasks(self) -> None:
        """SchedulerClusterCombo.shutdown() stops a busy cluster promptly, every process exiting cleanly."""
        cluster = SchedulerClusterCombo(n_workers=1, per_worker_task_queue_size=QUEUED_TASK_COUNT)
        address = cluster.get_address()

        with Client(address=address) as task_client:
            task_client.submit(noop_sleep, 0).result()  # the worker is up

            for _ in range(QUEUED_TASK_COUNT):
                task_client.submit(noop_sleep, LONG_TASK_SECONDS)
            time.sleep(DISPATCH_SECONDS)

            shutdown_start = time.monotonic()
            cluster.shutdown()
            shutdown_seconds = time.monotonic() - shutdown_start

        self.assertLess(shutdown_seconds, SHUTDOWN_DEADLINE_SECONDS)
        self.assertEqual(cluster._worker_manager_process.exitcode, 0)
        self.assertEqual(cluster._scheduler.exitcode, 0)
