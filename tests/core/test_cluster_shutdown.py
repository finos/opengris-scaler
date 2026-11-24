import threading
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.defaults import DRAIN_TIMEOUT_DESTROY_AT_ONCE
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

QUEUED_TASK_COUNT = 4
TASK_SECONDS = 2
DISPATCH_SECONDS = 1  # long enough for the scheduler to queue the tasks on the worker
LONG_TASK_SECONDS = 600  # a shutdown with a drain deadline, or without a drain, must not wait for it
SHORT_DRAIN_TIMEOUT_SECONDS = 2

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

    def test_combo_shutdown_lets_a_running_task_finish(self) -> None:
        """By default, SchedulerClusterCombo.shutdown() drains: the running task returns its result first."""
        cluster = SchedulerClusterCombo(n_workers=1)

        with Client(address=cluster.get_address()) as task_client:
            task_client.submit(noop_sleep, 0).result()  # the worker is up

            future = task_client.submit(noop_sleep, TASK_SECONDS)
            time.sleep(DISPATCH_SECONDS)

            # The result is read while the shutdown drains: shutdown() ends by stopping the object storage it lives in.
            shutdown_thread = threading.Thread(target=cluster.shutdown)
            shutdown_thread.start()
            self.addCleanup(shutdown_thread.join)
            self.assertEqual(future.result(timeout=SHUTDOWN_DEADLINE_SECONDS), TASK_SECONDS)
            shutdown_thread.join(SHUTDOWN_DEADLINE_SECONDS)
            self.assertFalse(shutdown_thread.is_alive(), f"the shutdown did not end in {SHUTDOWN_DEADLINE_SECONDS}s")

        self.assertEqual(cluster._worker_manager_process.exitcode, 0)
        self.assertEqual(cluster._scheduler.exitcode, 0)

    def test_combo_shutdown_without_a_drain_drops_running_tasks(self) -> None:
        """With drain_timeout_seconds=0, SchedulerClusterCombo.shutdown() stops a busy cluster at once."""
        self.__assert_prompt_clean_shutdown(DRAIN_TIMEOUT_DESTROY_AT_ONCE)

    def test_combo_shutdown_ends_a_drain_at_its_deadline(self) -> None:
        """A drain deadline bounds SchedulerClusterCombo.shutdown() when a task runs longer."""
        self.__assert_prompt_clean_shutdown(SHORT_DRAIN_TIMEOUT_SECONDS)

    def test_combo_shutdown_after_the_scheduler_crashed(self) -> None:
        """SchedulerClusterCombo.shutdown() still stops every process when the scheduler died first."""
        cluster = SchedulerClusterCombo(n_workers=1)
        cluster._scheduler.kill()
        cluster._scheduler.join()

        shutdown_start = time.monotonic()
        cluster.shutdown()

        self.assertLess(time.monotonic() - shutdown_start, SHUTDOWN_DEADLINE_SECONDS)
        self.assertFalse(cluster._worker_manager_process.is_alive())
        self.assertFalse(cluster._object_storage.is_alive())

    def __assert_prompt_clean_shutdown(self, drain_timeout_seconds: int) -> None:
        cluster = SchedulerClusterCombo(
            n_workers=1, per_worker_task_queue_size=QUEUED_TASK_COUNT, drain_timeout_seconds=drain_timeout_seconds
        )

        with Client(address=cluster.get_address()) as task_client:
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
