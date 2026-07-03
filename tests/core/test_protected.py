import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import POLL_INTERVAL_SECONDS, PROCESS_TERMINATION_TIMEOUT_SECONDS, logging_test_name


class TestProtected(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def _wait_until(self, predicate) -> bool:
        deadline = time.monotonic() + PROCESS_TERMINATION_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(POLL_INTERVAL_SECONDS)
        return predicate()

    def test_protected_true(self) -> None:
        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address, n_workers=2, per_worker_task_queue_size=2, event_loop="builtin", protected=True
        )
        self.addCleanup(cluster.shutdown)

        # Client construction blocks until the scheduler is reachable, so no fixed readiness sleep is needed.
        # Client.shutdown() always tears the client down in its own finally, so it needs no separate cleanup.
        client = Client(address=address)

        with self.assertRaises(ValueError):
            client.shutdown()

        # Protected mode must reject the shutdown request, so the scheduler stays alive.
        self.assertTrue(cluster._scheduler.is_alive())

    def test_protected_false(self) -> None:
        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address, n_workers=2, per_worker_task_queue_size=2, event_loop="builtin", protected=False
        )
        self.addCleanup(cluster.shutdown)

        # Client construction blocks until the scheduler is reachable, so no fixed readiness sleep is needed.
        client = Client(address=address)

        # Not protected: shutdown must be accepted and return without raising.
        client.shutdown()

        # After accepting the shutdown request the scheduler quits, so its process must stop being alive.
        self.assertTrue(self._wait_until(lambda: not cluster._scheduler.is_alive()))
