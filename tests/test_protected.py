import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port, logging_test_name


class TestProtected(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_protected_true(self) -> None:
        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address, n_workers=2, per_worker_queue_size=2, event_loop="builtin", protected=True
        )
        print("wait for 3 seconds")
        time.sleep(3)

        with self.assertRaises(ValueError):
            client = Client(address=address)
            client.shutdown()

        time.sleep(3)

        cluster.shutdown()

    def test_protected_false(self) -> None:
        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address, n_workers=2, per_worker_queue_size=2, event_loop="builtin", protected=False
        )
        print("wait for 3 seconds")
        time.sleep(3)

        client = Client(address=address)
        client.shutdown()

        # wait scheduler received shutdown instruction and kill all workers, then call cluster.shutdown
        time.sleep(3)
        print("finish slept 3 seconds")
        cluster.shutdown()
