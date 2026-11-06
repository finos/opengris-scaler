import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name


class TestProtected(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_protected_true(self) -> None:
        cluster = SchedulerClusterCombo(n_workers=2, per_worker_task_queue_size=2, event_loop="builtin", protected=True)
        address = cluster.get_address()
        print("wait for 3 seconds")
        time.sleep(3)

        with self.assertRaises(ValueError):
            client = Client(address=address)
            client.shutdown()

        time.sleep(3)

        cluster.shutdown()

    def test_protected_false(self) -> None:
        cluster = SchedulerClusterCombo(
            n_workers=2, per_worker_task_queue_size=2, event_loop="builtin", protected=False
        )
        address = cluster.get_address()
        print("wait for 3 seconds")
        time.sleep(3)

        client = Client(address=address)
        client.shutdown()

        # wait scheduler received shutdown instruction and kill all workers, then call cluster.shutdown
        time.sleep(3)
        print("finish slept 3 seconds")
        cluster.shutdown()
