import logging
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger, get_logger_info
from scaler.utility.network_util import get_available_tcp_port
from tests.utility import logging_test_name


class TestProtected(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.log_format, self.log_level_str, self.log_path = get_logger_info(logging.getLogger())

    def test_protected_true(self) -> None:
        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address,
            n_workers=2,
            per_worker_task_queue_size=2,
            event_loop="builtin",
            protected=True,
            logging_format=self.log_format,
            logging_level=self.log_level_str,
            logging_paths=tuple(self.log_path,),
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
            address=address, n_workers=2, per_worker_task_queue_size=2, event_loop="builtin", protected=False
        )
        print("wait for 3 seconds")
        time.sleep(3)

        client = Client(address=address)
        client.shutdown()

        # wait scheduler received shutdown instruction and kill all workers, then call cluster.shutdown
        time.sleep(3)
        print("finish slept 3 seconds")
        cluster.shutdown()
