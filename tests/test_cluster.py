import logging
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger, get_logger_info
from scaler.utility.network_util import get_available_tcp_port
from tests.utility import logging_test_name


class TestCluster(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.log_format, self.log_level_str, self.log_path = get_logger_info(logging.getLogger())

    def test_manual_storage_address(self):
        N_TASKS = 8
        N_WORKERS = N_TASKS

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        storage_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(
            address=address,
            storage_address=storage_address,
            n_workers=N_WORKERS,
            logging_format=self.log_format,
            logging_level=self.log_level_str,
            logging_paths=tuple(self.log_path,),
        )

        with Client(address=address) as client:
            future = client.submit(round, 3.14)
            self.assertEqual(future.result(), round(3.14))

        combo.shutdown()
