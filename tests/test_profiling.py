import logging
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger, get_logger_info
from scaler.utility.network_util import get_available_tcp_port
from tests.utility import logging_test_name


def dummy(n: int):
    time.sleep(n)
    return n * n


def busy_dummy(n: int):
    start_time = time.time()
    while time.time() - start_time < n:
        pass

    return n * n


class TestProfiling(unittest.TestCase):
    def setUp(self):
        setup_logger()
        logging_test_name(self)
        self.log_format, self.log_level_str, self.log_path = get_logger_info(logging.getLogger())

        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.cluster = SchedulerClusterCombo(
            address=self.address,
            n_workers=2,
            per_worker_task_queue_size=2,
            event_loop="builtin",
            logging_format=self.log_format,
            logging_level=self.log_level_str,
            logging_paths=tuple(self.log_path,),
        )
        self.client = Client(address=self.address, profiling=True)
        self.client_off = Client(address=self.address, profiling=False)

    def tearDown(self) -> None:
        self.client.disconnect()
        self.client_off.disconnect()
        self.cluster.shutdown()

    def test_future_completed(self) -> None:
        fut = self.client.submit(dummy, 1)
        fut.result()

        task_time = fut.profiling_info().duration_s
        assert task_time > 0

    def test_future_incomplete(self) -> None:
        fut = self.client.submit(dummy, 1, profiling=True)

        # Raises error because future is not done
        with self.assertRaises(ValueError):
            _ = fut.profiling_info().duration_s

    def test_cpu_time_busy(self) -> None:
        fut = self.client.submit(busy_dummy, 1, profiling=True)
        fut.result()

        cpu_time = fut.profiling_info().cpu_time_s
        assert cpu_time > 0

    def test_cpu_time_sleep(self) -> None:
        fut = self.client.submit(dummy, 1, profiling=True)
        fut.result()

        cpu_time = fut.profiling_info().cpu_time_s
        assert cpu_time < 1
