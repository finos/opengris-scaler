import time
import unittest

from scaler import Client, SchedulerClusterCombo

from tests.utility import get_available_tcp_port


def dummy(n: int):
    time.sleep(n)
    return n * n


class TestProfiling(unittest.TestCase):
    def setUp(self):
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.cluster = SchedulerClusterCombo(
            address=self.address, n_workers=2, per_worker_queue_size=2, event_loop="builtin"
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
