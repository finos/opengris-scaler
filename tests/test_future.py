import math
import time
import unittest
from concurrent.futures import CancelledError, as_completed
from threading import Event

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port


def noop_sleep(sec: int):
    time.sleep(sec)


class TestFuture(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._workers = 3
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()
        pass

    def test_callback(self):
        done_called_event = Event()

        def on_done_callback(fut):
            self.assertAlmostEqual(fut.result(), 4.0)
            done_called_event.set()

        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, 16.0)
            fut.add_done_callback(on_done_callback)
            done_called_event.wait()  # wait for the callback to be called, DO NOT call result().

    def test_as_completed(self):
        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, 100.0)

            for finished in as_completed([fut], timeout=5):
                self.assertAlmostEqual(finished.result(), 10.0)

    def test_state(self):
        with Client(address=self.address) as client:
            fut = client.submit(noop_sleep, 1.0)
            self.assertTrue(fut.running())

            fut.result()
            self.assertTrue(fut.done())

    def test_cancel(self):
        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, 100.0)
            fut.cancel()

            self.assertTrue(fut.cancelled())

            with self.assertRaises(CancelledError):
                fut.result()

    def test_exception(self):
        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, "16")

            with self.assertRaises(TypeError):
                fut.result()

            self.assertIsInstance(fut.exception(), TypeError)

    def test_client_disconnected(self):
        with Client(address=self.address) as client:
            fut = client.submit(noop_sleep, 10.0)

        with self.assertRaises(CancelledError):
            fut.result()
