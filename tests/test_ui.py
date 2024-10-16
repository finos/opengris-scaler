import random
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port, logging_test_name


def noop(sec: int):
    return sec * 1


def noop_sleep(sec: int):
    time.sleep(sec)
    return sec


def noop_memory(length: int):
    time.sleep(5)
    chunk_size = 1024
    chunks = []
    for i in range(length):
        chunks.append(bytearray(chunk_size))
    return len(chunks)


class TestUI(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._workers = 10
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()
        pass

    def test_noop(self):
        with Client(self.address) as client:
            tasks = [random.randint(2, 50) for _ in range(20)]
            with ScopedLogger(f"submit {len(tasks)} noop tasks"):
                futures = [client.submit(noop_sleep, i) for i in tasks]

            with ScopedLogger(f"gather {len(futures)} results"):
                results = [future.result() for future in futures]

            self.assertEqual(results, tasks)
            time.sleep(2)

            tasks = [random.randint(2, 50) for _ in range(20)]
            with ScopedLogger(f"submit {len(tasks)} noop tasks"):
                futures = [client.submit(noop_sleep, i) for i in tasks]

            with ScopedLogger(f"gather {len(futures)} results"):
                results = [future.result() for future in futures]

            self.assertEqual(results, tasks)

    def test_memory_usage(self):
        with Client(self.address) as client:
            tasks = [0, 1, 100, 1024, 1024 * 2, 1024 * 1024, 1024 * 1024 * 2, 1024 * 1024 * 4]
            results = []

            for task in tasks:
                with ScopedLogger(f"submit {task} memory task"):
                    future = client.submit(noop_memory, task)

                with ScopedLogger(f"gather {task} result"):
                    results.append(future.result())
                time.sleep(2)

            self.assertEqual(results, tasks)
