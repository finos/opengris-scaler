import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility import logging_test_name

N_TASKS = 30
N_WORKERS = 3
assert N_TASKS >= N_WORKERS


class TestNestedTask(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = "tcp://127.0.0.1:23456"
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=N_WORKERS, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()

    def test_nested_task_arg_client(self) -> None:
        with Client(self.address) as client:
            result = client.submit(parent_task_arg_client, client).result()
            self.assertEqual(result, sum(nested_task(v) for v in range(0, N_TASKS)))

    def test_recursive_task(self) -> None:
        with Client(self.address) as client:
            result = client.submit(factorial, client, 10).result()
            self.assertEqual(result, 3_628_800)

    @unittest.skip("this test occasionally never finishes")
    def test_multiple_recursive_task(self) -> None:
        with Client(self.address) as client:
            result = client.submit(fibonacci, client, 8).result()
            self.assertEqual(result, 21)


def parent_task_arg_client(client: Client) -> int:
    iterable = [(v,) for v in range(0, N_TASKS)]
    return sum(client.map(nested_task, iterable))


def nested_task(value: int) -> int:
    return value**2


def factorial(client: Client, value: int) -> int:
    if value == 0:
        return 1
    else:
        return value * client.submit(factorial, client, value - 1).result()


def fibonacci(client: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        a = client.submit(fibonacci, client, n - 1)
        b = client.submit(fibonacci, client, n - 2)
        return a.result() + b.result()
