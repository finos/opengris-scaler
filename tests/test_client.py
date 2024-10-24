import functools
import os
import random
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.exceptions import ProcessorDiedError
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port, logging_test_name


def noop(sec: int):
    return sec * 1


def noop_sleep(sec: int):
    time.sleep(sec)
    return sec


def heavy_function(sec: int, payload: bytes):
    return len(payload) * sec


def raise_exception(foo: int):
    if foo == 11:
        raise ValueError("foo cannot be 100")


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._workers = 3
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()
        pass

    def test_one_submit(self):
        with Client(self.address) as client:
            with ScopedLogger("submitting 1 task"):
                future1 = client.submit(noop, 1)

            self.assertEqual(future1.result(), 1)

    def test_one_map(self):
        with Client(self.address) as client:
            with ScopedLogger("mapping 1 task"):
                result = client.map(noop, [(1,), (2,)])

            self.assertEqual(result, [1, 2])

    def test_submit_and_map(self):
        with Client(self.address) as client:
            tasks = [random.randint(0, 100) for _ in range(100)]
            with ScopedLogger("submitting task use map"):
                map_results = client.map(noop, [(arg,) for arg in tasks])

            self.assertEqual(map_results, tasks)

            with ScopedLogger("submitting task use submit"):
                futures = [client.submit(noop, i) for i in tasks]
                submit_results = [future.result() for future in futures]

            self.assertEqual(submit_results, tasks)

    def test_noop_submit(self):
        with Client(self.address) as client:
            tasks = [random.randint(0, 100) for _ in range(10000)]
            with ScopedLogger(f"submit {len(tasks)} noop tasks"):
                futures = [client.submit(noop, i) for i in tasks]

            with ScopedLogger(f"gather {len(futures)} results"):
                results = [future.result() for future in futures]

            self.assertEqual(results, tasks)

    def test_noop_map(self):
        with Client(self.address) as client:
            tasks = [random.randint(0, 100) for _ in range(10000)]
            with ScopedLogger(f"submit {len(tasks)} noop tasks"):
                results = client.map(noop, [(i,) for i in tasks])

            self.assertEqual(results, tasks)

    def test_noop_cancel(self):
        with Client(self.address) as client:
            tasks = [10, 1, 1] * 10
            with ScopedLogger(f"submit {len(tasks)} noop and cancel tasks"):
                futures = [client.submit(noop_sleep, i) for i in tasks]
                assert isinstance(futures, list)

                for future in futures:
                    future.cancel()

                time.sleep(3)

        time.sleep(1)

    def test_heavy_function(self):
        with Client(self.address) as client:
            size = 500_000_000
            number_of_tasks = 10000
            tasks = [random.randint(0, 100) for _ in range(number_of_tasks)]
            function = functools.partial(heavy_function, payload=b"1" * size)

            with ScopedLogger(f"submit {len(tasks)} heavy function (500mb) for {number_of_tasks} tasks"):
                results = client.map(function, [(i,) for i in tasks])

            expected = [task * size for task in tasks]
            self.assertEqual(results, expected)

    def test_very_large_payload(self):
        def func(data: bytes):
            return data

        with Client(self.address) as client:
            payload = os.urandom(2**29 + 300)  # 512MB + 300B
            future = client.submit(func, payload)

            result = future.result()

            self.assertTrue(payload == result)

    def test_sleep(self):
        with Client(self.address) as client:
            time.sleep(5)

            tasks = [10, 1, 1] * 10
            # tasks = [10] * 10
            with ScopedLogger(f"submit {len(tasks)} sleep and balance tasks"):
                futures = [client.submit(noop_sleep, i) for i in tasks]

            # time.sleep(60)
            # print(f"number of futures: {len(futures)}")
            # print(f"number of states: {Counter([future._state for future in futures])}")
            with ScopedLogger(f"gather {len(futures)} results"):
                results = [future.result() for future in futures]

            self.assertEqual(results, tasks)

    def test_raise_exception(self):
        with Client(self.address) as client:
            tasks = [i for i in range(100)]
            with ScopedLogger(f"submit {len(tasks)} tasks, raise 1 of the tasks"):
                futures = [client.submit(raise_exception, i) for i in tasks]

            with self.assertRaises(ValueError), ScopedLogger(f"gather {len(futures)} results"):
                _ = [future.result() for future in futures]

    def test_function(self):
        def func_args(a: int, b: int, c: int, d: int = 0):
            return a, b, c, d

        def func_args2(a: int, b: int, *, c: int, d: int = 0):
            return a, b, c, d

        with Client(self.address) as client:
            with ScopedLogger("test mix of positional and keyword arguments and with some arguments default value"):
                self.assertEqual(client.submit(func_args, 1, c=4, b=2).result(), (1, 2, 4, 0))

            with ScopedLogger("test all keyword arguments"):
                self.assertEqual(client.submit(func_args, d=5, b=3, c=1, a=4).result(), (4, 3, 1, 5))

            with ScopedLogger("test mix of positional and keyword arguments with override default value"):
                self.assertEqual(client.submit(func_args, 1, c=4, b=2, d=6).result(), (1, 2, 4, 6))

            with ScopedLogger("test partial function"):
                self.assertEqual(client.submit(functools.partial(func_args, 5, 6), 1, 2).result(), (5, 6, 1, 2))

            with ScopedLogger("test insufficient arguments"), self.assertRaises(TypeError):
                client.submit(func_args, 1)

            with ScopedLogger("test not allow keyword only arguments even assigned"), self.assertRaises(TypeError):
                client.submit(func_args2, 1, c=4, b=2, d=6).result()

            with ScopedLogger("test not allow keyword only arguments"), self.assertRaises(TypeError):
                client.submit(func_args2, a=3, b=4).result()

    def test_map(self):
        def func(x, y):
            return x * y

        with Client(self.address) as client:
            result = client.map(func, [(1, 2), (3, 4), (5, 6), (7, 8)])
            self.assertEqual(result, [2, 12, 30, 56])

    def test_more_tasks(self):
        def func(a):
            time.sleep(random.randint(10, 50))
            return a * 2

        with Client(self.address) as client:
            client.map(func, [(i,) for i in range(self._workers * 2)])

    def test_context_manager(self):
        with Client(self.address) as client:
            self.assertEqual(client.submit(noop, 1).result(), 1)

    def test_no_disconnect(self):
        with Client(self.address) as client:
            self.assertEqual(client.submit(noop, 1).result(), 1)

    def test_extra_disconnects(self):
        with Client(self.address) as client:
            self.assertEqual(client.submit(noop, 1).result(), 1)
            client.disconnect()
            client.disconnect()
            client.disconnect()

    def test_processor_died(self):
        def func():
            time.sleep(1)
            os._exit(1)  # noqa
            return 1

        with Client(self.address) as client:
            with self.assertRaises(ProcessorDiedError):
                client.submit(func).result()

    def test_non_hashable_client(self):
        def func(a):
            return a * 2

        with Client(self.address) as client:
            client.submit(func, [1, 2, 3, 4, 5])

    def test_send_object(self):
        def func(a):
            return len(a)

        with Client(self.address) as client:
            ref1 = client.send_object("abcdef")
            self.assertEqual(client.submit(func, ref1).result(), 6)

            ref2 = client.send_object("123456789")
            self.assertEqual(client.map(func, [(ref1,), (ref2,)]), [6, 9])

    def test_send_object2(self):
        def add(a, b):
            return a + b

        with Client(address=self.address) as client:
            ref = client.send_object([1, 2, 3, 4, 5], name="large_object")

            fut = client.submit(add, ref, [6])
            self.assertEqual(fut.result(), [1, 2, 3, 4, 5, 6])

    def test_scheduler_crash(self):
        CLIENT_TIMEOUT_SECONDS = 5

        with Client(address=self.address, timeout_seconds=CLIENT_TIMEOUT_SECONDS) as client:
            future = client.submit(noop, 10)

            self.cluster._scheduler.kill()

            time.sleep(5)

            with self.assertRaises(TimeoutError):
                future.result()

    def test_responsiveness(self):
        MAX_DELAY_SECONDS = 0.25

        # Makes sure the cluster has the time to start up.
        with Client(self.address) as client:
            client.submit(pow, 1, 1).result()

        try:
            connect_start_time = time.time()
            client = Client(self.address)
            self.assertLess(time.time() - connect_start_time, MAX_DELAY_SECONDS)

            submit_start_time = time.time()
            future = client.submit(pow, 2, 3)
            self.assertLess(time.time() - submit_start_time, MAX_DELAY_SECONDS)

            result_start_time = time.time()
            self.assertEqual(future.result(), 8)
            self.assertLess(time.time() - result_start_time, MAX_DELAY_SECONDS)
        finally:
            disconnect_start_time = time.time()
            client.disconnect()
            self.assertLess(time.time() - disconnect_start_time, MAX_DELAY_SECONDS)
