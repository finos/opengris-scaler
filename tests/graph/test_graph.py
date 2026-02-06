import graphlib
import logging
import math
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.graph.optimization import cull_graph
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name


def inc(i):
    return i + 1


def add(a, b):
    return a + b


def minus(a, b):
    return a - b


def inc_error(i):
    assert isinstance(i, int)
    time.sleep(1)
    raise ValueError("Compute Error")


def add_sleep(a, b):
    time.sleep(5)
    return a + b


class TestGraph(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(n_workers=3, event_loop="builtin")
        self.address = self.combo.get_address()
        # self.address = "tcp://127.0.0.1:2345"

    def tearDown(self) -> None:
        self.combo.shutdown()
        pass

    def test_graph(self):
        graph = {"a": 2, "b": 2, "c": (inc, "a"), "d": (add, "a", "b"), "e": (minus, "d", "c")}

        with Client(self.address) as client:
            with ScopedLogger("test normal graph"):
                result = client.get(graph, ["e"])
                self.assertEqual(result, {"e": 1})

            with self.assertRaises(graphlib.CycleError):
                client.get({"b": (inc, "c"), "c": (inc, "b")}, ["b", "c"])

    def test_graph_fail(self):
        # fmt: off
        graph = {
            "a": 2,
            "b": 2,
            "c": (inc_error, "a"),
            "d": (add_sleep, "a", "b"),
            "e": (minus, "d", "c")}
        # fmt: on

        with Client(self.address) as client:
            with ScopedLogger("test graph node fail"), self.assertRaises(ValueError):
                client.get(graph, ["e"])

    def test_graph_fail_2(self):
        # fmt: off
        graph = {
            "a": 2,
            "b": 2,
            "c": (math.sqrt, "a"),  # c = sqft(a)
            "d": (add_sleep, "a", "b"),  # d = a + b (sleep for 5 seconds)
            "e": (minus, "d", "c")}  # e = d - c
        # fmt: on

        with Client(self.address) as client:
            with ScopedLogger("test graph node should restore from failure"):
                futures = client.get(graph, ["e"], block=False)
                futures["e"].result(timeout=15.0)

    def test_graph_return_order(self):
        graph = {
            "a": 2,
            "b": 2,
            "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
            "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
            "e": (minus, "d", "c"),  # e = d - c = 4 - 3 = 1
        }

        with Client(address=self.address) as client:
            with self.assertRaises(KeyError):
                client.get(graph, keys=["e", "d", "c", "c"])

            with ScopedLogger("test graph return order"):
                results = client.get(graph, keys=["e", "d", "c"])
                self.assertEqual(results, {"e": 1, "d": 4, "c": 3})

    def test_get_data_key(self):
        def func(a):
            return a

        # If we don't sleep, then cluster will shutdown before OSS address get exchanged
        # between scheduler and cluster. Thus, cluster will wait OSS address for a long time.
        time.sleep(2)
        with Client(self.address) as client:
            result = client.get({"a": (func, "b"), "b": [1]}, keys=["b"])
            self.assertEqual(result["b"], [1])

    def test_reference_data_key(self):
        with Client(address=self.address) as client:
            obj_ref = client.send_object(5, name="foobar")

            graph = {
                "a": 2,
                "b": obj_ref,
                "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
                "d": (add, "a", "b"),  # d = a + b = 2 + 5 = 7
                "e": (minus, "d", "c"),  # e = d - c = 7 - 3 = 4
            }

            with ScopedLogger("test reference data key"):
                results = client.get(graph, keys=["e", "d", "c"])
                self.assertEqual(results, {"e": 4, "d": 7, "c": 3})

    def test_none_value(self):
        def func(a, optional_param_can_be_none):
            return a

        with Client(address=self.address) as client:
            graph = {"None": None, "a": 1, "b": (func, "a", "None")}

            with ScopedLogger("test None value in graph"):
                results = client.get(graph, keys=["b"])
                self.assertEqual(results, {"b": 1})

    def test_cancel(self):
        def func(a):
            time.sleep(5)
            return a

        with Client(address=self.address) as client:
            graph = {"a": 1, "b": (func, "a")}
            futures = client.get(graph, keys=["b"], block=False)

            time.sleep(1)
            futures["b"].cancel()

    def test_cancel_unassigned(self):
        # Cancels a graph task that hasn't been assigned to a worker yet.
        combo = SchedulerClusterCombo(n_workers=0, event_loop="builtin")

        graph = {
            "a": 3.14,
            "b": (round, "a"),
            "c": (minus, "a", "b")
        }

        with Client(address=combo.get_address()) as client:
            future = client.get(graph, ["c"], block=False)["c"]

            time.sleep(0.15)

            future.cancel()

        combo.shutdown()

    def test_client_quit(self):
        def func(a):
            time.sleep(10)
            return a

        with Client(address=self.address) as client:
            graph = {"a": 1, "b": (func, "a")}
            futures = client.get(graph, keys=["b"], block=False)

            time.sleep(4)

        self.assertTrue(all(f.cancelled() for f in futures.values()))

    def test_cull_graph(self):
        graph = {
            "a": (lambda *_: None,),
            "b": (lambda *_: None, "a"),
            "c": (lambda *_: None, "a"),
            "d": (lambda *_: None, "b"),
            "e": (lambda *_: None, "b", "c"),
            "f": (lambda *_: None, "c"),
        }

        def filter_keys(_graph, keys):
            return {key: value for key, value in _graph.items() if key in keys}

        self.assertEqual(cull_graph(graph, ["d"]), filter_keys(graph, ["a", "b", "d"]))
        self.assertEqual(cull_graph(graph, ["e"]), filter_keys(graph, ["a", "b", "c", "e"]))
        self.assertEqual(cull_graph(graph, ["f"]), filter_keys(graph, ["a", "c", "f"]))
        self.assertEqual(cull_graph(graph, ["d", "e", "f"]), graph)

    def test_graph_error(self):
        def raise_exception(*args):
            raise ValueError("some error")

        graph = {
            "a": (lambda *_: None,),
            "b": (lambda *_: time.sleep(1), "a"),
            "c": (lambda *_: time.sleep(2), "a"),
            "d": (lambda *_: time.sleep(3), "a"),
            "e": (raise_exception, "b", "c"),
            "f": (lambda *_: time.sleep(2), "e", "d"),
            "g": (lambda *_: time.sleep(1), "f"),
        }

        with Client(address=self.address) as client:
            futures = client.get(graph, keys=["e", "f", "g"], block=False)

            time.sleep(4)
            for k, fut in futures.items():
                with self.assertRaises(ValueError):
                    fut.result()
                logging.info(f"Raised ValueError exception for {k}")
