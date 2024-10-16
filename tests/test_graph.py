import graphlib
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.graph.optimization import cull_graph
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port, logging_test_name


def inc(i):
    return i + 1


def add(a, b):
    return a + b


def minus(a, b):
    return a - b


class TestGraph(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=3, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()
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
        def inc_error(i):
            assert isinstance(i, int)
            time.sleep(1)
            raise ValueError("Compute Error")

        def add_sleep(a, b):
            time.sleep(5)
            return a + b

        graph = {"a": 2, "b": 2, "c": (inc_error, "a"), "d": (add_sleep, "a", "b"), "e": (minus, "d", "c")}

        with Client(self.address) as client:
            with ScopedLogger("test graph node fail"), self.assertRaises(ValueError):
                client.get(graph, ["e"])

            time.sleep(5)

        with Client(self.address) as client:
            import math

            graph["c"] = (math.sqrt, "a")

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
            time.sleep(10)
            return a

        with Client(address=self.address) as client:
            graph = {"a": 1, "b": (func, "a")}
            futures = client.get(graph, keys=["b"], block=False)

            time.sleep(4)
            futures["b"].cancel()

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
