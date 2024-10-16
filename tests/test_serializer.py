import enum
import functools
import random
import unittest
from typing import Any

import cloudpickle

from scaler import Client, SchedulerClusterCombo, Serializer
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port, logging_test_name


def noop(sec: int):
    return sec * 1


def heavy_function(sec: int, payload: bytes):
    return len(payload) * sec


class ObjectType(enum.Enum):
    Integer = b"I"
    Other = b"O"


class MySerializer(Serializer):
    @staticmethod
    def serialize(obj: Any) -> bytes:
        print(f"MySerializer.serialize({MySerializer.trim_message_internal(obj)})")
        if isinstance(obj, int):
            return ObjectType.Integer.value + str(obj).encode()
        return ObjectType.Other.value + cloudpickle.dumps(obj)

    @staticmethod
    def deserialize(payload: bytes) -> Any:
        print(f"MySerializer.deserialize({MySerializer.trim_message_internal(payload)})")
        if payload[0] == ObjectType.Integer.value[0]:
            return int(payload[1:])
        assert payload[0] == ObjectType.Other.value[0]
        return cloudpickle.loads(payload[1:])

    @staticmethod
    def trim_message_internal(message: Any) -> str:
        msg = str(message)
        if len(msg) > 30:
            return f"{msg[:30]}..."
        return msg


class TestSerializer(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._workers = 3
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()
        pass

    def test_one_task(self):
        client = Client(self.address, serializer=MySerializer())

        with ScopedLogger("submitting task"):
            future = client.submit(noop, 1)

        with ScopedLogger("gathering task"):
            result = future.result()

        self.assertEqual(result, 1)
        print("done test_one_task")

    def test_heavy_function(self):
        with Client(self.address, serializer=MySerializer()) as client:
            size = 500_000_000
            tasks = [random.randint(0, 100) for _ in range(10000)]
            function = functools.partial(heavy_function, payload=b"1" * size)
            with ScopedLogger(f"submit {len(tasks)} heavy function (500mb) tasks"):
                results = client.map(function, [(i,) for i in tasks])

            expected = [task * size for task in tasks]
            self.assertEqual(results, expected)
