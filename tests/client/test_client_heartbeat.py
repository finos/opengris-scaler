import time
import unittest
from typing import Any

import cloudpickle

from scaler import Client, SchedulerClusterCombo
from scaler.client.serializer.mixins import Serializer
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

# How long the scheduler waits for a client's heartbeat before disconnecting it.
SCHEDULER_CLIENT_TIMEOUT_SECONDS = 2

# How long it takes to serialize or deserialize a slow object. Must be noticeably longer than the scheduler's client
# timeout, so that a client that (de)serializes these objects on its agent's event loop gets disconnected.
SERIALIZATION_DURATION_SECONDS = SCHEDULER_CLIENT_TIMEOUT_SECONDS + 1

CLIENT_HEARTBEAT_INTERVAL_SECONDS = 1


class SlowObject:
    """An object that takes a significant amount of time to serialize and deserialize."""


def slow_task(value: SlowObject) -> SlowObject:
    return value


class SlowSerializer(Serializer):
    """
    A serializer that takes a significant amount of time to process `SlowObject` instances.

    This emulates the (de)serialization of very large objects without requiring the memory to hold these.
    """

    @staticmethod
    def serialize(obj: Any) -> bytes:
        if isinstance(obj, SlowObject):
            SlowSerializer.busy_wait()

        return cloudpickle.dumps(obj)

    @staticmethod
    def deserialize(payload: bytes) -> Any:
        obj = cloudpickle.loads(payload)

        if isinstance(obj, SlowObject):
            SlowSerializer.busy_wait()

        return obj

    @staticmethod
    def busy_wait() -> None:
        # Busy loops instead of sleeping, as (de)serializing a large object is CPU bound.
        deadline = time.monotonic() + SERIALIZATION_DURATION_SECONDS
        while time.monotonic() < deadline:
            pass


class TestClientHeartbeat(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(
            n_workers=1, event_loop="builtin", client_timeout_seconds=SCHEDULER_CLIENT_TIMEOUT_SECONDS
        )
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    def test_client_long_blocking_io(self):
        """Serializing or receiving task objects must not prevent the client agent's event loop to send heart-beats."""

        RESULT_TIMEOUT_SECONDS = 60

        with Client(
            self.address, serializer=SlowSerializer(), heartbeat_interval_seconds=CLIENT_HEARTBEAT_INTERVAL_SECONDS
        ) as client:
            # Serializing the argument, and deserializing the result, both take longer than the scheduler's client
            # timeout.

            # Registering a callback forces the result object to be deserialized as soon as the task finishes, on the
            # agent's event loop.
            future = client.submit(slow_task, SlowObject())
            future.add_done_callback(lambda _: None)

            self.assertIsInstance(future.result(timeout=RESULT_TIMEOUT_SECONDS), SlowObject)

            # The client must still be connected to the scheduler, i.e. it kept exchanging heartbeats while fetching the
            # result object.
            self.assertEqual(client.submit(round, 3.14).result(timeout=RESULT_TIMEOUT_SECONDS), 3)


if __name__ == "__main__":
    unittest.main()
