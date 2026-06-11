import unittest
from typing import Any
from unittest.mock import Mock

from scaler.client.serializer.mixins import Serializer
from scaler.utility.exceptions import DeserializeObjectError
from scaler.utility.identifiers import ClientID, ObjectID
from scaler.worker.agent.processor.object_cache import ObjectCache


def _make_cache() -> ObjectCache:
    # We construct the cache but never start the thread; ``add_object`` and
    # ``get_object`` do not require ``run()``.
    return ObjectCache(garbage_collect_interval_seconds=60, trim_memory_threshold_bytes=10**9)


def _make_object_id() -> ObjectID:
    return ObjectID(b"\x00" * 16 + b"\x01" * 16)


class ObjectCacheDeserializeFailureTest(unittest.TestCase):
    """Regression test for the ``UnboundLocalError`` that crashed the
    processor when ``deserialize`` raised (e.g. worker missing ``numpy``
    while the client sends a numpy array). ``add_object`` must record the
    failure and ``get_object`` must surface it as ``DeserializeObjectError``
    rather than allowing the processor to crash with an unbound local."""

    def setUp(self) -> None:
        self.client = ClientID(b"Client|test")
        self.object_id = _make_object_id()
        self.cache = _make_cache()

        self.serializer: Any = Mock(spec=Serializer)
        self.cache.add_serializer(self.client, self.serializer)

    def test_add_object_does_not_raise_when_deserialize_fails(self) -> None:
        self.serializer.deserialize.side_effect = ModuleNotFoundError("No module named 'numpy'")

        # Must not raise (and crucially must not raise UnboundLocalError).
        self.cache.add_object(self.client, self.object_id, b"payload-bytes")

        self.assertTrue(self.cache.has_object(self.object_id))

    def test_get_object_after_failed_deserialize_raises_deserialize_error(self) -> None:
        self.serializer.deserialize.side_effect = ModuleNotFoundError("No module named 'numpy'")
        self.cache.add_object(self.client, self.object_id, b"payload-bytes")

        with self.assertRaises(DeserializeObjectError) as ctx:
            self.cache.get_object(self.object_id)
        self.assertIn("ModuleNotFoundError", str(ctx.exception))
        self.assertIn("numpy", str(ctx.exception))

    def test_get_object_after_successful_deserialize_returns_value(self) -> None:
        self.serializer.deserialize.return_value = {"hello": "world"}
        self.cache.add_object(self.client, self.object_id, b"payload-bytes")

        self.assertEqual(self.cache.get_object(self.object_id), {"hello": "world"})


if __name__ == "__main__":
    unittest.main()
