"""Unit tests for ``ObjectBuffer`` identity-based dedup.

These tests use stub connectors so they exercise the buffering / dedup logic
without spinning up a scheduler or object-storage server.
"""

import gc
import unittest
from typing import List, Tuple

import numpy as np

from scaler.client.object_buffer import ObjectBuffer
from scaler.client.serializer.default import DefaultSerializer
from scaler.protocol.capnp import BaseMessage
from scaler.utility.identifiers import ClientID, ObjectID


class _FakeAgentConnector:
    """Minimal SyncConnector stub that records sent BaseMessages."""

    def __init__(self) -> None:
        self.sent: List[BaseMessage] = []

    def send(self, message: BaseMessage) -> None:
        self.sent.append(message)


class _FakeStorageConnector:
    """Minimal SyncObjectStorageConnector stub that records set_object calls."""

    def __init__(self) -> None:
        self.calls: List[Tuple[ObjectID, int]] = []  # (object_id, payload_size)

    def set_object(self, object_id: ObjectID, payload: bytes) -> None:
        self.calls.append((object_id, len(payload)))


def _make_buffer() -> Tuple[ObjectBuffer, _FakeAgentConnector, _FakeStorageConnector]:
    agent = _FakeAgentConnector()
    storage = _FakeStorageConnector()
    buf = ObjectBuffer(
        identity=ClientID.generate_client_id("test"),
        serializer=DefaultSerializer(),
        connector_agent=agent,  # type: ignore[arg-type]
        connector_storage=storage,  # type: ignore[arg-type]
    )
    # The constructor uploads the serializer object eagerly; clear those so the
    # tests below only see the calls they themselves trigger.
    agent.sent.clear()
    storage.calls.clear()
    return buf, agent, storage


class TestObjectBufferDedup(unittest.TestCase):
    def test_same_object_uploaded_only_once_across_submits(self) -> None:
        """Buffer + commit the same Python object N times -> 1 upload."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(10_000, dtype=np.float64)  # weakreffable, non-trivial payload

        caches = [buf.buffer_send_object(shared) for _ in range(5)]
        buf.commit_send_objects()

        # All returned caches should be the SAME entry.
        first_id = caches[0].object_id
        for c in caches[1:]:
            self.assertEqual(c.object_id, first_id)
            self.assertIs(c, caches[0])

        # Storage should have seen exactly one set_object call.
        self.assertEqual(len(storage.calls), 1)
        self.assertEqual(storage.calls[0][0], first_id)

    def test_distinct_objects_get_distinct_uploads(self) -> None:
        buf, _agent, storage = _make_buffer()

        a = np.zeros(100, dtype=np.float64)
        b = np.zeros(100, dtype=np.float64)  # equal contents but different identity

        ca = buf.buffer_send_object(a)
        cb = buf.buffer_send_object(b)
        buf.commit_send_objects()

        self.assertNotEqual(ca.object_id, cb.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_function_dedup(self) -> None:
        buf, _agent, storage = _make_buffer()

        def fn(x):  # noqa: D401
            return x

        c1 = buf.buffer_send_function(fn)
        c2 = buf.buffer_send_function(fn)
        buf.commit_send_objects()

        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_non_weakreffable_arg_falls_through_to_per_call_upload(self) -> None:
        """Built-in types like ``list`` / ``tuple`` / ``bytes`` cannot be
        weakref'd; we degrade gracefully (no dedup, no crash).  These are
        typically small and cheap to re-serialize so per-call upload is fine.
        """
        buf, _agent, storage = _make_buffer()

        shared_list = list(range(1000))

        c1 = buf.buffer_send_object(shared_list)
        c2 = buf.buffer_send_object(shared_list)
        buf.commit_send_objects()

        # Distinct uploads because lists cannot be tracked via weakref.
        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_dedup_survives_intermediate_commit(self) -> None:
        """Re-buffering the same object after a commit must not re-upload it."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(1024, dtype=np.uint8)  # weakreffable

        c1 = buf.buffer_send_object(shared)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        c2 = buf.buffer_send_object(shared)
        buf.commit_send_objects()

        self.assertEqual(c1.object_id, c2.object_id)
        # Still only one upload total.
        self.assertEqual(len(storage.calls), 1)

    def test_clear_invalidates_dedup_cache(self) -> None:
        """After clear(), the same object must be re-serialized + re-uploaded
        because the server has discarded its prior copy."""
        buf, _agent, storage = _make_buffer()

        shared = np.ones(1024, dtype=np.uint8)

        c1 = buf.buffer_send_object(shared)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        buf.clear()

        c2 = buf.buffer_send_object(shared)
        buf.commit_send_objects()

        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_id_recycled_after_gc_does_not_serve_stale_cache(self) -> None:
        """If the original object is GC'd and another object happens to land on
        the same id, the dedup lookup must miss (the weakref guard catches it).
        """
        buf, _agent, storage = _make_buffer()

        # Use a class instance which supports weakref.
        class _Box:
            def __init__(self, v):
                self.v = v

        first = _Box(b"a" * 256)
        c1 = buf.buffer_send_object(first)
        buf.commit_send_objects()
        first_id = id(first)

        del first
        gc.collect()

        # Try to place another object such that its id() collides with the freed
        # one.  Allocate until we get a match, with a small budget.
        second = None
        for _ in range(10_000):
            candidate = _Box(b"b" * 256)
            if id(candidate) == first_id:
                second = candidate
                break
        if second is None:
            self.skipTest("could not provoke id recycling on this interpreter")

        c2 = buf.buffer_send_object(second)
        buf.commit_send_objects()

        # Must NOT have returned the stale cache entry.
        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)


if __name__ == "__main__":
    unittest.main()
