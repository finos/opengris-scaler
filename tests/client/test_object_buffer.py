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
    def test_same_object_uploaded_only_once_within_batch(self) -> None:
        """Buffer the same Python object N times in one cycle, commit once -> 1 upload."""
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

    def test_non_weakreffable_arg_deduped_within_batch(self) -> None:
        """Dedup keys on ``id(obj)`` with a plain dict, so non-weakreffable
        built-ins (``list`` / ``dict`` / ``tuple`` / ...) dedup within a batch
        too -- they're kept alive by the caller for the whole burst, so id reuse
        cannot happen.  Across a commit, dedup resets like everything else.
        """
        buf, _agent, storage = _make_buffer()

        shared_list = list(range(1000))

        c1 = buf.buffer_send_object(shared_list)
        c2 = buf.buffer_send_object(shared_list)
        buf.commit_send_objects()

        # One upload within the batch.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

        # After the commit the cache is dropped, so the next buffer re-uploads.
        c3 = buf.buffer_send_object(shared_list)
        buf.commit_send_objects()
        self.assertNotEqual(c1.object_id, c3.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_dedup_does_not_survive_commit(self) -> None:
        """Dedup is scoped to a single commit cycle: re-buffering the same
        object after a commit re-serializes and re-uploads it.  This is what
        keeps the cache from ever serving a snapshot taken before user code
        (which could have mutated the object) ran -- see
        ``test_mutation_after_commit_is_resent``.
        """
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(1024, dtype=np.uint8)

        c1 = buf.buffer_send_object(shared)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        c2 = buf.buffer_send_object(shared)
        buf.commit_send_objects()

        # Distinct upload after the intervening commit.
        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_mutation_after_commit_is_resent(self) -> None:
        """The fix for the in-place-mutation footgun: mutating an object after
        it has been committed and re-buffering it must upload the NEW contents,
        not a stale snapshot.  (On the un-scoped cache this returned the old
        object_id with the old bytes.)
        """
        buf, _agent, storage = _make_buffer()

        class _Box:
            def __init__(self, v):
                self.v = v

        obj = _Box([1, 2, 3])
        c1 = buf.buffer_send_object(obj)
        buf.commit_send_objects()

        obj.v.append(4)  # in-place mutation after the upload
        c2 = buf.buffer_send_object(obj)
        buf.commit_send_objects()

        self.assertNotEqual(c1.object_id, c2.object_id)
        # The second payload reflects the mutation (it was re-serialized).
        self.assertNotEqual(c1.object_payload, c2.object_payload)
        self.assertEqual(len(storage.calls), 2)

    def test_send_object_path_does_not_dedup(self) -> None:
        """The standalone send_object() path (dedup=False) never dedups, even
        within a batch -- it buffers without committing and returns control to
        the user, so a lingering entry could serve a stale snapshot to a later
        submit().  The user reuses the upload via the returned ObjectReference.
        """
        buf, _agent, storage = _make_buffer()

        class _Box:
            def __init__(self, v):
                self.v = v

        obj = _Box(b"data")
        c1 = buf.buffer_send_object(obj, dedup=False)
        c2 = buf.buffer_send_object(obj, dedup=False)
        buf.commit_send_objects()

        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

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
        """If the original object is GC'd after its commit and another object
        later lands on the same id, the dedup lookup must miss.  The commit
        already dropped the cache, so the recycled id finds nothing.
        """
        buf, _agent, storage = _make_buffer()

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
