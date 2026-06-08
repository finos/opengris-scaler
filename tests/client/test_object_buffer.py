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
        """Non-weakreffable args (list / dict / tuple) dedup within a batch too,
        and reset across a commit like everything else."""
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

    def test_dedup_survives_commit(self) -> None:
        """A weakref-able object reused across separate commit cycles is uploaded
        only once: the persistent identity cache is not dropped on commit."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(1024, dtype=np.uint8)

        c1 = buf.buffer_send_object(shared)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        c2 = buf.buffer_send_object(shared)
        buf.commit_send_objects()

        # Same cached upload reused across the intervening commit.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_mutation_without_reserialize_serves_cached_snapshot(self) -> None:
        """By default the persistent cache is reused across commits, so mutating
        an object in place and re-buffering it returns the cached pre-mutation
        snapshot. Callers who mutate must pass reserialize=True (see below)."""
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

        # Cache hit: same object_id, no second upload, stale snapshot served.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_reserialize_reuploads_mutated_object_and_refreshes_cache(self) -> None:
        """reserialize=True re-serializes a mutated object, uploads the new
        contents, and refreshes the persistent cache so a later default call
        reuses the new snapshot (not the original)."""
        buf, _agent, storage = _make_buffer()

        class _Box:
            def __init__(self, v):
                self.v = v

        obj = _Box([1, 2, 3])
        c1 = buf.buffer_send_object(obj)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        obj.v.append(4)  # in-place mutation after the upload
        c2 = buf.buffer_send_object(obj, reserialize=True)
        buf.commit_send_objects()

        # Re-uploaded with the mutated contents.
        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertNotEqual(c1.object_payload, c2.object_payload)
        self.assertEqual(len(storage.calls), 2)

        # The cache now holds the refreshed snapshot: a later default call hits
        # c2, not c1, and does not re-upload.
        c3 = buf.buffer_send_object(obj)
        buf.commit_send_objects()
        self.assertEqual(c3.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_reserialize_dedups_repeats_within_one_cycle(self) -> None:
        """A shared object passed many times in one reserialize call is refreshed
        once, then deduped within that commit cycle -- not re-uploaded per task."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(10_000, dtype=np.float64)

        # Prime the persistent cache in an earlier cycle.
        buf.buffer_send_object(shared)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        # Now reserialize the same object across five tasks in one cycle.
        caches = [buf.buffer_send_object(shared, reserialize=True) for _ in range(5)]
        buf.commit_send_objects()

        # Exactly one extra upload (the refresh), all five share it.
        first_id = caches[0].object_id
        for cache in caches[1:]:
            self.assertEqual(cache.object_id, first_id)
        self.assertEqual(len(storage.calls), 2)

    def test_send_object_path_does_not_dedup(self) -> None:
        """The send_object() path (dedup=False) never dedups, even within a
        batch -- it returns to the user before committing."""
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
        """An object GC'd after its commit, whose id is later reused by a
        different object, must miss: the persistent cache survives the commit,
        but the parallel weakref guard detects that id() now names a different
        object and drops the stale entry."""
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

    def test_weakref_guard_misses_when_alive_entry_lost(self) -> None:
        """Deterministic counterpart to the id-recycling test: when the parallel
        weakref no longer maps id(obj) to the buffered object (its original
        referent was collected), the persistent entry is treated as stale and
        the object is re-uploaded, even though id(obj) still matches a live
        _dedup_cache key."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(1024, dtype=np.uint8)

        c1 = buf.buffer_send_object(shared)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        # Simulate the original referent being garbage collected while id(shared)
        # survives in _dedup_cache: drop only the weakref guard. The cycle cache
        # was already cleared by the commit above.
        buf._dedup_alive.clear()

        c2 = buf.buffer_send_object(shared)
        buf.commit_send_objects()

        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)


if __name__ == "__main__":
    unittest.main()
