"""Tests for ``ScalerFuture.__await__`` — the async-await entrypoint used by
browser notebooks that prefer ``await client.submit(...)`` over the
synchronous ``.result()`` path.

These tests do not spin up a scheduler; they construct a ``ScalerFuture``
with mocked connectors and drive it via ``set_result`` / ``set_exception`` to
exercise only the await bridge.
"""

import asyncio
import unittest
from unittest.mock import Mock

from scaler.client.future import ScalerFuture
from scaler.client.serializer.default import DefaultSerializer
from scaler.io.mixins import SyncConnector, SyncObjectStorageConnector
from scaler.protocol.capnp import Task
from scaler.utility.identifiers import ClientID, ObjectID, TaskID


def _make_future(is_delayed: bool = False) -> ScalerFuture:
    client_id = ClientID.generate_client_id()
    task = Task(
        taskId=TaskID.generate_task_id(),
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities={},
    )
    fut = ScalerFuture(
        task=task,
        is_delayed=is_delayed,
        group_task_id=None,
        serializer=DefaultSerializer(),
        connector_agent=Mock(spec=SyncConnector),
        connector_storage=Mock(spec=SyncObjectStorageConnector),
    )
    fut.set_running_or_notify_cancel()
    return fut


class ScalerFutureAwaitTest(unittest.IsolatedAsyncioTestCase):
    async def test_await_returns_result_set_before_await(self) -> None:
        fut = _make_future()
        fut.set_result(42)
        got = await fut
        self.assertEqual(got, 42)

    async def test_await_returns_result_set_during_await(self) -> None:
        fut = _make_future()
        loop = asyncio.get_event_loop()
        # Schedule a late result set on the same loop.
        loop.call_later(0.01, fut.set_result, "hello")
        got = await fut
        self.assertEqual(got, "hello")

    async def test_await_propagates_exception(self) -> None:
        fut = _make_future()
        fut.set_exception(ValueError("boom"))
        with self.assertRaises(ValueError):
            await fut

    async def test_await_after_cancel_raises(self) -> None:
        fut = _make_future()
        fut.set_canceled()
        with self.assertRaises(asyncio.CancelledError):
            await fut


class ScalerFutureSyncPathUnaffectedTest(unittest.TestCase):
    """``__await__`` must not break the synchronous ``.result()`` path that
    native CPython clients rely on."""

    def test_result_still_works(self) -> None:
        fut = _make_future()
        fut.set_result(123)
        self.assertEqual(fut.result(timeout=1), 123)

    def test_exception_still_works(self) -> None:
        fut = _make_future()
        fut.set_exception(RuntimeError("x"))
        with self.assertRaises(RuntimeError):
            fut.result(timeout=1)


if __name__ == "__main__":
    unittest.main()
