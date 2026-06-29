"""Regression tests for the worker's top-level YMQ error handling (``Worker.__get_loops``).

The ``jne-fix-ymq`` failure: a worker's internal YMQ binder is shut down (``disconnect``/teardown)
while a ``binder.send`` driven by one of the worker's loops is still in flight. The send surfaces
``SocketStopRequested`` (see ``tests/io/test_ymq_async_binder.py``), which propagates through
``asyncio.gather`` into ``Worker.__get_loops``. Before the fix the handler logged it as a "failed
with unhandled exception" crash; it must instead be treated like ``ConnectorSocketClosedByRemoteEnd``
- an expected teardown condition that is logged, never surfaced as a crash.
"""

import asyncio
import unittest
from typing import Any, Awaitable, Callable
from unittest import mock

import scaler.worker.worker as worker_module
from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.ymq import SocketStopRequestedError, SysCallError
from scaler.worker.worker import Worker


async def _hang() -> None:
    await asyncio.Event().wait()


class _StubCollaborator:
    """Minimal stand-in for a worker collaborator, exposing the hooks ``__get_loops`` touches."""

    def __init__(self, routine_behavior: Callable[[], Awaitable[None]] = _hang) -> None:
        self._routine_behavior = routine_behavior
        self.destroyed = False

    async def routine(self) -> None:
        await self._routine_behavior()

    async def connect(self, *args: object, **kwargs: object) -> None:
        return None

    async def bind(self, *args: object, **kwargs: object) -> None:
        return None

    async def initialize(self, *args: object, **kwargs: object) -> None:
        await _hang()

    def destroy(self, *args: object) -> None:
        self.destroyed = True


class WorkerTeardownYMQErrorTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _build_worker(task_routine_error: ymq.YMQException) -> Any:
        # Typed as Any: the test deliberately injects duck-typed stubs into the worker's typed
        # collaborator slots and reaches a name-mangled private loop, which the type system rejects.
        worker: Any = Worker(
            event_loop="builtin",
            name="test-worker",
            address=AddressConfig.from_string("tcp://127.0.0.1:2345"),
            object_storage_address=None,
            preload=None,
            capabilities={},
            io_threads=1,
            task_queue_size=10,
            heartbeat_interval_seconds=1,
            garbage_collect_interval_seconds=1,
            trim_memory_threshold_bytes=0,
            task_timeout_seconds=10,
            death_timeout_seconds=10,
            hard_processor_suspend=False,
            logging_paths=(),
            logging_level="INFO",
            worker_manager_id=b"wm",
        )

        worker._backend = None  # not a ZMQ backend -> no graceful-shutdown handshake on teardown
        worker._address_internal = AddressConfig.from_string("tcp://127.0.0.1:2346")  # tcp -> no ipc unlink

        async def _raise() -> None:
            raise task_routine_error

        worker._connector_external = _StubCollaborator()
        worker._connector_storage = _StubCollaborator()
        worker._binder_internal = _StubCollaborator()
        worker._heartbeat_manager = _StubCollaborator()
        worker._timeout_manager = _StubCollaborator()
        worker._profiling_manager = _StubCollaborator()
        worker._processor_manager = _StubCollaborator()
        # The task loop is what drives processor_manager.on_task -> binder_internal.send; model that
        # send raising the YMQ error by having the task routine raise it directly into the gather.
        worker._task_manager = _StubCollaborator(_raise)

        return worker

    async def _drain_pending_tasks(self) -> None:
        # The sibling loop coroutines created by __get_loops's gather keep running after the gather
        # propagates the first exception; cancel them so the test loop tears down cleanly.
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _run_get_loops(self, task_routine_error: ymq.YMQException) -> mock.MagicMock:
        worker = self._build_worker(task_routine_error)
        with mock.patch.object(worker_module, "logger") as mock_logger:
            await worker._Worker__get_loops()  # name-mangled private method
        await self._drain_pending_tasks()

        # Regardless of the error, the worker must always reach clean teardown.
        self.assertTrue(worker._binder_internal.destroyed, "worker did not reach teardown / destroy")
        return mock_logger

    async def test_socket_stop_requested_during_teardown_is_logged_not_crashed(self) -> None:
        error = SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "binder socket shut down mid-send")
        mock_logger = await self._run_get_loops(error)

        unhandled = [c for c in mock_logger.exception.call_args_list if "failed with unhandled exception" in str(c)]
        self.assertEqual(unhandled, [], "SocketStopRequested surfaced as an unhandled-exception crash")

        handled = [c for c in mock_logger.info.call_args_list if "shut down during teardown" in str(c)]
        self.assertTrue(handled, "SocketStopRequested was not logged as an expected teardown condition")

    async def test_unexpected_ymq_error_still_surfaces_as_unhandled(self) -> None:
        # A YMQ error that is NOT an expected teardown condition must still be logged loudly, so real
        # bugs continue to fail fast during development rather than being blanket-swallowed.
        error = SysCallError(ymq.ErrorCode.SysCallError, "something genuinely broke")
        mock_logger = await self._run_get_loops(error)

        unhandled = [c for c in mock_logger.exception.call_args_list if "failed with unhandled exception" in str(c)]
        self.assertTrue(unhandled, "an unexpected YMQ error should still be logged as an unhandled exception")


if __name__ == "__main__":
    unittest.main()
