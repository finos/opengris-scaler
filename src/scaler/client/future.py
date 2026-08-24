import asyncio
import concurrent.futures
import sys
import time
from typing import Any, Callable, Optional

from scaler.client.object_buffer import ObjectBuffer
from scaler.client.serializer.mixins import Serializer
from scaler.io.mixins import SyncConnector
from scaler.protocol.capnp import Task, TaskCancel, TaskState
from scaler.utility.event_list import EventList
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.profile_result import ProfileResult


class ScalerFuture(concurrent.futures.Future):
    """
    A drop-in replacement for Python's `concurrent.futures.Future`.

    This class is designed to be compatible with Python's Future API, but with some key differences:

    - Delayed futures (`is_delayed` set to `True`) might not fetch the result data when the future is done.
      Instead, the result is lazily fetched when `result()` or `exception()` is called, or when a callback or waiter is
      added. That is, `result()` might temporarily be blocking even if `done()` is `True`.

    - `cancel()` may block until a cancellation confirmation is received from Scaler's scheduler.
    """

    def __init__(
        self,
        task: Task,
        is_delayed: bool,
        group_task_id: Optional[TaskID],
        serializer: Serializer,
        connector_agent: SyncConnector,
        object_buffer: ObjectBuffer,
    ):
        super().__init__()

        self._waiters = EventList(self._waiters)  # type: ignore[assignment]
        self._waiters.add_update_callback(self._on_waiters_updated)  # type: ignore[attr-defined]

        self._task_id: TaskID = task.taskId
        self._is_delayed: bool = is_delayed
        self._group_task_id: Optional[TaskID] = group_task_id
        self._serializer: Serializer = serializer
        self._connector_agent: SyncConnector = connector_agent
        self._object_buffer: ObjectBuffer = object_buffer

        self._result_object_id: Optional[ObjectID] = None
        self._result_received = False

        # Set as soon as the result object's fetching starts, ensuring the object is never fetched more than once.
        self._result_object_future: Optional[concurrent.futures.Future] = None

        self._task_state: Optional[TaskState] = None
        self._cancel_requested: bool = False

        self._profiling_info: Optional[ProfileResult] = None

    @property
    def task_id(self) -> TaskID:
        return self._task_id

    def profiling_info(self) -> ProfileResult:
        with self._condition:  # type: ignore[attr-defined]
            if self._profiling_info is None:
                raise ValueError(f"didn't receive profiling info for {self} yet")

            return self._profiling_info

    def set_result_ready(
        self, object_id: Optional[ObjectID], task_state: TaskState, profile_result: Optional[ProfileResult] = None
    ) -> None:
        with self._condition:  # type: ignore[attr-defined]
            if self.done():
                raise concurrent.futures.InvalidStateError(f"invalid future state: {self._state}")

            self._state = "FINISHED"

            self._result_object_id = object_id

            self._task_state = task_state

            if profile_result is not None:
                self._profiling_info = profile_result

            # if it's not delayed future, or if there is any listener (waiter or callback), get the result immediately
            if not self._is_delayed or self._has_result_listeners():
                self._start_result_object_fetch()

            self._condition.notify_all()  # type: ignore[attr-defined]

    def set_canceled(self):
        with self._condition:
            if self.done():
                return

            self._state = "CANCELLED_AND_NOTIFIED"
            self._result_received = True
            self._cancel_requested = True

            for waiter in self._waiters:
                waiter.add_cancelled(self)

            self._condition.notify_all()  # type: ignore[attr-defined]

        self._invoke_callbacks()  # type: ignore[attr-defined]

    def force_set_canceled(self):
        """Mark the future as cancelled regardless of its current state.

        Unlike `set_canceled`, this also overrides FINISHED states (set_result_ready /
        set_exception). Intended for `Client.disconnect`, where the network-driven cancel
        round-trip can race with the agent thread setting an exception/result on the same
        future; once the client is disconnecting, the original outcome is no longer reachable
        to user code, so we collapse to CANCELLED for a consistent observable state.
        """
        with self._condition:
            if self.cancelled():
                return

            self._state = "CANCELLED_AND_NOTIFIED"
            self._result_received = True
            self._cancel_requested = True

            for waiter in self._waiters:
                waiter.add_cancelled(self)

            self._condition.notify_all()  # type: ignore[attr-defined]

        self._invoke_callbacks()  # type: ignore[attr-defined]

    def _set_result_or_exception(
        self,
        result: Optional[Any] = None,
        exception: Optional[BaseException] = None,
        profiling_info: Optional[ProfileResult] = None,
    ) -> None:
        with self._condition:  # type: ignore[attr-defined]
            if self.cancelled():
                raise concurrent.futures.InvalidStateError(f"invalid future state: {self._state}")

            if self._result_received:
                raise concurrent.futures.InvalidStateError("future already received object data.")

            if profiling_info is not None:
                if self._profiling_info is not None:
                    raise concurrent.futures.InvalidStateError("cannot set profiling info twice.")

                self._profiling_info = profiling_info

            self._state = "FINISHED"
            self._result_received = True

            if exception is not None:
                assert result is None
                self._exception = exception
                for waiter in self._waiters:
                    waiter.add_exception(self)
            else:
                self._result = result
                for waiter in self._waiters:
                    waiter.add_result(self)

            self._condition.notify_all()  # type: ignore[attr-defined]

        self._invoke_callbacks()  # type: ignore[attr-defined]

    def set_result(self, result: Any, profiling_info: Optional[ProfileResult] = None) -> None:
        self._set_result_or_exception(result=result, profiling_info=profiling_info)

    def set_exception(self, exception: Optional[BaseException], profiling_info: Optional[ProfileResult] = None) -> None:
        self._set_result_or_exception(exception=exception, profiling_info=profiling_info)

    def result(self, timeout: Optional[float] = None) -> Any:
        with self._condition:  # type: ignore[attr-defined]
            self._wait_result_object(timeout)

            return super().result()

    def exception(self, timeout: Optional[float] = None) -> Optional[BaseException]:
        with self._condition:  # type: ignore[attr-defined]
            self._wait_result_object(timeout)

            return super().exception()

    def cancel(self, timeout: Optional[float] = None) -> bool:
        with self._condition:  # type: ignore[attr-defined]
            if self.cancelled():
                return True

            if self.done():
                return False

            if not self._cancel_requested:
                # Send cancellation request to the server
                cancel_flags = TaskCancel.TaskCancelFlags(force=True)

                if self._group_task_id is not None:
                    self._connector_agent.send(TaskCancel(taskId=self._group_task_id, flags=cancel_flags))
                else:
                    self._connector_agent.send(TaskCancel(taskId=self._task_id, flags=cancel_flags))

                self._cancel_requested = True

            # Wait for the answer from the server, can either be a cancel confirmation, or the results if the task
            # finished while being canceled.
            self._wait_result_ready(timeout)

        return self.cancelled()

    def __await__(self):
        """Allow ``await scaler_future`` from any asyncio context.

        ``ScalerFuture`` is a :class:`concurrent.futures.Future` subclass, so
        it is completed by the client agent (on a background thread natively,
        or on the same asyncio loop under Pyodide). ``asyncio.wrap_future``
        bridges the two worlds using thread-safe future completion, and
        degrades to the same-loop case gracefully when both sides already
        share a loop.

        This enables notebook code such as ``result = await client.submit(...)``
        to work identically in CPython and in the browser without requiring
        the sync blocking ``.result()`` path (which in the browser depends on
        JSPI).
        """
        return asyncio.wrap_future(self).__await__()

    def add_done_callback(self, fn: Callable[["ScalerFuture"], Any]) -> None:
        with self._condition:
            if self.done():
                self._start_result_object_fetch()
            else:
                self._done_callbacks.append(fn)  # type: ignore[attr-defined]
                return

        try:
            fn(self)
        except Exception:
            concurrent.futures._base.LOGGER.exception(f"exception calling callback for {self!r}")
            raise

    def _on_waiters_updated(self, waiters: EventList):
        with self._condition:  # type: ignore[attr-defined]
            # if it's delayed future, get the result when waiter gets added
            if self._is_delayed and len(self._waiters) > 0:
                self._start_result_object_fetch()

    def _has_result_listeners(self) -> bool:
        return len(self._done_callbacks) > 0 or len(self._waiters) > 0  # type: ignore[attr-defined]

    def _start_result_object_fetch(self):
        """
        Starts the fetching of the future's result object, at most once.

        As it never blocks, this can be called from the client agent's event loop.
        """

        with self._condition:  # type: ignore[attr-defined]
            if self._result_object_id is None or self.cancelled() or self._result_received:
                return

            if self._result_object_future is not None:
                return  # the object is already being fetched

            if self._task_state == TaskState.success:
                is_exception = False
            elif self._task_state == TaskState.failed:
                is_exception = True
            else:
                raise ValueError(f"unexpected task status: {self._task_state}")

            # TODO: graph task results could also be deleted if these are not required by another task of the graph.
            delete_after_fetch = self._is_simple_task()

            self._result_object_future = self._object_buffer.fetch_object(
                self._result_object_id, is_exception=is_exception, delete_after_fetch=delete_after_fetch
            )

        self._result_object_future.add_done_callback(self.__on_result_object_fetched)

    def __on_result_object_fetched(self, _: concurrent.futures.Future) -> None:
        assert self._result_object_future is not None and self._result_object_future.done()
        is_exception = self._task_state == TaskState.failed

        try:
            result_object = self._result_object_future.result()
        except Exception as exception:
            # The result object could not be fetched, e.g. the object storage server is unreachable.
            result_object = exception
            is_exception = True

        try:
            if is_exception:
                self.set_exception(result_object)
            else:
                self.set_result(result_object)
        except concurrent.futures.InvalidStateError:
            # The future got canceled while its result object was being fetched, e.g. by `Client.disconnect()`.
            pass

    def _wait_result_object(self, timeout: Optional[float] = None):
        """
        Blocks until the future's result object is fetched, starting its fetching if it did not start yet.

        While waiting, this releases the future's condition lock, letting the fetching's callback set the future's
        result.

        Raises a `TimeoutError` if it blocks more than `timeout` seconds.
        """

        assert self._condition._is_owned()  # type: ignore[attr-defined]

        deadline = None if timeout is None else time.monotonic() + timeout

        self._wait_result_ready(timeout)

        # if it's a delayed future, the result object gets fetched when result() or exception() gets called
        if self._is_delayed:
            self._start_result_object_fetch()

        if self._result_object_id is None:
            return  # umbrella graph tasks do not have a result object

        while not self._result_received and not self.cancelled():
            if deadline is None:
                remaining_seconds = None
            else:
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    raise concurrent.futures.TimeoutError()

            self._condition.wait(remaining_seconds)  # type: ignore[attr-defined]

    def _wait_result_ready(self, timeout: Optional[float] = None):
        """
        Blocks until the future is done (either successfully, or on failure/cancellation).

        Raises a `TimeoutError` if it blocks more than `timeout` seconds.
        """

        assert self._condition._is_owned()  # type: ignore[attr-defined]

        if self.done():
            return

        if sys.platform == "emscripten":
            # On Pyodide the agent task runs on the same single-threaded
            # asyncio loop as this caller. ``threading.Condition.wait`` blocks
            # the only thread, so the agent never gets a chance to run and
            # signal completion -> deadlock. Instead, suspend the wasm stack
            # via JSPI while the asyncio loop continues to drive the agent.
            from pyodide.ffi import run_sync  # type: ignore[import-not-found]

            async def _await_done() -> None:
                fut: asyncio.Future = asyncio.wrap_future(self)
                if timeout is None:
                    await fut
                else:
                    await asyncio.wait_for(fut, timeout)

            # ``self._condition`` is held by the caller; release it while we
            # suspend so the agent (running on the same loop) can acquire it
            # in ``set_result_ready`` to mark the future done.
            self._condition.release()  # type: ignore[attr-defined]
            try:
                try:
                    run_sync(_await_done())
                except asyncio.TimeoutError as exc:
                    raise concurrent.futures.TimeoutError() from exc
                except (asyncio.CancelledError, concurrent.futures.CancelledError):
                    # ``asyncio.wrap_future`` raises ``CancelledError`` once
                    # the underlying ``ScalerFuture`` transitions to
                    # cancelled. The native ``Condition.wait`` path also
                    # returns silently in that case (it is just woken up by
                    # ``notify_all``), so callers like ``cancel()`` only
                    # care that the future has settled.
                    pass
            finally:
                self._condition.acquire()  # type: ignore[attr-defined]
            return

        if not self._condition.wait(timeout):
            raise concurrent.futures.TimeoutError()

    def _is_simple_task(self):
        return self._group_task_id is None and self._task_id is not None

    def __task_type(self) -> str:
        if self._group_task_id is None:
            return "SimpleTask"

        if self._group_task_id == self._task_id:
            return "GraphUmbrellaTask"
        else:
            return "GraphSubTask"
