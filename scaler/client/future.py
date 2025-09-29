import concurrent.futures
from typing import Any, Callable, Optional

from scaler.client.serializer.mixins import Serializer
from scaler.io.mixins import SyncConnector, SyncObjectStorageConnector
from scaler.protocol.python.common import TaskState
from scaler.protocol.python.message import Task, TaskCancel
from scaler.utility.event_list import EventList
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.serialization import deserialize_failure


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
        connector_storage: SyncObjectStorageConnector,
    ):
        super().__init__()

        self._waiters = EventList(self._waiters)  # type: ignore[assignment]
        self._waiters.add_update_callback(self._on_waiters_updated)  # type: ignore[attr-defined]

        self._task_id: TaskID = task.task_id
        self._is_delayed: bool = is_delayed
        self._group_task_id: Optional[TaskID] = group_task_id
        self._serializer: Serializer = serializer
        self._connector_agent: SyncConnector = connector_agent
        self._connector_storage: SyncObjectStorageConnector = connector_storage

        self._result_object_id: Optional[ObjectID] = None
        self._result_received = False
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
                self._get_result_object()

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
            self._wait_result_ready(timeout)

            # if it's delayed future, get the result when future.result() gets called
            if self._is_delayed:
                self._get_result_object()

            return super().result()

    def exception(self, timeout: Optional[float] = None) -> Optional[BaseException]:
        with self._condition:  # type: ignore[attr-defined]
            self._wait_result_ready(timeout)

            # if it's delayed future, get the result when future.exception() gets called
            if self._is_delayed:
                self._get_result_object()

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
                    self._connector_agent.send(TaskCancel.new_msg(self._group_task_id, flags=cancel_flags))
                else:
                    self._connector_agent.send(TaskCancel.new_msg(self._task_id, flags=cancel_flags))

                self._cancel_requested = True

            # Wait for the answer from the server, can either be a cancel confirmation, or the results if the task
            # finished while being canceled.
            self._wait_result_ready(timeout)

        return self.cancelled()

    def add_done_callback(self, fn: Callable[["ScalerFuture"], Any]) -> None:
        with self._condition:
            if self.done():
                self._get_result_object()
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
                self._get_result_object()

    def _has_result_listeners(self) -> bool:
        return len(self._done_callbacks) > 0 or len(self._waiters) > 0  # type: ignore[attr-defined]

    def _get_result_object(self):
        with self._condition:  # type: ignore[attr-defined]
            if self._result_object_id is None or self.cancelled() or self._result_received:
                return

            object_bytes = self._connector_storage.get_object(self._result_object_id)

            if self._is_simple_task():
                # immediately delete non graph result objects
                # TODO: graph task results could also be deleted if these are not required by another task of the graph.
                self._connector_storage.delete_object(self._result_object_id)

            if self._task_state == TaskState.Success:
                self.set_result(self._serializer.deserialize(object_bytes))
            elif self._task_state == TaskState.Failed:
                self.set_exception(deserialize_failure(object_bytes))
            else:
                raise ValueError(f"unexpected task status: {self._task_state}")

    def _wait_result_ready(self, timeout: Optional[float] = None):
        """
        Blocks until the future is done (either successfully, or on failure/cancellation).

        Raises a `TimeoutError` if it blocks more than `timeout` seconds.
        """
        if not self.done() and not self._condition.wait(timeout):
            raise TimeoutError()

    def _is_simple_task(self):
        return self._group_task_id is None and self._task_id is not None

    def __task_type(self) -> str:
        if self._group_task_id is None:
            return "SimpleTask"

        if self._group_task_id == self._task_id:
            return "GraphUmbrellaTask"
        else:
            return "GraphSubTask"
