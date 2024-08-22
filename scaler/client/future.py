import threading
from concurrent.futures import Future, InvalidStateError
from typing import Any, Callable, Optional

from scaler.io.sync_connector import SyncConnector
from scaler.protocol.python.message import ObjectRequest, Task, TaskCancel
from scaler.utility.event_list import EventList
from scaler.utility.metadata.profile_result import ProfileResult


class ScalerFuture(Future):
    def __init__(self, task: Task, is_delayed: bool, group_task_id: Optional[bytes], connector: SyncConnector):
        super().__init__()

        self._waiters = EventList(self._waiters)  # type: ignore[assignment]
        self._waiters.add_update_callback(self._on_waiters_updated)  # type: ignore[attr-defined]

        self._task_id: bytes = task.task_id
        self._is_delayed: bool = is_delayed
        self._group_task_id: Optional[bytes] = group_task_id
        self._connector: SyncConnector = connector

        self._result_object_id: Optional[bytes] = None
        self._result_ready_event = threading.Event()
        self._result_request_sent = False

        self._profiling_info: Optional[ProfileResult] = None

    @property
    def task_id(self):
        return self._task_id

    def profiling_info(self) -> ProfileResult:
        with self._condition:  # type: ignore[attr-defined]
            if self._profiling_info is None:
                raise ValueError(f"didn't receive profiling info for {self} yet")

            return self._profiling_info

    def set_result_ready(self, object_id: Optional[bytes], profile_result: Optional[ProfileResult] = None) -> None:
        with self._condition:  # type: ignore[attr-defined]
            if self.done():
                raise InvalidStateError(f"invalid future state: {self._state}")

            if object_id is not None:
                self._result_object_id = object_id

            if profile_result is not None:
                self._profiling_info = profile_result

            # if it's not delayed future, or if there is any listener (waiter or callback), get the result immediately
            if not self._is_delayed or self._has_result_listeners():
                self._request_result_object()

            self._result_ready_event.set()

    def set_exception(self, exception: Optional[BaseException], profile_result: Optional[ProfileResult] = None) -> None:
        with self._condition:  # type: ignore[attr-defined]
            if profile_result is not None:
                self._profiling_info = profile_result

            self._result_ready_event.set()

            return super().set_exception(exception)

    def result(self, timeout=None):
        self._result_ready_event.wait(timeout)

        with self._condition:  # type: ignore[attr-defined]
            # if it's delayed future, get the result when future.result() get called
            if self._is_delayed:
                self._request_result_object()

            # wait for
            return super().result(timeout)

    def cancel(self) -> bool:
        with self._condition:  # type: ignore[attr-defined]
            if self.cancelled():
                return True

            if self.done():
                return False

            if self._group_task_id is not None:
                self._connector.send(TaskCancel.new_msg(self._group_task_id))
            else:
                self._connector.send(TaskCancel.new_msg(self._task_id))

            self._state = "CANCELLED"

            self._result_ready_event.set()
            self._condition.notify_all()  # type: ignore[attr-defined]

        self._invoke_callbacks()  # type: ignore[attr-defined]
        return True

    def add_done_callback(self, fn: Callable[[Future], Any]) -> None:
        with self._condition:  # type: ignore[attr-defined]
            # if it's delayed future, get the result when a callback gets added
            if self._is_delayed:
                self._request_result_object()

            return super().add_done_callback(fn)

    def _on_waiters_updated(self, waiters: EventList):
        with self._condition:  # type: ignore[attr-defined]
            # if it's delayed future, get the result when waiter gets added
            if self._is_delayed and len(self._waiters) > 0:
                self._request_result_object()

    def _has_result_listeners(self) -> bool:
        return len(self._done_callbacks) > 0 or len(self._waiters) > 0  # type: ignore[attr-defined]

    def _request_result_object(self):
        if self._result_request_sent or self._result_object_id is None or self.cancelled():
            return

        self._connector.send(ObjectRequest.new_msg(ObjectRequest.ObjectRequestType.Get, (self._result_object_id,)))
        self._result_request_sent = True
