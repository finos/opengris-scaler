import asyncio
import dataclasses
import enum
import logging
from typing import Any, Awaitable, Callable, Coroutine, Dict, Optional, Set, Union, cast

from scaler.protocol.capnp import Task, TaskCancel, TaskCancelConfirmType
from scaler.utility.exceptions import TaskCancelUnsupportedError
from scaler.utility.identifiers import TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.worker_manager.proxy.mixins import ExecutionBackend

logger = logging.getLogger(__name__)

CancelConfirmSender = Callable[[TaskID, TaskCancelConfirmType], Awaitable[None]]
ResultReporter = Callable[[Task, asyncio.Future], None]


class _TaskState(enum.Enum):
    QUEUED = enum.auto()  # waiting in the priority queue for a permit
    STARTING = enum.auto()  # execute() is in flight, so the task has no future yet
    RUNNING = enum.auto()  # execute() returned its future; the result is reported when the future resolves
    CANCELING = enum.auto()  # force-cancelled: its future, once it has one, is cancelled and dropped unreported


@dataclasses.dataclass
class _TaskEntry:
    task: Task
    state: _TaskState
    holds_permit: bool = False
    future: Optional[asyncio.Future] = None


@dataclasses.dataclass(frozen=True)
class _TaskNew:
    task: Task


@dataclasses.dataclass(frozen=True)
class _CancelRequested:
    task_cancel: TaskCancel


@dataclasses.dataclass(frozen=True)
class _Started:
    task_id: TaskID
    future: asyncio.Future


@dataclasses.dataclass(frozen=True)
class _Finished:
    task_id: TaskID
    future: asyncio.Future


@dataclasses.dataclass(frozen=True)
class _CancelDone:
    task_id: TaskID
    succeeded: bool


_Event = Union[_TaskNew, _CancelRequested, _Started, _Finished, _CancelDone]


class TaskActor:
    """Owns every task on the worker. Only routine() changes task state; everything else posts an event to it."""

    def __init__(
        self,
        base_concurrency: int,
        execution_backend: ExecutionBackend,
        send_cancel_confirm: CancelConfirmSender,
        report_result: ResultReporter,
    ) -> None:
        if isinstance(base_concurrency, int) and base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be a positive integer, got {base_concurrency}")

        self._base_concurrency = base_concurrency
        self._execution_backend = execution_backend
        self._send_cancel_confirm = send_cancel_confirm
        self._report_result = report_result

        self._events: asyncio.Queue[_Event] = asyncio.Queue()
        self._background_tasks: Set[asyncio.Task] = set()

        self._task_id_to_entry: Dict[TaskID, _TaskEntry] = dict()
        self._queued_task_id_queue = AsyncPriorityQueue()
        self._permits_held = 0

    def post_task_new(self, task: Task) -> None:
        self._events.put_nowait(_TaskNew(task))

    def post_cancel(self, task_cancel: TaskCancel) -> None:
        self._events.put_nowait(_CancelRequested(task_cancel))

    @property
    def queued_task_count(self) -> int:
        return self._queued_task_id_queue.qsize()

    @property
    def has_free_permit(self) -> bool:
        return self._permits_held < self._base_concurrency

    @property
    def processing_task_count(self) -> int:
        return sum(
            entry.state in (_TaskState.STARTING, _TaskState.RUNNING) for entry in self._task_id_to_entry.values()
        )

    async def routine(self) -> None:
        match await self._events.get():
            case _TaskNew(task):
                self._handle_task_new(task)
            case _CancelRequested(task_cancel):
                await self._handle_cancel(task_cancel)
            case _Started(task_id, future):
                self._handle_started(task_id, future)
            case _Finished(task_id, future):
                self._handle_finished(task_id, future)
            case _CancelDone(task_id, succeeded):
                await self._handle_cancel_done(task_id, succeeded)

    def _handle_task_new(self, task: Task) -> None:
        task_priority = self._get_task_priority(task)
        entry = _TaskEntry(task=task, state=_TaskState.QUEUED)
        self._task_id_to_entry[task.taskId] = entry

        if self._permits_held == self._base_concurrency and all(
            task_priority > self._get_task_priority(other.task)
            for other in self._task_id_to_entry.values()
            if other.holds_permit
        ):
            # Bypass tasks intentionally exceed base_concurrency to service higher-priority requests immediately.
            self._start_task(entry, holds_permit=False)
            return

        self._queued_task_id_queue.put_nowait((-task_priority, task.taskId))
        self._start_queued_tasks()

    async def _handle_cancel(self, task_cancel: TaskCancel) -> None:
        task_id = task_cancel.taskId
        entry = self._task_id_to_entry.get(task_id)

        if entry is None or entry.state == _TaskState.CANCELING:
            await self._send_cancel_confirm(task_id, TaskCancelConfirmType.cancelNotFound)
            return

        if entry.state == _TaskState.QUEUED:
            self._queued_task_id_queue.remove(task_id)
            self._task_id_to_entry.pop(task_id)
            await self._send_cancel_confirm(task_id, TaskCancelConfirmType.canceled)
            return

        if not task_cancel.flags.force:
            await self._send_cancel_confirm(task_id, TaskCancelConfirmType.cancelFailed)
            return

        # A STARTING task has nothing to cancel in the backend yet: _handle_started cancels it once it does.
        # Either way the confirm waits for _CancelDone, so it reports what the backend did.
        if entry.state == _TaskState.RUNNING:
            self._spawn(self._cancel_in_backend(task_id, cast(asyncio.Future, entry.future)))

        entry.state = _TaskState.CANCELING

    def _handle_started(self, task_id: TaskID, future: asyncio.Future) -> None:
        entry = self._task_id_to_entry[task_id]
        entry.future = future
        future.add_done_callback(lambda done: self._events.put_nowait(_Finished(task_id, done)))

        if entry.state == _TaskState.CANCELING:
            self._spawn(self._cancel_in_backend(task_id, future))
            return

        entry.state = _TaskState.RUNNING

    def _handle_finished(self, task_id: TaskID, future: asyncio.Future) -> None:
        entry = self._task_id_to_entry.pop(task_id)
        if entry.holds_permit:
            self._permits_held -= 1
        self._execution_backend.on_cleanup(task_id)
        self._start_queued_tasks()

        if entry.state == _TaskState.RUNNING:
            self._report_result(entry.task, future)

    async def _handle_cancel_done(self, task_id: TaskID, succeeded: bool) -> None:
        entry = self._task_id_to_entry.get(task_id)

        # A task that ended while its cancel was in flight was dropped unreported, so it reads as cancelled.
        if succeeded or entry is None:
            await self._send_cancel_confirm(task_id, TaskCancelConfirmType.canceled)
            return

        # The remote work may still be running: report its result when it comes, as if the cancel never happened.
        entry.state = _TaskState.RUNNING
        await self._send_cancel_confirm(task_id, TaskCancelConfirmType.cancelFailed)

    def _start_queued_tasks(self) -> None:
        while self._permits_held < self._base_concurrency and self._queued_task_id_queue.qsize() > 0:
            _, task_id = self._queued_task_id_queue.get_nowait()
            self._start_task(self._task_id_to_entry[task_id], holds_permit=True)

    def _start_task(self, entry: _TaskEntry, holds_permit: bool) -> None:
        entry.state = _TaskState.STARTING
        entry.holds_permit = holds_permit
        if holds_permit:
            self._permits_held += 1
        self._spawn(self._execute(entry.task))

    def _spawn(self, coroutine: Coroutine[Any, Any, None]) -> None:
        background_task = asyncio.get_running_loop().create_task(coroutine)
        self._background_tasks.add(background_task)
        background_task.add_done_callback(self._background_tasks.discard)

    async def _execute(self, task: Task) -> None:
        try:
            future = await self._execution_backend.execute(task)
        except Exception as exc:
            logger.exception(f"Failed to start task: task_id={task.taskId.hex()}")
            future = asyncio.get_running_loop().create_future()
            future.set_exception(exc)

        self._events.put_nowait(_Started(task.taskId, future))

    async def _cancel_in_backend(self, task_id: TaskID, future: asyncio.Future) -> None:
        try:
            await self._execution_backend.on_cancel(
                TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=True))
            )
        except TaskCancelUnsupportedError as error:
            logger.warning(f"Backend cannot cancel task: task_id={task_id.hex()}: {error}")
            self._events.put_nowait(_CancelDone(task_id, succeeded=False))
            return
        except Exception:
            logger.exception(f"Failed to cancel task in the backend: task_id={task_id.hex()}")
            self._events.put_nowait(_CancelDone(task_id, succeeded=False))
            return

        future.cancel()
        self._events.put_nowait(_CancelDone(task_id, succeeded=True))

    @staticmethod
    def _get_task_priority(task: Task) -> int:
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
