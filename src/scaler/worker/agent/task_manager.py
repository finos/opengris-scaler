import asyncio
import dataclasses
import enum
from typing import Dict, Optional

from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import Task, TaskCancel, TaskCancelConfirm, TaskCancelConfirmType, TaskResult
from scaler.utility.identifiers import TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.stable_priority_queue import StablePriorityQueue
from scaler.worker.agent.mixins import ProcessorManager, TaskManager


class _TaskState(enum.Enum):
    WAITING = enum.auto()  # received, never started
    RUNNING = enum.auto()  # on the current processor
    SUSPENDED = enum.auto()  # paused on its own processor for a higher-priority task


@dataclasses.dataclass
class _TaskEntry:
    task: Task
    priority: int  # higher runs first
    state: _TaskState


class _TaskTable:
    """Every task on the worker with its state: the only place that holds them.

    Keeps the waiting and suspended tasks in priority queues, so picking the next one costs O(log n). Only this class
    writes the queues, so they always agree with the entries.
    """

    def __init__(self) -> None:
        self._entries: Dict[TaskID, _TaskEntry] = {}

        # StablePriorityQueue hands out the lowest key first, the oldest among equal keys: keys are negated priorities.
        self._waiting_task_ids: StablePriorityQueue[int, TaskID] = StablePriorityQueue()
        self._suspended_task_ids: StablePriorityQueue[int, TaskID] = StablePriorityQueue()
        self._runnable_added = asyncio.Event()
        self._draining = False  # a draining table hands out only suspended tasks

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, task_id: TaskID) -> Optional[_TaskEntry]:
        return self._entries.get(task_id)

    def waiting_count(self) -> int:
        return len(self._waiting_task_ids)

    def drain(self) -> None:
        self._draining = True

    def is_draining(self) -> bool:
        return self._draining

    def add(self, task: Task, priority: int) -> _TaskEntry:
        entry = _TaskEntry(task=task, priority=priority, state=_TaskState.WAITING)
        self._entries[task.taskId] = entry
        self.__add_to_queue(entry)
        return entry

    def set_state(self, entry: _TaskEntry, state: _TaskState) -> None:
        self.__remove_from_queue(entry)
        entry.state = state
        self.__add_to_queue(entry)

    def remove(self, entry: _TaskEntry) -> None:
        self.__remove_from_queue(entry)
        self._entries.pop(entry.task.taskId)

    async def wait_for_next(self) -> _TaskEntry:
        """The highest priority first, a suspended task before a waiting one of equal priority, then the oldest."""
        while (entry := self.__next()) is None:
            self._runnable_added.clear()
            await self._runnable_added.wait()
        return entry

    def __next(self) -> Optional[_TaskEntry]:
        suspended = self.__head(self._suspended_task_ids)
        waiting = None if self._draining else self.__head(self._waiting_task_ids)
        if waiting is None or (suspended is not None and suspended.priority >= waiting.priority):
            return suspended
        return waiting

    def __head(self, task_ids: StablePriorityQueue[int, TaskID]) -> Optional[_TaskEntry]:
        if not task_ids:
            return None
        _, task_id = task_ids.max_priority_item()
        return self._entries[task_id]

    def __add_to_queue(self, entry: _TaskEntry) -> None:
        task_ids = self.__queue_for(entry.state)
        if task_ids is None:
            return
        task_ids.put(-entry.priority, entry.task.taskId)
        self._runnable_added.set()

    def __remove_from_queue(self, entry: _TaskEntry) -> None:
        task_ids = self.__queue_for(entry.state)
        if task_ids is not None:
            task_ids.remove(entry.task.taskId)

    def __queue_for(self, state: _TaskState) -> Optional[StablePriorityQueue[int, TaskID]]:
        match state:
            case _TaskState.WAITING:
                return self._waiting_task_ids
            case _TaskState.SUSPENDED:
                return self._suspended_task_ids
            case _TaskState.RUNNING:
                return None


class VanillaTaskManager(Looper, TaskManager):
    def __init__(self, task_timeout_seconds: int):
        self._task_timeout_seconds = task_timeout_seconds

        self._tasks = _TaskTable()

        self._connector_external: Optional[AsyncConnector] = None
        self._processor_manager: Optional[ProcessorManager] = None

    def register(self, connector: AsyncConnector, processor_manager: ProcessorManager):
        self._connector_external = connector
        self._processor_manager = processor_manager

    async def on_task_new(self, task: Task):
        entry = self._tasks.add(task, self.__get_task_priority(task))

        await self.__suspend_if_priority_is_higher(entry)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        task_id = task_cancel.taskId
        entry = self._tasks.get(task_id)

        if entry is None:
            await self.__send_cancel_confirm(task_id, TaskCancelConfirmType.cancelNotFound)
            return

        if entry.state != _TaskState.WAITING and not task_cancel.flags.force:
            # a task on a processor, running or suspended, is cancelled only by force
            await self.__send_cancel_confirm(task_id, TaskCancelConfirmType.cancelFailed)
            return

        self._tasks.remove(entry)
        if entry.state != _TaskState.WAITING:
            await self._processor_manager.on_cancel_task(task_id)

        await self.__send_cancel_confirm(task_id, TaskCancelConfirmType.canceled)

    async def on_task_result(self, result: TaskResult):
        # A task can finish while it is being suspended, so the entry is running or suspended.
        entry = self._tasks.get(result.taskId)
        assert entry is not None, f"result for a task this worker does not hold: {result.taskId!r}"
        self._tasks.remove(entry)

        await self._connector_external.send(result, detached=True)

    async def routine(self):
        await self.__processing_task()

    def get_queued_size(self):
        return self._tasks.waiting_count()

    def drain(self) -> None:
        self._tasks.drain()

    def is_draining(self) -> bool:
        return self._tasks.is_draining()

    def is_idle(self) -> bool:
        return len(self._tasks) == 0

    async def __processing_task(self):
        await self._processor_manager.wait_until_can_accept_task()

        entry = await self._tasks.wait_for_next()
        resume = entry.state == _TaskState.SUSPENDED
        self._tasks.set_state(entry, _TaskState.RUNNING)

        if resume:
            await self._processor_manager.on_resume_task(entry.task.taskId)
        else:
            await self._processor_manager.on_task(entry.task)

    async def __suspend_if_priority_is_higher(self, new_entry: _TaskEntry):
        current_task = self._processor_manager.current_task()

        if current_task is None:
            return

        running = self._tasks.get(current_task.taskId)
        assert running is not None, f"the current task is not on this worker: {current_task.taskId!r}"

        if new_entry.priority <= running.priority:
            return

        self._tasks.set_state(running, _TaskState.SUSPENDED)

        await self._processor_manager.on_suspend_task(current_task.taskId)

    async def __send_cancel_confirm(self, task_id: TaskID, cancel_confirm_type: TaskCancelConfirmType) -> None:
        await self._connector_external.send(
            TaskCancelConfirm(taskId=task_id, cancelConfirmType=cancel_confirm_type), detached=True
        )

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
