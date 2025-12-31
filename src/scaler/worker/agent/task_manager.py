from typing import Dict, Optional, Set

from scaler.io.mixins import AsyncConnector
from scaler.protocol.python.common import TaskCancelConfirmType
from scaler.protocol.python.message import Task, TaskCancel, TaskCancelConfirm, TaskResult
from scaler.utility.identifiers import TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.worker.agent.mixins import ProcessorManager, TaskManager

_SUSPENDED_TASKS_PRIORITY = 1
_QUEUED_TASKS_PRIORITY = 2


class VanillaTaskManager(Looper, TaskManager):
    def __init__(self, task_timeout_seconds: int):
        self._task_timeout_seconds = task_timeout_seconds

        self._queued_task_id_to_task: Dict[TaskID, Task] = dict()

        # Queued tasks are sorted first by task's priorities, then suspended tasks are prioritized over non yet started
        # tasks, finally the sorted queue ensure we execute the oldest tasks first.
        #
        # For example, if we receive these tasks in this order:
        #   1. Task(priority=0) [suspended]
        #   2. Task(priority=3) [suspended]
        #   3. Task(priority=3)
        #   4. Task(priority=0)
        #
        # We want to execute the tasks in this order: 2-3-1-4.
        self._queued_task_ids = AsyncPriorityQueue()

        self._processing_task_ids: Set[TaskID] = set()  # Tasks associated with a processor, including suspended tasks

        self._connector_external: Optional[AsyncConnector] = None
        self._processor_manager: Optional[ProcessorManager] = None

    def register(self, connector: AsyncConnector, processor_manager: ProcessorManager):
        self._connector_external = connector
        self._processor_manager = processor_manager

    async def on_task_new(self, task: Task):
        self.__enqueue_task(task, is_suspended=False)

        await self.__suspend_if_priority_is_higher(task)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        task_not_found = (
            task_cancel.task_id not in self._processing_task_ids
            and task_cancel.task_id not in self._queued_task_id_to_task
        )
        if task_not_found:
            await self._connector_external.send(
                TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.CancelNotFound
                )
            )
            return

        if task_cancel.task_id in self._processing_task_ids and not task_cancel.flags.force:
            # ignore cancel task while in processing if is not force cancel
            await self._connector_external.send(
                TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.CancelFailed
                )
            )
            return

        # A suspended task will be both processing AND queued

        if task_cancel.task_id in self._processing_task_ids:
            # if task is in processing
            self._processing_task_ids.remove(task_cancel.task_id)
            _ = await self._processor_manager.on_cancel_task(task_cancel.task_id)
        else:
            # if task is queued
            assert task_cancel.task_id in self._queued_task_id_to_task
            self._queued_task_ids.remove(task_cancel.task_id)
            _ = self._queued_task_id_to_task.pop(task_cancel.task_id)

        await self._connector_external.send(
            TaskCancelConfirm.new_msg(task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.Canceled)
        )

    async def on_task_result(self, result: TaskResult):
        if result.task_id in self._queued_task_id_to_task:
            # Finishing a queued task might happen if a task ended during the suspension process.
            self._queued_task_id_to_task.pop(result.task_id)
            self._queued_task_ids.remove(result.task_id)

        self._processing_task_ids.remove(result.task_id)

        await self._connector_external.send(result)

    async def routine(self):
        await self.__processing_task()

    def get_queued_size(self):
        return self._queued_task_ids.qsize()

    async def __processing_task(self):
        await self._processor_manager.wait_until_can_accept_task()

        _, task_id = await self._queued_task_ids.get()
        task = self._queued_task_id_to_task.pop(task_id)

        if task_id not in self._processing_task_ids:
            self._processing_task_ids.add(task_id)
            await self._processor_manager.on_task(task)
        else:
            self._processor_manager.on_resume_task(task_id)

    async def __suspend_if_priority_is_higher(self, new_task: Task):
        current_task = self._processor_manager.current_task()

        if current_task is None:
            return

        new_task_priority = self.__get_task_priority(new_task)
        current_task_priority = self.__get_task_priority(current_task)

        if new_task_priority <= current_task_priority:
            return

        self.__enqueue_task(current_task, is_suspended=True)

        await self._processor_manager.on_suspend_task(current_task.task_id)

    def __enqueue_task(self, task: Task, is_suspended: bool):
        task_priority = self.__get_task_priority(task)

        # Higher-priority tasks have a higher priority value. But as the queue is sorted by increasing order, we negate
        # the inserted value that it will be at the head of the queue.
        if is_suspended:
            queue_priority = (-task_priority, _SUSPENDED_TASKS_PRIORITY)
        else:
            queue_priority = (-task_priority, _QUEUED_TASKS_PRIORITY)

        self._queued_task_ids.put_nowait((queue_priority, task.task_id))
        self._queued_task_id_to_task[task.task_id] = task

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
