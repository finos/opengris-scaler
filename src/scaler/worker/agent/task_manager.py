from typing import Dict, Optional, Set

from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import Task, TaskCancel, TaskCancelConfirm, TaskCancelConfirmType, TaskResult, TaskResultType
from scaler.utility.identifiers import TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.utility.semantic_conventions import (
    TASK_ID_ATTRIBUTE,
    WORKER_TASK_CANCELED_EVENT,
    WORKER_TASK_RESUMED_EVENT,
    WORKER_TASK_SPAN,
    WORKER_TASK_STARTED_EVENT,
    WORKER_TASK_SUSPENDED_EVENT,
)
from scaler.utility.telemetry import extract_trace_context, inject_trace_context
from scaler.worker.agent.mixins import ProcessorManager, TaskManager

tracer = trace.get_tracer(__name__)


RESULT_STATUS: Dict[TaskResultType, StatusCode] = {
    TaskResultType.success: StatusCode.OK,
    TaskResultType.failed: StatusCode.ERROR,
    TaskResultType.failedWorkerDied: StatusCode.ERROR,
}


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

        # one span per task this worker holds, from receipt to the result or cancel it sends back
        self._task_id_to_span: Dict[TaskID, Span] = dict()

        self._connector_external: Optional[AsyncConnector] = None
        self._processor_manager: Optional[ProcessorManager] = None

    def register(self, connector: AsyncConnector, processor_manager: ProcessorManager):
        self._connector_external = connector
        self._processor_manager = processor_manager

    async def on_task_new(self, task: Task):
        parent_context = extract_trace_context(task.traceContext)
        span = tracer.start_span(
            WORKER_TASK_SPAN,
            context=parent_context,
            kind=SpanKind.CONSUMER,
            attributes={TASK_ID_ATTRIBUTE: task.taskId.hex()},
        )
        self._task_id_to_span[task.taskId] = span
        # the processor nests under this span, and receives the client's baggage with it
        task.traceContext = inject_trace_context(trace.set_span_in_context(span, parent_context))

        self.__enqueue_task(task, is_suspended=False)

        await self.__suspend_if_priority_is_higher(task)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        task_not_found = (
            task_cancel.taskId not in self._processing_task_ids
            and task_cancel.taskId not in self._queued_task_id_to_task
        )
        if task_not_found:
            await self._connector_external.send(
                TaskCancelConfirm(taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.cancelNotFound),
                detached=True,
            )
            return

        if task_cancel.taskId in self._processing_task_ids and not task_cancel.flags.force:
            # ignore cancel task while in processing if is not force cancel
            await self._connector_external.send(
                TaskCancelConfirm(taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.cancelFailed),
                detached=True,
            )
            return

        # A suspended task will be both processing AND queued

        if task_cancel.taskId in self._processing_task_ids:
            # if task is in processing
            self._processing_task_ids.remove(task_cancel.taskId)
            _ = await self._processor_manager.on_cancel_task(task_cancel.taskId)
        else:
            # if task is queued
            assert task_cancel.taskId in self._queued_task_id_to_task
            self._queued_task_ids.remove(task_cancel.taskId)
            _ = self._queued_task_id_to_task.pop(task_cancel.taskId)

        self.__end_span(task_cancel.taskId, StatusCode.UNSET, WORKER_TASK_CANCELED_EVENT)
        await self._connector_external.send(
            TaskCancelConfirm(taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.canceled),
            detached=True,
        )

    async def on_task_result(self, result: TaskResult):
        if result.taskId in self._queued_task_id_to_task:
            # Finishing a queued task might happen if a task ended during the suspension process.
            self._queued_task_id_to_task.pop(result.taskId)
            self._queued_task_ids.remove(result.taskId)

        self._processing_task_ids.remove(result.taskId)

        self.__end_span(
            result.taskId,
            RESULT_STATUS.get(result.resultType, StatusCode.UNSET),
            TaskResultType(result.resultType).name,
        )
        await self._connector_external.send(result, detached=True)

    async def routine(self):
        await self.__processing_task()

    def get_queued_size(self):
        return self._queued_task_ids.qsize()

    async def __processing_task(self):
        await self._processor_manager.wait_until_can_accept_task()

        _, task_id = await self._queued_task_ids.get()
        task = self._queued_task_id_to_task.pop(task_id)

        span = self._task_id_to_span.get(task_id)
        if task_id not in self._processing_task_ids:
            self._processing_task_ids.add(task_id)
            if span is not None:
                span.add_event(WORKER_TASK_STARTED_EVENT)
            await self._processor_manager.on_task(task)
        else:
            if span is not None:
                span.add_event(WORKER_TASK_RESUMED_EVENT)
            await self._processor_manager.on_resume_task(task_id)

    async def __suspend_if_priority_is_higher(self, new_task: Task):
        current_task = self._processor_manager.current_task()

        if current_task is None:
            return

        new_task_priority = self.__get_task_priority(new_task)
        current_task_priority = self.__get_task_priority(current_task)

        if new_task_priority <= current_task_priority:
            return

        self.__enqueue_task(current_task, is_suspended=True)

        span = self._task_id_to_span.get(current_task.taskId)
        if span is not None:
            span.add_event(WORKER_TASK_SUSPENDED_EVENT)
        await self._processor_manager.on_suspend_task(current_task.taskId)

    def __enqueue_task(self, task: Task, is_suspended: bool):
        task_priority = self.__get_task_priority(task)

        # Higher-priority tasks have a higher priority value. But as the queue is sorted by increasing order, we negate
        # the inserted value that it will be at the head of the queue.
        # The order of insertion is preserved and unique because f(x) = 10*x is a bijection.
        #   1. Task(priority=0) [suspended] = -(0 * 10 + 1) = -1
        #   2. Task(priority=3) [suspended] = -(3 * 10 + 1) = -31
        #   3. Task(priority=3)             = -(3 * 10 + 0) = -30
        #   4. Task(priority=0)             = -(3 *  0 + 0) = -3
        # We want to execute the tasks in this order: 2-3-1-4.
        # As you can see -31 < -30 < -3 < -1 and the task will be executed in order 2-3-1-4.
        # `queue_priority` used to be a tuple of the form (-task_priority, !is_suspended).
        # Because it is difficult to implement the semantics in C++ implementation, the two
        # items are combined together, extending the last digit of task_priority to determine
        # whether a task is suspended.
        queue_priority = -(task_priority * 10 + int(is_suspended))

        self._queued_task_ids.put_nowait((queue_priority, task.taskId))
        self._queued_task_id_to_task[task.taskId] = task

    def __end_span(self, task_id: TaskID, status: StatusCode, outcome: str) -> None:
        span = self._task_id_to_span.pop(task_id, None)
        if span is None:
            return
        span.add_event(outcome)
        span.set_status(Status(status))
        span.end()

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
