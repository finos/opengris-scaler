import asyncio
import dataclasses
import enum
import logging
from typing import Any, Dict, List, Optional, Tuple, cast

import cloudpickle
from bidict import bidict

from scaler import Serializer
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import (
    ObjectInstruction,
    ObjectMetadata,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
)
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.mixins import HeartbeatManager
from scaler.worker.agent.mixins import TaskManager as TaskManagerMixin
from scaler.worker_manager.proxy.mixins import ExecutionBackend

logger = logging.getLogger(__name__)


class _TaskState(enum.Enum):
    QUEUED = enum.auto()
    STARTING = enum.auto()  # execute() is awaited, so the task has no future yet
    RUNNING = enum.auto()
    CANCELING = enum.auto()  # force-cancelled: its future, once it has one, is dropped rather than reported


@dataclasses.dataclass
class _TaskEntry:
    task: Task
    state: _TaskState
    holds_permit: bool = False


class TaskManager(Looper, TaskManagerMixin):
    def __init__(
        self, base_concurrency: int, execution_backend: ExecutionBackend, idle_sleep_seconds: float = 0.0
    ) -> None:
        if isinstance(base_concurrency, int) and base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be a positive integer, got {base_concurrency}")

        self._base_concurrency = base_concurrency
        self._execution_backend = execution_backend
        self._idle_sleep_seconds = idle_sleep_seconds

        self._executor_semaphore = asyncio.Semaphore(value=self._base_concurrency)

        # Each state change happens with no await inside it, so no coroutine sees a task half-moved.
        self._task_id_to_entry: Dict[TaskID, _TaskEntry] = dict()
        self._task_id_to_future: bidict[TaskID, asyncio.Future] = bidict()

        self._serializers: Dict[bytes, Serializer] = dict()

        self._queued_task_id_queue = AsyncPriorityQueue()

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._heartbeat_manager: Optional[HeartbeatManager] = None

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        heartbeat_manager: HeartbeatManager,
    ) -> None:
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._heartbeat_manager = heartbeat_manager
        self._execution_backend.register(self.load_task_inputs)

    async def on_object_instruction(self, instruction: ObjectInstruction) -> None:
        if instruction.instructionType == ObjectInstruction.ObjectInstructionType.delete:
            for object_id in instruction.objectMetadata.objectIds:
                self._serializers.pop(object_id, None)
            return

        logger.error(f"worker received unknown object instruction type {instruction=}")

    async def on_task_new(self, task: Task) -> None:
        task_priority = self._get_task_priority(task)

        if self._executor_semaphore.locked() and all(
            task_priority > self._get_task_priority(entry.task)
            for entry in self._task_id_to_entry.values()
            if entry.holds_permit
        ):
            entry = _TaskEntry(task=task, state=_TaskState.STARTING)
            self._task_id_to_entry[task.taskId] = entry
            # Bypass tasks intentionally exceed base_concurrency to service higher-priority requests immediately.
            await self._start_task(entry)
            return

        self._task_id_to_entry[task.taskId] = _TaskEntry(task=task, state=_TaskState.QUEUED)
        self._queued_task_id_queue.put_nowait((-task_priority, task.taskId))

    async def on_cancel_task(self, task_cancel: TaskCancel) -> None:
        entry = self._task_id_to_entry.get(task_cancel.taskId)

        if entry is None or entry.state == _TaskState.CANCELING:
            await self._send_cancel_confirm(task_cancel.taskId, TaskCancelConfirmType.cancelNotFound)
            return

        if entry.state == _TaskState.QUEUED:
            self._queued_task_id_queue.remove(task_cancel.taskId)
            self._task_id_to_entry.pop(task_cancel.taskId)
            await self._send_cancel_confirm(task_cancel.taskId, TaskCancelConfirmType.canceled)
            return

        if not task_cancel.flags.force:
            await self._send_cancel_confirm(task_cancel.taskId, TaskCancelConfirmType.cancelFailed)
            return

        previous_state = entry.state
        entry.state = _TaskState.CANCELING

        # A STARTING task is cancelled by _start_task once execute() returns the handle to cancel.
        if previous_state == _TaskState.RUNNING:
            future = self._task_id_to_future[task_cancel.taskId]
            await self._execution_backend.on_cancel(task_cancel)
            future.cancel()

        await self._send_cancel_confirm(task_cancel.taskId, TaskCancelConfirmType.canceled)

    async def on_task_result(self, result: TaskResult) -> None:
        # Required by TaskManagerMixin but not dispatched from WorkerProcess.__on_receive_external.
        # WorkerProcess drives result handling via resolve_tasks() instead.
        entry = self._task_id_to_entry.pop(result.taskId)
        if entry.state == _TaskState.QUEUED:
            self._queued_task_id_queue.remove(result.taskId)

        await self._connector_external.send(result, detached=True)

    def get_queued_size(self) -> int:
        return self._queued_task_id_queue.qsize()

    def can_accept_task(self) -> bool:
        return not self._executor_semaphore.locked()

    async def resolve_tasks(self) -> None:
        if not self._task_id_to_future:
            await asyncio.sleep(self._idle_sleep_seconds)
            return

        done, _ = await asyncio.wait(self._task_id_to_future.values(), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = self._task_id_to_future.inv.pop(future)
            entry = self._task_id_to_entry.pop(task_id)
            try:
                if entry.state == _TaskState.RUNNING:
                    await self._send_task_result(entry.task, future)
            finally:
                self._release_task(entry)

    async def _start_task(self, entry: _TaskEntry) -> None:
        task_id = entry.task.taskId
        future = await self._execution_backend.execute(entry.task)

        if entry.state == _TaskState.CANCELING:
            await self._execution_backend.on_cancel(
                TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=True))
            )
            future.cancel()
        else:
            entry.state = _TaskState.RUNNING

        self._task_id_to_future[task_id] = future

    async def _send_task_result(self, task: Task, future: asyncio.Future) -> None:
        if future.exception() is None:
            serializer_id = ObjectID.generate_serializer_object_id(task.source)
            serializer = self._serializers[serializer_id]
            result_bytes = serializer.serialize(future.result())
            result_type = TaskResultType.success
        else:
            result_bytes = serialize_failure(cast(Exception, future.exception()))
            result_type = TaskResultType.failed

        result_object_id = ObjectID.generate_object_id(task.source)

        await self._connector_storage.set_object(result_object_id, result_bytes)
        await self._connector_external.send(
            ObjectInstruction(
                instructionType=ObjectInstruction.ObjectInstructionType.create,
                objectUser=task.source,
                objectMetadata=ObjectMetadata(
                    objectIds=(result_object_id,),
                    objectTypes=(ObjectMetadata.ObjectContentType.object,),
                    objectNames=(f"<res {result_object_id.hex()[:6]}>".encode(),),
                ),
            ),
            detached=True,
        )

        await self._connector_external.send(
            TaskResult(taskId=task.taskId, resultType=result_type, metadata=b"", results=[bytes(result_object_id)]),
            detached=True,
        )

    async def _send_cancel_confirm(self, task_id: TaskID, cancel_confirm_type: TaskCancelConfirmType) -> None:
        await self._connector_external.send(
            TaskCancelConfirm(taskId=task_id, cancelConfirmType=cancel_confirm_type), detached=True
        )

    def _release_task(self, entry: _TaskEntry) -> None:
        if entry.holds_permit:
            self._executor_semaphore.release()

        self._execution_backend.on_cleanup(entry.task.taskId)

    async def routine(self) -> None:
        pass

    async def process_task(self) -> None:
        await self._executor_semaphore.acquire()

        _, task_id = await self._queued_task_id_queue.get()
        entry = self._task_id_to_entry[task_id]
        entry.state = _TaskState.STARTING
        entry.holds_permit = True
        await self._start_task(entry)

    @property
    def processing_task_count(self) -> int:
        return sum(
            entry.state in (_TaskState.STARTING, _TaskState.RUNNING) for entry in self._task_id_to_entry.values()
        )

    async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]:
        serializer_id = ObjectID.generate_serializer_object_id(task.source)

        if serializer_id not in self._serializers:
            serializer_bytes = await self._connector_storage.get_object(serializer_id)
            serializer = cloudpickle.loads(serializer_bytes)
            self._serializers[serializer_id] = serializer
        else:
            serializer = self._serializers[serializer_id]

        get_tasks = [
            self._connector_storage.get_object(object_id)
            for object_id in [ObjectID(task.funcObjectId), *(ObjectID(argument.data) for argument in task.functionArgs)]
        ]

        function_bytes, *arg_bytes = await asyncio.gather(*get_tasks)

        function = serializer.deserialize(function_bytes)
        arg_objects = [serializer.deserialize(object_bytes) for object_bytes in arg_bytes]
        return function, arg_objects

    @staticmethod
    def _get_task_priority(task: Task) -> int:
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
