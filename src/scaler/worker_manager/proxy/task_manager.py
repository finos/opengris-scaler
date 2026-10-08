import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple, cast

import cloudpickle

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
from scaler.utility.mixins import Looper
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.mixins import HeartbeatManager
from scaler.worker.agent.mixins import TaskManager as TaskManagerMixin
from scaler.worker_manager.proxy.mixins import ExecutionBackend
from scaler.worker_manager.proxy.task_actor import TaskActor

logger = logging.getLogger(__name__)


class TaskManager(Looper, TaskManagerMixin):
    """The worker's side of the TaskActor: registration, serializers, result uploads, and the mixin entry points.

    upload_results() sends the results of finished tasks, so a slow upload does not hold up the actor.
    """

    def __init__(self, base_concurrency: int, execution_backend: ExecutionBackend) -> None:
        self._execution_backend = execution_backend
        self._results: asyncio.Queue[Tuple[Task, asyncio.Future]] = asyncio.Queue()
        self._actor = TaskActor(
            base_concurrency=base_concurrency,
            execution_backend=execution_backend,
            send_cancel_confirm=self._send_cancel_confirm,
            report_result=lambda task, future: self._results.put_nowait((task, future)),
        )

        self._serializers: Dict[bytes, Serializer] = dict()

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
        self._actor.post_task_new(task)

    async def on_cancel_task(self, task_cancel: TaskCancel) -> None:
        self._actor.post_cancel(task_cancel)

    async def on_task_result(self, result: TaskResult) -> None:
        raise NotImplementedError("WorkerProcess never dispatches TaskResult to the proxy task manager")

    def get_queued_size(self) -> int:
        return self._actor.queued_task_count

    def can_accept_task(self) -> bool:
        return self._actor.has_free_permit

    @property
    def processing_task_count(self) -> int:
        return self._actor.processing_task_count

    async def routine(self) -> None:
        await self._actor.routine()

    async def upload_results(self) -> None:
        task, future = await self._results.get()
        await self._send_task_result(task, future)

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
