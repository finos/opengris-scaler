import abc
from typing import Any, Optional, Set

from scaler.protocol.python.common import ObjectMetadata
from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeat,
    DisconnectRequest,
    GraphTask,
    InformationRequest,
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskResult,
    WorkerHeartbeat,
)
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.mixins import Reporter


class ConfigController(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_config(self, path: str) -> Any:
        raise NotImplementedError()

    @abc.abstractmethod
    def update_config(self, path: str, value: Any):
        raise NotImplementedError()


class ObjectController(Reporter):
    @abc.abstractmethod
    async def on_object_instruction(self, source: bytes, request: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_add_object(
        self,
        client_id: ClientID,
        object_id: ObjectID,
        object_type: ObjectMetadata.ObjectContentType,
        object_name: bytes,
    ):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_del_objects(self, client_id: ClientID, object_ids: Set[ObjectID]):
        raise NotImplementedError()

    @abc.abstractmethod
    def clean_client(self, client_id: ClientID):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_object(self, object_id: ObjectID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object_name(self, object_id: ObjectID) -> bytes:
        raise NotImplementedError()


class ClientController(Reporter):
    @abc.abstractmethod
    def get_client_task_ids(self, client_id: ClientID) -> Set[TaskID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def has_client_id(self, client_id: ClientID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_client_id(self, task_id: TaskID) -> Optional[ClientID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_begin(self, client_id: ClientID, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_finish(self, task_id: TaskID) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, client_id: ClientID, info: ClientHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_disconnect(self, client_id: ClientID, request: ClientDisconnect):
        raise NotImplementedError()


class GraphTaskController(Reporter):
    @abc.abstractmethod
    async def on_graph_task(self, client_id: ClientID, graph_task: GraphTask):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_task_cancel(self, graph_task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_sub_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_sub_task_result(self, result: TaskResult) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_graph_subtask(self, task_id: TaskID) -> bool:
        raise NotImplementedError()


class TaskController(Reporter):
    @abc.abstractmethod
    async def on_task_new(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client_id: ClientID, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_balance_cancel(self, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_worker_connect(self, worker_id: WorkerID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_worker_disconnect(self, task_id: TaskID, worker_id: WorkerID):
        raise NotImplementedError()


class WorkerController(Reporter):
    @abc.abstractmethod
    def acquire_worker(self, task: Task) -> Optional[WorkerID]:
        """this acquires worker should be atomic, means it cannot be async decorated, otherwise it will create gap that
        get worker but task is not send to worker, and cannot find task in the worker state"""

        # TODO: this function should return things that expose 3 kinds of information:
        # TODO: 1. worker id as bytes if have capacity and able to assign to worker id
        # TODO: 2. capacity is full, and unable to add new task
        # TODO: 3. capacity is not full, but all the workers are busy right now, so tasks will be queued
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, task_cancel: TaskCancel) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker_id: WorkerID, info: WorkerHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_shutdown(self, client_id: ClientID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_disconnect(self, worker_id: WorkerID, request: DisconnectRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> Set[WorkerID]:
        raise NotImplementedError()


class InformationController(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_request(self, request: InformationRequest):
        raise NotImplementedError()
