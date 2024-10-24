import abc
from typing import List, Optional, Set

from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeat,
    DisconnectRequest,
    GraphTask,
    GraphTaskCancel,
    ObjectInstruction,
    ObjectRequest,
    Task,
    TaskCancel,
    TaskResult,
    WorkerHeartbeat,
)
from scaler.utility.mixins import Reporter


class ObjectManager(Reporter):
    @abc.abstractmethod
    async def on_object_instruction(self, source: bytes, request: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_object_request(self, source: bytes, request: ObjectRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_add_object(self, object_user: bytes, object_id: bytes, object_name: bytes, object_bytes: List[bytes]):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_del_objects(self, task_id: bytes, object_ids: Set[bytes]):
        raise NotImplementedError()

    @abc.abstractmethod
    def clean_client(self, client: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_object(self, object_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object_name(self, object_id: bytes) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object_content(self, object_id: bytes) -> List[bytes]:
        raise NotImplementedError()


class ClientManager(Reporter):
    @abc.abstractmethod
    def get_client_task_ids(self, client: bytes) -> Set[bytes]:
        raise NotImplementedError()

    @abc.abstractmethod
    def has_client_id(self, client_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_client_id(self, task_id: bytes) -> Optional[bytes]:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_begin(self, client: bytes, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_finish(self, task_id: bytes) -> bytes:
        raise NotImplementedError()

    async def on_heartbeat(self, client: bytes, info: ClientHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_disconnect(self, client: bytes, request: ClientDisconnect):
        raise NotImplementedError()


class GraphTaskManager(Reporter):
    @abc.abstractmethod
    async def on_graph_task(self, client: bytes, graph_task: GraphTask):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_task_cancel(self, client: bytes, graph_task_cancel: GraphTaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_sub_task_done(self, result: TaskResult) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_graph_sub_task(self, task_id: bytes):
        raise NotImplementedError()


class TaskManager(Reporter):
    @abc.abstractmethod
    async def on_task_new(self, client: bytes, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client: bytes, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_reroute(self, task_id: bytes):
        raise NotImplementedError()


class WorkerManager(Reporter):
    @abc.abstractmethod
    async def assign_task_to_worker(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, task_result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker: bytes, info: WorkerHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_shutdown(self, client: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_disconnect(self, source: bytes, request: DisconnectRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: bytes) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> Set[bytes]:
        raise NotImplementedError()
