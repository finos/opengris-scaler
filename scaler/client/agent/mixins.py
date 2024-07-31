import abc
from concurrent.futures import Future

from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeatEcho,
    ClientShutdownResponse,
    GraphTask,
    ObjectInstruction,
    ObjectRequest,
    ObjectResponse,
    Task,
    TaskResult,
)


class HeartbeatManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def send_heartbeat(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat_echo(self, heartbeat: ClientHeartbeatEcho):
        raise NotImplementedError()


class TimeoutManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update_last_seen_time(self):
        raise NotImplementedError()


class ObjectManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_object_instruction(self, object_instruction: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_object_request(self, request: ObjectRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    def record_task_result(self, task_id: bytes, object_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    async def clean_all_objects(self):
        raise NotImplementedError()


class TaskManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_new_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_new_graph_task(self, task: GraphTask):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()


class FutureManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_future(self, future: Future):
        raise NotImplementedError()

    @abc.abstractmethod
    def cancel_all_futures(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_all_futures_with_exception(self, exception: Exception):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_object_response(self, response: ObjectResponse):
        raise NotImplementedError()


class DisconnectManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_client_disconnect(self, disconnect: ClientDisconnect):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_shutdown_response(self, response: ClientShutdownResponse):
        raise NotImplementedError()
