import abc
from typing import Dict, List, Optional, Set

from scaler.protocol.python.message import (
    ObjectInstruction,
    ObjectRequest,
    ObjectResponse,
    Task,
    TaskCancel,
    TaskResult,
    WorkerHeartbeatEcho,
)
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.worker.agent.processor_holder import ProcessorHolder


class HeartbeatManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_processor_pid(self, process_id: int):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        raise NotImplementedError()


class TimeoutManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update_last_seen_time(self):
        raise NotImplementedError()


class TaskManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_task_new(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_cancel_task(self, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_queued_size(self):
        raise NotImplementedError()


class ProcessorManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_object_instruction(self, instruction: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_object_response(self, request: ObjectResponse):
        raise NotImplementedError()

    @abc.abstractmethod
    async def acquire_task_active_lock(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_cancel_task(self, task_id: bytes) -> Optional[Task]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_failing_task(self, error: str):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_suspend_task(self, task_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_resume_task(self, task_id: bytes) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def restart_current_processor(self, reason: str):
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self, reason: str):
        raise NotImplementedError()

    @abc.abstractmethod
    def initialized(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def current_task(self) -> Optional[Task]:
        raise NotImplementedError()

    @abc.abstractmethod
    def current_task_id(self) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    def processors(self) -> List[ProcessorHolder]:
        raise NotImplementedError()

    @abc.abstractmethod
    def num_suspended_processors(self) -> int:
        raise NotImplementedError()

    @abc.abstractmethod
    def task_lock(self) -> bool:
        raise NotImplementedError()


class ProfilingManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_process_start(self, pid: int):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_process_end(self, pid: int):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_start(self, pid: int, task_id: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_end(self, pid: int, task_id: bytes) -> ProfileResult:
        raise NotImplementedError()


class ObjectTracker(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_object_request(self, processor_id: bytes, object_request: ObjectRequest) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_object_response(self, object_response: ObjectResponse) -> Set[bytes]:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_object_instruction(self, object_instruction: ObjectInstruction) -> Dict[bytes, ObjectInstruction]:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_processor_end(self, processor_id: bytes) -> None:
        raise NotImplementedError()
