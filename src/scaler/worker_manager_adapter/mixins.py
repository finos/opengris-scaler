import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Tuple

from scaler.protocol.capnp import ProcessorStatus, Task, TaskCancel, WorkerManagerCommandResponse
from scaler.utility.identifiers import TaskID

if TYPE_CHECKING:
    from scaler.worker_manager_adapter.task_manager import TaskManager

Status = WorkerManagerCommandResponse.Status


class ProcessorStatusProvider(ABC):
    @abstractmethod
    def set_task_manager(self, task_manager: "TaskManager") -> None: ...

    @abstractmethod
    def get_processor_statuses(self) -> List[ProcessorStatus]: ...


class TaskInputLoader(ABC):
    @abstractmethod
    async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]: ...

    @abstractmethod
    def register(self, load_task_inputs: Callable[[Task], Awaitable[Tuple[Any, List[Any]]]]) -> None: ...


class ExecutionBackend(ABC):
    @abstractmethod
    async def execute(self, task: Task) -> asyncio.Future: ...

    @abstractmethod
    async def on_cancel(self, task_cancel: TaskCancel) -> None: ...

    @abstractmethod
    def on_cleanup(self, task_id: TaskID) -> None: ...

    @abstractmethod
    async def routine(self) -> None: ...

    @abstractmethod
    def register(self, load_task_inputs: Callable[[Task], Awaitable[Tuple[Any, List[Any]]]]) -> None: ...


class WorkerProvisioner(ABC):
    @abstractmethod
    async def start_worker(self) -> Tuple[List[bytes], Status]: ...

    @abstractmethod
    async def shutdown_workers(self, worker_ids: List[bytes]) -> Tuple[List[bytes], Status]: ...
