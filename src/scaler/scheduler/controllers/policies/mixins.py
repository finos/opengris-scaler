import abc
from typing import Dict, List, Optional, Set

from scaler.protocol.python.message import InformationSnapshot, Task, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerGroupCapabilities,
    WorkerGroupState,
    WorkerManagerSnapshot,
)
from scaler.utility.identifiers import TaskID, WorkerID


class ScalerPolicy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: WorkerID, capabilities: Dict[str, int], queue_size: int) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_worker(self, worker: WorkerID) -> List[TaskID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> Set[WorkerID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def balance(self) -> Dict[WorkerID, List[TaskID]]:
        raise NotImplementedError()

    @abc.abstractmethod
    def assign_task(self, task: Task) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_task(self, task_id: TaskID) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self, capabilities: Optional[Dict[str, int]] = None) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def statistics(self) -> Dict:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_scaling_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        raise NotImplementedError()
