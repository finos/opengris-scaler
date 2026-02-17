import abc
from typing import Dict, List, Optional, Set

from scaler.protocol.python.message import (
    InformationSnapshot,
    Task,
    WorkerAdapterCommandResponse,
    WorkerAdapterHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import CommandSender
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.mixins import Reporter


class ScalerPolicy(Reporter, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: WorkerID, capabilities: Dict[str, int], queue_size: int) -> bool:
        """add worker to worker collection"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_worker(self, worker: WorkerID) -> List[TaskID]:
        """remove worker to worker collection, and return list of task_ids of removed worker"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> Set[WorkerID]:
        """get all worker ids as list"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        """get worker that been assigned to this task_id, return an invalid worker ID if it cannot find the worker
        assigned to this task id"""
        raise NotImplementedError()

    @abc.abstractmethod
    def balance(self) -> Dict[WorkerID, List[TaskID]]:
        """balance worker, it should return list of task ids for over burdened worker, represented as worker
        identity to list of task ids dictionary"""
        raise NotImplementedError()

    @abc.abstractmethod
    def assign_task(self, task: Task) -> WorkerID:
        """assign task in allocator, return an invalid worker ID if available worker, otherwise will return worker been
        assigned to"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_task(self, task_id: TaskID) -> WorkerID:
        """remove task in allocator, return an invalid worker ID if it did not find any worker, otherwise will return
        worker associate with the removed task_id"""
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self, capabilities: Optional[Dict[str, int]] = None) -> bool:
        """has available worker or not, possibly constrained to the requested task capabilities"""
        raise NotImplementedError()

    @abc.abstractmethod
    def statistics(self) -> Dict:
        raise NotImplementedError()

    @abc.abstractmethod
    def register_command_sender(self, sender: CommandSender) -> None:
        """Register a callback function to send commands to the worker adapter."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_snapshot(
        self, information_snapshot: InformationSnapshot, adapter_heartbeat: WorkerAdapterHeartbeat
    ) -> None:
        """Process an information_snapshot and send scaling commands via the registered sender."""
        raise NotImplementedError()

    @abc.abstractmethod
    def on_command_response(self, response: WorkerAdapterCommandResponse) -> None:
        """Handle a response to a previously sent command. Updates internal state."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_scaling_status(self) -> ScalingManagerStatus:
        """Return the current scaling status."""
        raise NotImplementedError()
