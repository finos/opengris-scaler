import abc
from typing import Dict, List, Optional, Set

from scaler.protocol.python.message import Task
from scaler.utility.identifiers import TaskID, WorkerID


class TaskAllocatePolicy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def add_worker(self, worker: WorkerID, tags: Set[str], queue_size: int) -> bool:
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
    async def assign_task(self, task: Task) -> WorkerID:
        """assign task in allocator, return an invalid worker ID if available worker, otherwise will return worker been
        assigned to"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_task(self, task_id: TaskID) -> WorkerID:
        """remove task in allocator, return an invalid worker ID if it did not found any worker, otherwise will return
        worker associate with the removed task_id"""
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self, tags: Optional[Set[str]] = None) -> bool:
        """has available worker or not, possibly constrained to the provided task tags"""
        raise NotImplementedError()

    @abc.abstractmethod
    def statistics(self) -> Dict:
        raise NotImplementedError()
