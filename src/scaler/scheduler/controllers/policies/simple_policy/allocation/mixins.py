import abc
from typing import Dict, List, Optional, Set

from scaler.protocol.capnp import Task
from scaler.utility.identifiers import TaskID, WorkerID


class TaskAllocatePolicy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: WorkerID, capabilities: Dict[str, int], queue_size: int) -> bool:
        """add worker to worker collection"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_worker(self, worker: WorkerID) -> List[TaskID]:
        """remove worker to worker collection, and return list of task_ids of removed worker"""
        raise NotImplementedError()

    @abc.abstractmethod
    def mark_worker_draining(self, worker: WorkerID) -> bool:
        """mark worker as draining so it receives no further tasks, return True the first time only

        A drain cannot be reversed, so there is no matching call to clear the flag. The worker keeps
        the tasks it is already running; use evacuate_worker to reclaim the ones it has queued.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def evacuate_worker(self, worker: WorkerID) -> List[TaskID]:
        """return the task_ids this worker has queued but not started, so they can be reassigned

        Unlike balance, this ignores load: every queued task of a draining worker is returned,
        whatever the rest of the cluster looks like.
        """
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
