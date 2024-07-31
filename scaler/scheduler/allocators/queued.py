import math
from typing import Dict, List, Optional, Set

from scaler.scheduler.allocators.mixins import TaskAllocator
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.utility.queues.indexed_queue import IndexedQueue


class QueuedAllocator(TaskAllocator):
    def __init__(self, max_tasks_per_worker: int):
        self._max_tasks_per_worker = max_tasks_per_worker
        self._workers_to_task_ids: Dict[bytes, IndexedQueue] = dict()
        self._task_id_to_worker: Dict[bytes, bytes] = {}

        self._worker_queue: AsyncPriorityQueue = AsyncPriorityQueue()

    async def add_worker(self, worker: bytes) -> bool:
        if worker in self._workers_to_task_ids:
            return False

        self._workers_to_task_ids[worker] = IndexedQueue()
        await self._worker_queue.put([0, worker])
        return True

    def remove_worker(self, worker: bytes) -> List[bytes]:
        if worker not in self._workers_to_task_ids:
            return []

        self._worker_queue.remove(worker)

        task_ids = list(self._workers_to_task_ids.pop(worker))
        for task_id in task_ids:
            self._task_id_to_worker.pop(task_id)
        return task_ids

    def get_worker_ids(self) -> Set[bytes]:
        return set(self._workers_to_task_ids.keys())

    def get_worker_by_task_id(self, task_id: bytes) -> bytes:
        return self._task_id_to_worker.get(task_id, b"")

    def balance(self) -> Dict[bytes, List[bytes]]:
        """Returns, for every worker, the list of tasks to balance out."""

        balance_count = self.__get_balance_count_by_worker()

        balance_result = {}

        for worker, count in balance_count.items():
            if count == 0:
                continue

            tasks = list(self._workers_to_task_ids[worker])
            balance_result[worker] = tasks[-count:]  # balance out the most recently queued tasks

        return balance_result

    def __get_balance_count_by_worker(self) -> Dict[bytes, int]:
        """Returns, for every worker, the number of tasks to balance out."""

        queued_tasks_per_worker = {
            worker: max(0, len(tasks) - 1) for worker, tasks in self._workers_to_task_ids.items()
        }

        any_worker_has_queued_task = any(queued_tasks_per_worker.values())

        if not any_worker_has_queued_task:
            return {}

        number_of_idle_workers = sum(1 for tasks in self._workers_to_task_ids.values() if len(tasks) == 0)

        if number_of_idle_workers == 0:
            return {}

        mean_queued = math.ceil(sum(queued_tasks_per_worker.values()) / len(queued_tasks_per_worker))

        balance_count = {worker: max(0, count - mean_queued) for worker, count in queued_tasks_per_worker.items()}

        over_mean_advice_total = sum(balance_count.values())
        minimal_allocate = min(number_of_idle_workers, sum(queued_tasks_per_worker.values()))

        if over_mean_advice_total >= minimal_allocate:
            return balance_count

        total_to_be_balance = minimal_allocate - over_mean_advice_total
        for worker, count in queued_tasks_per_worker.items():
            assert total_to_be_balance >= 0, "total_to_be_balance must be positive"
            if total_to_be_balance == 0:
                break

            leftover = count - balance_count[worker]
            if leftover < 1:
                continue

            to_to_balance = min(leftover, total_to_be_balance)
            balance_count[worker] += to_to_balance
            total_to_be_balance -= to_to_balance

        return balance_count

    async def assign_task(self, task_id: bytes) -> Optional[bytes]:
        if task_id in self._task_id_to_worker:
            return self._task_id_to_worker[task_id]

        count, worker = await self._worker_queue.get()
        if count == self._max_tasks_per_worker:
            await self._worker_queue.put([count, worker])
            return None

        self._workers_to_task_ids[worker].put(task_id)
        self._task_id_to_worker[task_id] = worker
        await self._worker_queue.put([count + 1, worker])
        return worker

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_id_to_worker:
            return None

        worker = self._task_id_to_worker.pop(task_id)
        self._workers_to_task_ids[worker].remove(task_id)

        self._worker_queue.decrease_priority(worker)
        return worker

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_id_to_worker:
            return None

        return self._task_id_to_worker[task_id]

    def has_available_worker(self) -> bool:
        if not len(self._worker_queue):
            return False

        count = self._worker_queue.max_priority()
        if count == self._max_tasks_per_worker:
            return False

        return True

    def statistics(self) -> Dict:
        return {
            worker: {"free": self._max_tasks_per_worker - len(tasks), "sent": len(tasks)}
            for worker, tasks in self._workers_to_task_ids.items()
        }
