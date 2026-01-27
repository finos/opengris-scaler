import unittest
from typing import Dict, Set

from scaler.protocol.python.message import Task
from scaler.scheduler.controllers.policies.simple_policy.allocation.capability_allocate_policy import (
    CapabilityAllocatePolicy,
)
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

MAX_TASKS_PER_WORKER = 10


class TestCapabilityAllocatePolicy(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_assign_task(self):
        allocator = CapabilityAllocatePolicy()

        regular_task = self.__create_task(TaskID(b"task_regular"), {})

        # No worker, should return an invalid worker ID
        assigned_worker = allocator.assign_task(regular_task)
        self.assertFalse(assigned_worker.is_valid())

        # Adds a bunch of workers
        worker_added = allocator.add_worker(WorkerID(b"worker_regular"), {}, MAX_TASKS_PER_WORKER)
        self.assertTrue(worker_added)
        worker_added = allocator.add_worker(WorkerID(b"worker_gpu"), {"gpu": -1}, MAX_TASKS_PER_WORKER)
        self.assertTrue(worker_added)

        self.assertEqual(allocator.get_worker_ids(), {WorkerID(b"worker_regular"), WorkerID(b"worker_gpu")})

        # Assign a task to the GPU worker
        gpu_task = self.__create_task(TaskID(b"task_gpu"), {"gpu": -1})
        assigned_worker = allocator.assign_task(gpu_task)
        self.assertEqual(assigned_worker, WorkerID(b"worker_gpu"))

        # Assign a task with a non-supported tag should fail
        mac_os_task = self.__create_task(TaskID(b"task_mac_os"), {"mac_os": -1})
        assigned_worker = allocator.assign_task(mac_os_task)
        self.assertFalse(assigned_worker.is_valid())

        # Assign a task without tag
        assigned_worker = allocator.assign_task(regular_task)
        self.assertEqual(assigned_worker, WorkerID(b"worker_regular"))

        # Assign should fail when the number of tasks exceeds MAX_TASKS_PER_WORKER

        for i in range(0, (MAX_TASKS_PER_WORKER * 2) - 2):
            self.assertTrue(allocator.has_available_worker())

            task = self.__create_task(TaskID(f"task_{i}".encode()), {})
            assigned_worker = allocator.assign_task(task)
            self.assertTrue(assigned_worker.is_valid())

        self.assertFalse(allocator.has_available_worker())

        overloaded_task = self.__create_task(TaskID(b"task_overload"), {})
        assigned_worker = allocator.assign_task(overloaded_task)
        self.assertFalse(assigned_worker.is_valid())

    def test_remove_task(self):
        allocator = CapabilityAllocatePolicy()

        allocator.add_worker(WorkerID(b"worker"), {}, MAX_TASKS_PER_WORKER)

        task = self.__create_task(TaskID(b"task_regular"), {})

        # Removing a non-assigned task returns an invalid Worker ID

        self.assertFalse(allocator.remove_task(task.task_id).is_valid())

        # Removing an assigned task returns the assigned worker

        assigned_worker = allocator.assign_task(task)
        self.assertTrue(assigned_worker, allocator.remove_task(task.task_id).is_valid())

        # Removing it again returns an invalid worker ID

        self.assertFalse(allocator.remove_task(task.task_id).is_valid())

    def test_remove_worker(self):
        N_TASKS = MAX_TASKS_PER_WORKER + 3

        allocator = CapabilityAllocatePolicy()

        allocator.add_worker(WorkerID(b"worker_1"), {}, MAX_TASKS_PER_WORKER)
        allocator.add_worker(WorkerID(b"worker_2"), {}, MAX_TASKS_PER_WORKER)

        # Adds a bunch of tasks

        worker_id_to_tasks: Dict[WorkerID, Set[TaskID]] = {WorkerID(b"worker_1"): set(), WorkerID(b"worker_2"): set()}

        for i in range(0, N_TASKS):
            task = self.__create_task(TaskID(f"task_{i}".encode()), {})

            assigned_worker = allocator.assign_task(task)
            self.assertTrue(assigned_worker.is_valid())

            worker_id_to_tasks[assigned_worker].add(task.task_id)

        # Tasks should be balanced between the two workers

        for workers_tasks in worker_id_to_tasks.values():
            self.assertAlmostEqual(len(workers_tasks), N_TASKS / 2, delta=1.0)

        # Removes the two workers

        worker_tasks = allocator.remove_worker(WorkerID(b"worker_1"))
        self.assertSetEqual(set(worker_tasks), worker_id_to_tasks[WorkerID(b"worker_1")])

        worker_tasks = allocator.remove_worker(WorkerID(b"worker_2"))
        self.assertSetEqual(set(worker_tasks), worker_id_to_tasks[WorkerID(b"worker_2")])

    def test_balancing(self):
        N_TASKS = MAX_TASKS_PER_WORKER // 2 * 2  # must be even

        n_workers = 0

        allocator = CapabilityAllocatePolicy()

        allocator.add_worker(WorkerID(b"worker_1"), {"linux": -1, "gpu": -1}, MAX_TASKS_PER_WORKER)
        n_workers += 1

        # Assign a few tasks

        for i in range(0, N_TASKS // 2):
            allocator.assign_task(self.__create_task(TaskID(f"gpu_task_{i}".encode()), {"gpu": -1}))
            allocator.assign_task(self.__create_task(TaskID(f"linux+gpu_task_{i}".encode()), {"linux": -1, "gpu": -1}))

        self.assertDictEqual(allocator.balance(), {})

        # Adds a worker that cannot accept the balanced tasks

        allocator.add_worker(WorkerID(b"worker_2"), {"windows": -1}, MAX_TASKS_PER_WORKER)
        n_workers += 1

        self.assertDictEqual(allocator.balance(), {})

        # Adds a worker that can accept some of the tasks

        allocator.add_worker(WorkerID(b"worker_3"), {"gpu": -1}, MAX_TASKS_PER_WORKER)
        n_workers += 1

        balancing_advice = allocator.balance()

        avg_tasks_per_worker = N_TASKS / n_workers

        self.assertListEqual(list(balancing_advice.keys()), [WorkerID(b"worker_1")])

        self.assertAlmostEqual(len(balancing_advice[WorkerID(b"worker_1")]), avg_tasks_per_worker, delta=1.0)
        self.assertSetEqual(
            set(balancing_advice[WorkerID(b"worker_1")]),
            {f"gpu_task_{(N_TASKS // 2) - i - 1}".encode() for i in range(0, int(avg_tasks_per_worker))},
            msg="youngest task should be advised first for balancing.",
        )

        # Adds a fourth worker that can accept all tasks

        allocator.add_worker(WorkerID(b"worker_4"), {"gpu": -1, "linux": -1}, MAX_TASKS_PER_WORKER)
        n_workers += 1

        balancing_advice = allocator.balance()

        avg_tasks_per_worker = N_TASKS / n_workers

        self.assertListEqual(list(balancing_advice.keys()), [WorkerID(b"worker_1")])
        self.assertAlmostEqual(len(balancing_advice[WorkerID(b"worker_1")]), avg_tasks_per_worker * 2, delta=1.0)

    @staticmethod
    def __create_task(task_id: TaskID, capabilities: Dict[str, int]) -> Task:
        return Task.new_msg(task_id, ClientID(b"client_id"), b"", None, [], capabilities)
