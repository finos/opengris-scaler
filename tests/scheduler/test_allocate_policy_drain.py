import unittest
from typing import Callable, List

from scaler.protocol.capnp import Task
from scaler.scheduler.controllers.policies.simple_policy.allocation.capability_allocate_policy import (
    CapabilityAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.even_load_allocate_policy import (
    EvenLoadAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.mixins import TaskAllocatePolicy
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

QUEUE_SIZE = 10

POLICY_FACTORIES: List[Callable[[], TaskAllocatePolicy]] = [CapabilityAllocatePolicy, EvenLoadAllocatePolicy]


def _task(name: str) -> Task:
    return Task(
        taskId=TaskID(name.encode()),
        source=ClientID(b"client"),
        metadata=b"",
        funcObjectId=b"",
        functionArgs=[],
        capabilities={},
    )


class TestAllocatePolicyDrain(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_drain_returns_held_tasks_and_stops_assignment(self) -> None:
        """A drained worker hands back its tasks and never receives another one."""
        for factory in POLICY_FACTORIES:
            with self.subTest(policy=factory.__name__):
                policy = factory()
                draining = WorkerID(b"draining")
                policy.add_worker(draining, {}, QUEUE_SIZE)
                held = [_task(f"held-{task_i}") for task_i in range(3)]
                for task in held:
                    self.assertEqual(policy.assign_task(task), draining)

                self.assertEqual(set(policy.drain_worker(draining)), {task.taskId for task in held})

                self.assertFalse(policy.has_available_worker())
                self.assertFalse(policy.assign_task(_task("new")).is_valid())

                serving = WorkerID(b"serving")
                policy.add_worker(serving, {}, QUEUE_SIZE)
                self.assertEqual(policy.assign_task(_task("new")), serving)

    def test_drained_worker_can_still_finish_and_leave(self) -> None:
        """remove_task and remove_worker keep working on a drained worker."""
        for factory in POLICY_FACTORIES:
            with self.subTest(policy=factory.__name__):
                policy = factory()
                worker = WorkerID(b"worker")
                policy.add_worker(worker, {}, QUEUE_SIZE)
                running, queued = _task("running"), _task("queued")
                policy.assign_task(running)
                policy.assign_task(queued)
                policy.drain_worker(worker)

                self.assertEqual(policy.remove_task(queued.taskId), worker)
                self.assertEqual(policy.remove_worker(worker), [running.taskId])
                self.assertEqual(policy.get_worker_ids(), set())

    def test_idle_drained_worker_does_not_receive_balanced_tasks(self) -> None:
        """An idle drained worker is not a balance target."""
        for factory in POLICY_FACTORIES:
            with self.subTest(policy=factory.__name__):
                policy = factory()
                busy = WorkerID(b"busy")
                policy.add_worker(busy, {}, QUEUE_SIZE)
                for task_i in range(5):
                    policy.assign_task(_task(f"task-{task_i}"))

                idle = WorkerID(b"idle")
                policy.add_worker(idle, {}, QUEUE_SIZE)
                policy.drain_worker(idle)

                self.assertEqual(policy.balance(), {})

    def test_drain_unknown_worker_returns_nothing(self) -> None:
        for factory in POLICY_FACTORIES:
            with self.subTest(policy=factory.__name__):
                self.assertEqual(factory().drain_worker(WorkerID(b"unknown")), [])
