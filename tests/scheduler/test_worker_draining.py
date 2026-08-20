import unittest
from typing import TYPE_CHECKING, List

from scaler.protocol.capnp import Task
from scaler.scheduler.controllers.policies.simple_policy.allocation.capability_allocate_policy import (
    CapabilityAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.even_load_allocate_policy import (
    EvenLoadAllocatePolicy,
)
from scaler.utility.identifiers import TaskID, WorkerID

QUEUE_SIZE = 10


def _task(name: bytes) -> Task:
    return Task(taskId=TaskID(name.ljust(16, b"\0")), capabilities={})


def _assign(policy, count: int) -> List[WorkerID]:
    return [policy.assign_task(_task(f"task{i}".encode())) for i in range(count)]


if TYPE_CHECKING:
    # The contract is mixed into a TestCase by each concrete subclass. Naming TestCase as the base
    # for type checking only keeps the assert methods visible without collecting the mixin itself.
    _ContractBase = unittest.TestCase
else:
    _ContractBase = object


class _DrainingPolicyContract(_ContractBase):
    """Behaviour every allocate policy owes a draining worker."""

    def make_policy(self):
        raise NotImplementedError()

    def setUp(self) -> None:
        self.policy = self.make_policy()
        self.draining = WorkerID(b"worker_draining_")
        self.serving = WorkerID(b"worker_serving__")
        self.policy.add_worker(self.draining, {}, QUEUE_SIZE)
        self.policy.add_worker(self.serving, {}, QUEUE_SIZE)

    def test_a_draining_worker_receives_no_new_task(self) -> None:
        self.policy.mark_worker_draining(self.draining)

        assigned = _assign(self.policy, 6)

        self.assertNotIn(self.draining, assigned)
        self.assertEqual(set(assigned), {self.serving})

    def test_marking_is_idempotent(self) -> None:
        self.assertTrue(self.policy.mark_worker_draining(self.draining))
        self.assertFalse(self.policy.mark_worker_draining(self.draining))

    def test_marking_an_unknown_worker_is_refused(self) -> None:
        self.assertFalse(self.policy.mark_worker_draining(WorkerID(b"never_connected_")))

    def test_evacuate_leaves_the_running_task_behind(self) -> None:
        # everything lands on the one worker, then it drains
        self.policy.mark_worker_draining(self.serving)
        assigned = _assign(self.policy, 4)
        self.assertEqual(set(assigned), {self.draining})

        evacuated = self.policy.evacuate_worker(self.draining)

        self.assertEqual(len(evacuated), 3)  # 4 assigned, the oldest is running

    def test_evacuate_an_idle_worker_returns_nothing(self) -> None:
        self.assertEqual(self.policy.evacuate_worker(self.draining), [])

    def test_evacuate_an_unknown_worker_returns_nothing(self) -> None:
        self.assertEqual(self.policy.evacuate_worker(WorkerID(b"never_connected_")), [])

    def test_the_cluster_still_schedules_when_a_worker_drains(self) -> None:
        """The regression this design exists to avoid.

        Expressing a drain as a capacity of zero wedges even_load: the drained worker sorts to the
        front of the candidate queue on an empty task list, and assign_task reads "the least loaded
        worker is full" as "the cluster is full", so nothing is ever scheduled again anywhere.
        """
        self.policy.mark_worker_draining(self.draining)

        self.assertTrue(self.policy.has_available_worker())
        assigned = _assign(self.policy, 3)
        self.assertTrue(all(worker.is_valid() for worker in assigned))

    def test_balance_never_targets_a_draining_worker(self) -> None:
        self.policy.mark_worker_draining(self.draining)
        _assign(self.policy, 5)

        advice = self.policy.balance()

        self.assertNotIn(self.draining, advice)

    def test_removing_a_worker_clears_its_drain_flag(self) -> None:
        self.policy.mark_worker_draining(self.draining)
        self.policy.remove_worker(self.draining)
        self.policy.add_worker(self.draining, {}, QUEUE_SIZE)

        self.assertTrue(self.policy.mark_worker_draining(self.draining))

    def test_every_worker_draining_is_survivable(self) -> None:
        self.policy.mark_worker_draining(self.draining)
        self.policy.mark_worker_draining(self.serving)

        self.assertFalse(self.policy.has_available_worker())
        self.assertEqual(self.policy.balance(), {})
        self.assertFalse(self.policy.assign_task(_task(b"orphan")).is_valid())


class TestEvenLoadDraining(_DrainingPolicyContract, unittest.TestCase):
    def make_policy(self):
        return EvenLoadAllocatePolicy()


class TestCapabilityDraining(_DrainingPolicyContract, unittest.TestCase):
    def make_policy(self):
        return CapabilityAllocatePolicy()


if __name__ == "__main__":
    unittest.main()
