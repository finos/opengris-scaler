import time
import unittest
from typing import Dict, Optional

from scaler.protocol.capnp import Task
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.types import WorkerManagerInfo
from scaler.scheduler.policies.scaling.capability import CapabilityScalingPolicy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID, WorkerManagerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.snapshot import InformationSnapshot


class TestCapabilityScalingPolicy(unittest.TestCase):
    """Unit tests for CapabilityScalingPolicy declarative emission."""

    def setUp(self):
        setup_logger()
        self.policy = CapabilityScalingPolicy()

    def _commands(self, snapshot, manager_id, managed, max_task_concurrency=10, capabilities=None):
        info = _create_worker_manager_info(manager_id, max_task_concurrency, capabilities)
        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos={manager_id: info},
            worker_count_by_manager={manager_id: len(managed)},
        )
        self.assertEqual(len(commands), 1)
        return commands[0]

    def test_no_tasks_emits_empty_requests(self):
        """No tasks: per-capset list is empty -> emits a command with no capset requests."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_id = WorkerManagerID(b"mgr")
        info = _create_worker_manager_info(manager_id)

        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos={manager_id: info},
            worker_count_by_manager={manager_id: 0},
        )

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 0)

    def test_one_capset_targets_at_least_one_worker(self):
        """A single capset with one task targets one worker for that capset."""
        task_id = TaskID.generate_task_id()
        snapshot = InformationSnapshot(tasks={task_id: _create_mock_task(task_id, {"gpu": 1})}, workers={})

        command = self._commands(snapshot, WorkerManagerID(b"mgr"), [], capabilities={"gpu": -1})

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        self.assertEqual(capabilities_to_dict(requests[0].capabilities), {"gpu": 1})
        self.assertEqual(requests[0].taskConcurrency, 1)

    def test_two_capsets_get_separate_requests(self):
        """Two capsets with tasks each: declarative has one request per capset."""
        gpu_id = TaskID.generate_task_id()
        tpu_id = TaskID.generate_task_id()
        tasks = {gpu_id: _create_mock_task(gpu_id, {"gpu": 1}), tpu_id: _create_mock_task(tpu_id, {"tpu": 1})}
        snapshot = InformationSnapshot(tasks=tasks, workers={})

        command = self._commands(snapshot, WorkerManagerID(b"mgr"), [], capabilities={"gpu": -1, "tpu": -1})

        requests = list(command.setDesiredTaskConcurrencyRequests)
        caps_in_requests = {frozenset(capabilities_to_dict(r.capabilities).keys()) for r in requests}
        self.assertEqual(len(requests), 2)
        self.assertIn(frozenset({"gpu"}), caps_in_requests)
        self.assertIn(frozenset({"tpu"}), caps_in_requests)

    def test_high_task_count_scales_per_capset(self):
        """Many tasks for one capset: desired = ceil(task_count / upper_task_ratio)."""
        tasks = {}
        for _ in range(20):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})

        command = self._commands(snapshot, WorkerManagerID(b"mgr"), [], capabilities={"gpu": -1})

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        # ceil(20 / 5) = 4
        self.assertEqual(requests[0].taskConcurrency, 4)

    def test_max_concurrency_clamps_per_capset(self):
        """Per-capset desired clamped by manager's max_task_concurrency."""
        tasks = {}
        for _ in range(50):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})

        command = self._commands(
            snapshot, WorkerManagerID(b"mgr"), [], max_task_concurrency=3, capabilities={"gpu": -1}
        )

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 3)

    def test_capability_desired_omits_empty_capset(self):
        """A capability set with zero observed tasks must not appear in the emitted requests."""
        # 1 gpu task; tpu capset has no tasks even if the manager advertises tpu.
        task_id = TaskID.generate_task_id()
        snapshot = InformationSnapshot(tasks={task_id: _create_mock_task(task_id, {"gpu": 1})}, workers={})

        command = self._commands(snapshot, WorkerManagerID(b"mgr"), [], capabilities={"gpu": -1, "tpu": -1})

        requests = list(command.setDesiredTaskConcurrencyRequests)
        capsets = [set(capabilities_to_dict(r.capabilities).keys()) for r in requests]
        self.assertIn({"gpu"}, capsets)
        self.assertNotIn({"tpu"}, capsets)

    def test_starts_worker_request_when_no_capable_workers_yet(self):
        """A task requiring a capability that no manager-side worker yet provides
        still produces a per-capset request so the manager can spawn one."""
        task_id = TaskID.generate_task_id()
        snapshot = InformationSnapshot(tasks={task_id: _create_mock_task(task_id, {"gpu": 1})}, workers={})
        # Manager advertises gpu so the request is serviceable; no managed workers yet.

        command = self._commands(snapshot, WorkerManagerID(b"mgr"), [], capabilities={"gpu": -1})

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        self.assertEqual(set(capabilities_to_dict(requests[0].capabilities).keys()), {"gpu"})
        self.assertEqual(requests[0].taskConcurrency, 1)


class TestCapabilityDeclarativeEquivalents(unittest.TestCase):
    """Declarative equivalents of the master-branch capability shutdown tests."""

    def setUp(self):
        setup_logger()
        self.policy = CapabilityScalingPolicy()

    def test_drain_capset_when_no_tasks(self):
        """With 3 connected workers and no tasks, the policy emits setDesired with an
        empty request list -- effective desired (0) differs from current (3) so the manager
        is told to drain (via extract_desired_count returning 0 for the empty list)."""
        managed = [WorkerID(f"w{i}".encode()) for i in range(3)]
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_id = WorkerManagerID(b"mgr")
        info = _create_worker_manager_info(manager_id, capabilities={"gpu": -1})

        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos={manager_id: info},
            worker_count_by_manager={manager_id: len(managed)},
        )

        self.assertEqual(len(commands), 1)
        # No per-capset requests because there are no tasks -- the manager interprets this
        # as "desired 0" and drains its workers.
        self.assertEqual(list(commands[0].setDesiredTaskConcurrencyRequests), [])


def _create_mock_task(task_id: TaskID, capabilities: dict) -> Task:
    client_id = ClientID.generate_client_id()
    return Task(
        taskId=task_id,
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities=capabilities,
    )


def _create_worker_manager_info(
    worker_manager_id: WorkerManagerID, max_task_concurrency: int = 10, capabilities: Optional[Dict[str, int]] = None
) -> WorkerManagerInfo:
    return WorkerManagerInfo(
        manager_id=worker_manager_id,
        source=b"test",
        last_seen_at=time.time(),
        capabilities=capabilities or {},
        max_task_concurrency=max_task_concurrency,
    )
