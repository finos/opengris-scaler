import time
import unittest
from typing import Dict, Optional

from scaler.protocol.capnp import Resource, Task, WorkerHeartbeat
from scaler.scheduler.types import WorkerManagerInfo
from scaler.scheduler.policies.scaling.types import WorkerManagerBounds
from scaler.scheduler.policies.scaling.vanilla import (
    VanillaScalingPolicy,
    clamp_to_bounds,
    floor_held_worker_count,
    ratio_target,
)
from scaler.scheduler.policies.utility import create_scaling_policy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID, WorkerManagerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.snapshot import InformationSnapshot


class TestVanillaScalingPolicy(unittest.TestCase):
    """Unit tests for VanillaScalingPolicy declarative emission."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy({})

    def _single_request(self, snapshot, manager_id, managed, max_task_concurrency=10):
        info = _create_worker_manager_info(manager_id, max_task_concurrency)
        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos={manager_id: info},
            worker_count_by_manager={manager_id: len(managed)},
        )
        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        return requests[0]

    def test_idle_emits_desired_zero(self):
        """Empty state: ratio computes desired=0 -> policy unconditionally emits setDesired(0)."""
        snapshot = InformationSnapshot(tasks={}, workers={})

        request = self._single_request(snapshot, WorkerManagerID(b"mgr"), [])

        self.assertEqual(request.taskConcurrency, 0)

    def test_tasks_with_no_workers_targets_one(self):
        """Tasks present but no workers: desired = 1 to bootstrap."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        snapshot = InformationSnapshot(tasks=tasks, workers={})

        request = self._single_request(snapshot, WorkerManagerID(b"mgr"), [])

        self.assertEqual(request.taskConcurrency, 1)

    def test_high_task_ratio_targets_current_plus_one(self):
        """Task/worker ratio above upper threshold scales by +1 toward equilibrium."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(20)}
        managed = [WorkerID(b"w0")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=5) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)

        request = self._single_request(snapshot, WorkerManagerID(b"mgr"), managed, max_task_concurrency=10)

        self.assertEqual(request.taskConcurrency, 2)

    def test_no_tasks_with_workers_targets_zero(self):
        """No tasks but managers exist: desired drains to 0."""
        workers = {
            WorkerID(b"w0"): _create_mock_worker_heartbeat({}, queued_tasks=0),
            WorkerID(b"w1"): _create_mock_worker_heartbeat({}, queued_tasks=1),
            WorkerID(b"w2"): _create_mock_worker_heartbeat({}, queued_tasks=2),
        }
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks={}, workers=workers)

        request = self._single_request(snapshot, WorkerManagerID(b"mgr"), managed)

        self.assertEqual(request.taskConcurrency, 0)

    def test_few_tasks_with_many_workers_targets_min_keep(self):
        """Low task ratio shrinks toward ceil(tasks / upper_task_ratio) min_keep."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(10)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)

        request = self._single_request(snapshot, WorkerManagerID(b"mgr"), managed)

        # ceil(5 / 10) = 1 minimum to keep
        self.assertEqual(request.taskConcurrency, 1)

    def test_max_concurrency_clamps_then_emits(self):
        """Ratio asks for current+1 but cap clamps to current -> emits setDesired(current)."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(50)}
        managed = [WorkerID(b"w0"), WorkerID(b"w1")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=20) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)

        request = self._single_request(snapshot, WorkerManagerID(b"mgr"), managed, max_task_concurrency=2)

        # Ratio would say desired=3, cap clamps to 2; emits setDesired(2).
        self.assertEqual(request.taskConcurrency, 2)


class TestRatioTarget(unittest.TestCase):
    """A manager grows by one above 10 tasks per worker, holds in band, and shrinks below 1 task per worker."""

    def test_the_ratio_bands(self):
        # current, tasks, workers, floor_held -> target
        cases = (
            (0, 0, 0, 0, 0),  # idle
            (0, 5, 0, 0, 1),  # tasks and no workers: start one
            (1, 20, 1, 0, 2),  # ratio 20 > 10: grow by one
            (5, 15, 5, 0, 5),  # ratio 3 in band: hold
            (10, 5, 10, 0, 1),  # ratio 0.5 < 1: shrink to ceil(5 / 10)
            (3, 0, 3, 0, 0),  # no tasks: drain
        )
        for current, task_count, worker_count, floor_held, target in cases:
            with self.subTest(current=current, task_count=task_count, worker_count=worker_count):
                self.assertEqual(ratio_target(current, task_count, worker_count, floor_held), target)

    def test_workers_held_at_other_floors_count_as_supply_when_shrinking(self):
        # an idle pinned pool of 8 covers 5 tasks, so the scaling manager asks for nothing
        self.assertEqual(ratio_target(current=0, task_count=5, worker_count=8, floor_held=8), 0)
        # 40 tasks need 4 workers, 2 of which another manager's floor holds
        self.assertEqual(ratio_target(current=40, task_count=40, worker_count=46, floor_held=2), 2)

    def test_load_beyond_a_pinned_pool_still_grows_the_manager(self):
        self.assertEqual(ratio_target(current=0, task_count=100, worker_count=8, floor_held=8), 1)


class TestClampToBounds(unittest.TestCase):
    def test_bounds_and_the_advertised_maximum(self):
        # desired, bounds, advertised maximum -> clamped
        cases = (
            (0, WorkerManagerBounds(8, 8), 100, 8),  # a pinned manager holds its floor when idle
            (500, WorkerManagerBounds(3), 100, 3),  # the bounds' cap wins below the advertised maximum
            (500, WorkerManagerBounds(None), 4, 4),  # without a cap in the bounds, the advertised maximum caps
            (0, WorkerManagerBounds(8, 8), 4, 4),  # a floor above the advertised maximum asks for that maximum
            (500, WorkerManagerBounds(None, 2), -1, 500),  # no cap anywhere
            (0, WorkerManagerBounds(None, 2), -1, 2),  # a floor without a cap
        )
        for desired, bounds, advertised_max, clamped in cases:
            with self.subTest(desired=desired, bounds=bounds, advertised_max=advertised_max):
                self.assertEqual(clamp_to_bounds(desired, bounds, advertised_max), clamped)


class TestFloorHeldWorkerCount(unittest.TestCase):
    """Only the connected workers that other managers' floors hold count as supply."""

    def test_floor_held_workers(self):
        bounds = {
            WorkerManagerID(b"native"): WorkerManagerBounds(8, 8),
            WorkerManagerID(b"warm"): WorkerManagerBounds(10, 2),
        }
        # worker count by manager -> workers held for b"cloud"
        cases = (
            ({WorkerManagerID(b"native"): 8, WorkerManagerID(b"cloud"): 0}, 8),  # a full pinned pool
            (
                {WorkerManagerID(b"native"): 0, WorkerManagerID(b"cloud"): 10},
                0,
            ),  # a pool that is still starting supplies nothing
            (
                {WorkerManagerID(b"warm"): 6, WorkerManagerID(b"cloud"): 40},
                2,
            ),  # workers above a floor can shrink, so they do not count
            ({WorkerManagerID(b"native"): 8, WorkerManagerID(b"warm"): 6}, 10),  # floors add up
            ({WorkerManagerID(b"other"): 30}, 0),  # a manager without bounds has no floor
        )
        for worker_count_by_manager, held in cases:
            with self.subTest(worker_count_by_manager=worker_count_by_manager):
                self.assertEqual(
                    floor_held_worker_count(WorkerManagerID(b"cloud"), bounds, worker_count_by_manager), held
                )

    def test_a_manager_does_not_count_its_own_floor(self):
        bounds = {WorkerManagerID(b"native"): WorkerManagerBounds(8, 8)}
        self.assertEqual(
            floor_held_worker_count(WorkerManagerID(b"native"), bounds, {WorkerManagerID(b"native"): 8}), 0
        )


class TestVanillaBounds(unittest.TestCase):
    """Bounds reach a manager through the policy and the scaling string."""

    def setUp(self):
        setup_logger()

    def test_an_idle_pinned_pool_keeps_the_scaling_manager_at_zero_through_the_policy(self):
        policy = VanillaScalingPolicy(
            {WorkerManagerID(b"native"): WorkerManagerBounds(max_task_concurrency=8, min_task_concurrency=8)}
        )
        native_workers = {WorkerID(f"native-{i}".encode()): _create_mock_worker_heartbeat({}) for i in range(8)}
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        manager_infos = {
            manager_id: _create_worker_manager_info(manager_id, max_task_concurrency=100)
            for manager_id in (WorkerManagerID(b"native"), WorkerManagerID(b"cloud"))
        }
        worker_count_by_manager = {WorkerManagerID(b"native"): 8, WorkerManagerID(b"cloud"): 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=WorkerManagerID(b"cloud"),
            information_snapshot=InformationSnapshot(tasks=tasks, workers=native_workers),
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(commands[0].setDesiredTaskConcurrencyRequests[0].taskConcurrency, 0)

    def test_bounds_parse_from_the_scaling_string(self):
        """`vanilla[id:max[:min], ...]` sets each manager's bounds; an empty max leaves the cap to the manager."""
        policy = create_scaling_policy("vanilla[native-local:8:8, ecs-burst:100, warm::2]")
        assert isinstance(policy, VanillaScalingPolicy)
        self.assertEqual(
            policy._bounds,
            {
                WorkerManagerID(b"native-local"): WorkerManagerBounds(8, 8),
                WorkerManagerID(b"ecs-burst"): WorkerManagerBounds(100, 0),
                WorkerManagerID(b"warm"): WorkerManagerBounds(None, 2),
            },
        )

    def test_invalid_bounds_are_refused(self):
        for scaling in [
            "vanilla[]",
            "vanilla[native]",
            "vanilla[native:8:8",
            "vanilla[:8]",
            "vanilla[native:4:8]",
            "vanilla[native:-1]",
            "vanilla[native:many]",
            "vanilla[native:8:8:8]",
            "vanilla[native:8, native:4]",
            "capability[native:8]",
        ]:
            with self.subTest(scaling=scaling):
                with self.assertRaises(ValueError):
                    create_scaling_policy(scaling)


class TestVanillaDeclarativeEquivalents(unittest.TestCase):
    """Declarative equivalents of the master-branch vanilla shutdown tests."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy({})

    def test_drain_all_when_idle(self):
        """With workers connected and no tasks, the policy targets desired=0 and emits
        setDesired(0) so the manager can drain its workers."""
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=0) for i in range(4)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        manager_id = WorkerManagerID(b"mgr")
        info = _create_worker_manager_info(manager_id)

        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos={manager_id: info},
            worker_count_by_manager={manager_id: len(managed)},
        )

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 0)

    def test_shrink_to_ratio_floor(self):
        """With few tasks relative to workers (ratio below lower threshold), the policy
        targets ceil(tasks/upper_task_ratio) and emits setDesired with that smaller count."""
        # 5 tasks, 10 connected workers -> ratio 0.5 < 1; floor = max(1, ceil(5/10)) = 1.
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(10)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        manager_id = WorkerManagerID(b"mgr")
        info = _create_worker_manager_info(manager_id)

        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos={manager_id: info},
            worker_count_by_manager={manager_id: len(managed)},
        )

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 1)

    def test_no_action_when_ratio_is_in_band(self):
        """With the task/worker ratio inside [lower, upper], the policy targets the current
        worker count and emits setDesired(current) unconditionally."""
        # 15 tasks, 5 workers -> ratio 3, in [1, 10]; desired = current = 5.
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(15)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(5)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        manager_id = WorkerManagerID(b"mgr")
        info = _create_worker_manager_info(manager_id)

        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos={manager_id: info},
            worker_count_by_manager={manager_id: len(managed)},
        )

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 5)


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


def _create_mock_worker_heartbeat(capabilities: dict, queued_tasks: int = 0) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        agent=Resource(cpu=1, rss=1000000),
        rssFree=500000,
        queueSize=10,
        queuedTasks=queued_tasks,
        latencyMicroseconds=100,
        taskLock=False,
        processors=[],
        capabilities=capabilities,
        workerManagerID=WorkerManagerID(b"test"),
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
