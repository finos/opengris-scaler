import asyncio
import time
import unittest
from typing import Dict, List, Optional

from scaler.protocol.python.message import (
    InformationSnapshot,
    Task,
    WorkerHeartbeat,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
)
from scaler.protocol.python.status import Resource
from scaler.scheduler.controllers.policies.library.utility import create_policy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.waterfall import WaterfallScalingPolicy
from scaler.scheduler.controllers.policies.waterfall_v1.waterfall_v1_policy import WaterfallV1Policy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger


class TestWaterfallScalingPolicy(unittest.TestCase):
    """Unit tests for WaterfallScalingPolicy with stateless interface."""

    def setUp(self):
        setup_logger()
        self.rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"manager_b", max_task_concurrency=20),
        ]
        self.policy = WaterfallScalingPolicy(self.rules)

    def test_single_priority_scale_up(self):
        """Single manager with tasks and no workers should scale up."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=0)
        }

        commands = policy.get_scaling_commands(
            snapshot, heartbeat, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.StartWorkers)

    def test_priority_cascade_higher_priority_fills_first(self):
        """Lower-priority manager should not scale up while higher-priority has capacity."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        # Manager A (priority 1) has capacity remaining
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=3),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0),
        }

        # Heartbeat from manager_a: should scale up
        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = self.policy.get_scaling_commands(
            snapshot, heartbeat_a, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(commands_a[0].command, WorkerManagerCommandType.StartWorkers)

        # Heartbeat from manager_b: should NOT scale up (manager_a still has room)
        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = self.policy.get_scaling_commands(
            snapshot, heartbeat_b, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_b), 0)

    def test_overflow_to_lower_priority(self):
        """Lower-priority manager should scale up when higher-priority is at capacity."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        # Manager A at full capacity
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=10),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0),
        }

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands = self.policy.get_scaling_commands(
            snapshot, heartbeat_b, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.StartWorkers)

    def test_offline_manager_fallback(self):
        """Lower-priority manager should scale up when higher-priority is offline (absent from snapshots)."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        # Manager A is offline (cleaned up by WorkerManagerController, not in snapshots)
        manager_snapshots = {
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0)
        }

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands = self.policy.get_scaling_commands(
            snapshot, heartbeat_b, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.StartWorkers)

    def test_reverse_shutdown_order(self):
        """Higher-priority manager should not shut down while lower-priority still has workers."""
        workers = _create_workers(5, queued_tasks=0)
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        managed_worker_capabilities: Dict[str, int] = {}

        # Both managers have workers
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=3),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=2),
        }

        # Heartbeat from manager_b (lower priority): should shut down
        managed_worker_ids_b = [WorkerID(b"worker-3"), WorkerID(b"worker-4")]
        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = self.policy.get_scaling_commands(
            snapshot, heartbeat_b, managed_worker_ids_b, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(commands_b[0].command, WorkerManagerCommandType.ShutdownWorkers)

        # Heartbeat from manager_a (higher priority): should NOT shut down (B still has workers)
        managed_worker_ids_a = [WorkerID(b"worker-0"), WorkerID(b"worker-1"), WorkerID(b"worker-2")]
        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = self.policy.get_scaling_commands(
            snapshot, heartbeat_a, managed_worker_ids_a, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_a), 0)

    def test_manager_not_in_config(self):
        """Manager with unknown worker_manager_id should receive no commands."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}
        manager_snapshots = {b"unknown": _create_manager_snapshot(b"unknown", max_task_concurrency=10, worker_count=0)}

        heartbeat = _create_worker_manager_heartbeat(b"unknown", max_task_concurrency=10)
        commands = self.policy.get_scaling_commands(
            snapshot, heartbeat, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 0)

    def test_effective_capacity_min_config_and_heartbeat(self):
        """Effective capacity should be min(config max_task_concurrency, heartbeat max_task_concurrency)."""
        # Rule says max_task_concurrency=5, heartbeat says max_task_concurrency=3
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=5)]
        policy = WaterfallScalingPolicy(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})

        # Already at 3 workers (heartbeat limit)
        managed_worker_ids = [WorkerID(b"w1"), WorkerID(b"w2"), WorkerID(b"w3")]
        managed_worker_capabilities: Dict[str, int] = {}
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=3, worker_count=3)
        }

        heartbeat = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=3)
        commands = policy.get_scaling_commands(
            snapshot, heartbeat, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        # Should NOT scale up: at effective capacity (min(5, 3) = 3)
        self.assertEqual(len(commands), 0)

    def test_no_workers_no_tasks(self):
        """No tasks and no workers should return no commands."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}
        manager_snapshots = {b"manager_a": _create_manager_snapshot(b"manager_a")}

        heartbeat = _create_worker_manager_heartbeat(b"manager_a")
        commands = self.policy.get_scaling_commands(
            snapshot, heartbeat, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 0)

    def test_same_priority_concurrent_scaling(self):
        """Two managers at same priority should both be able to scale up concurrently."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=1, worker_manager_id=b"manager_b", max_task_concurrency=10),
        ]
        policy = WaterfallScalingPolicy(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=0),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=10, worker_count=0),
        }

        # Both should scale up
        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = policy.get_scaling_commands(
            snapshot, heartbeat_a, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(commands_a[0].command, WorkerManagerCommandType.StartWorkers)

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=10)
        commands_b = policy.get_scaling_commands(
            snapshot, heartbeat_b, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(commands_b[0].command, WorkerManagerCommandType.StartWorkers)

    def test_scale_down_least_busy_worker(self):
        """When shutting down, should select the worker with fewest queued tasks."""
        workers = {
            WorkerID(b"worker-busy"): _create_mock_worker_heartbeat(queued_tasks=5),
            WorkerID(b"worker-idle"): _create_mock_worker_heartbeat(queued_tasks=0),
        }
        snapshot = InformationSnapshot(tasks={}, workers=workers)

        managed_worker_ids = [WorkerID(b"worker-busy"), WorkerID(b"worker-idle")]
        managed_worker_capabilities: Dict[str, int] = {}

        # No lower-priority managers with workers
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=2)
        }

        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)

        heartbeat = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands = policy.get_scaling_commands(
            snapshot, heartbeat, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.ShutdownWorkers)
        self.assertEqual(commands[0].worker_ids, [bytes(WorkerID(b"worker-idle"))])

    def test_higher_priority_manager_never_seen(self):
        """If higher-priority manager was never seen, lower-priority should scale up."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        # Only manager_b in snapshots, manager_a never seen
        manager_snapshots = {
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0)
        }

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands = self.policy.get_scaling_commands(
            snapshot, heartbeat_b, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.StartWorkers)

    def test_shutdown_allowed_when_lower_priority_offline(self):
        """Higher-priority manager can shut down if lower-priority manager is offline (absent from snapshots)."""
        workers = _create_workers(3, queued_tasks=0)
        snapshot = InformationSnapshot(tasks={}, workers=workers)

        managed_worker_ids_a = [WorkerID(b"worker-0"), WorkerID(b"worker-1"), WorkerID(b"worker-2")]
        managed_worker_capabilities: Dict[str, int] = {}

        # Manager B is offline (cleaned up by WorkerManagerController, not in snapshots)
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=3)
        }

        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands = self.policy.get_scaling_commands(
            snapshot, heartbeat_a, managed_worker_ids_a, managed_worker_capabilities, manager_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.ShutdownWorkers)

    def test_exact_matching_with_runtime_ids(self):
        """Worker manager IDs like NAT|12345 should match rules with exact worker_manager_id."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"NAT|12345", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"ECS|67890", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        manager_snapshots = {
            b"NAT|12345": _create_manager_snapshot(b"NAT|12345", max_task_concurrency=10, worker_count=3),
            b"ECS|67890": _create_manager_snapshot(b"ECS|67890", max_task_concurrency=20, worker_count=0),
        }

        # Heartbeat from NAT manager: should scale up (still has capacity)
        heartbeat_nat = _create_worker_manager_heartbeat(b"NAT|12345", max_task_concurrency=10)
        commands_nat = policy.get_scaling_commands(
            snapshot, heartbeat_nat, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_nat), 1)
        self.assertEqual(commands_nat[0].command, WorkerManagerCommandType.StartWorkers)

        # Heartbeat from ECS manager: should NOT scale up (NAT still has room)
        heartbeat_ecs = _create_worker_manager_heartbeat(b"ECS|67890", max_task_concurrency=20)
        commands_ecs = policy.get_scaling_commands(
            snapshot, heartbeat_ecs, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_ecs), 0)

    def test_multiple_managers_same_priority(self):
        """Multiple worker managers at the same priority should all need to be at capacity before overflow."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"NAT|111", max_task_concurrency=10),
            WaterfallRule(priority=1, worker_manager_id=b"NAT|222", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"ECS|333", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        # Two NAT managers, both at capacity
        manager_snapshots = {
            b"NAT|111": _create_manager_snapshot(b"NAT|111", max_task_concurrency=10, worker_count=10),
            b"NAT|222": _create_manager_snapshot(b"NAT|222", max_task_concurrency=10, worker_count=10),
            b"ECS|333": _create_manager_snapshot(b"ECS|333", max_task_concurrency=20, worker_count=0),
        }

        # ECS manager should scale up since all NAT managers are at capacity
        heartbeat_ecs = _create_worker_manager_heartbeat(b"ECS|333", max_task_concurrency=20)
        commands = policy.get_scaling_commands(
            snapshot, heartbeat_ecs, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.StartWorkers)

    def test_blocked_when_any_higher_priority_has_room(self):
        """Lower priority should not scale up if any manager at a higher priority still has room."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"NAT|111", max_task_concurrency=10),
            WaterfallRule(priority=1, worker_manager_id=b"NAT|222", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"ECS|333", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        # One NAT manager full, another still has room
        manager_snapshots = {
            b"NAT|111": _create_manager_snapshot(b"NAT|111", max_task_concurrency=10, worker_count=10),
            b"NAT|222": _create_manager_snapshot(b"NAT|222", max_task_concurrency=10, worker_count=5),
            b"ECS|333": _create_manager_snapshot(b"ECS|333", max_task_concurrency=20, worker_count=0),
        }

        # ECS should NOT scale up — NAT|222 still has room
        heartbeat_ecs = _create_worker_manager_heartbeat(b"ECS|333", max_task_concurrency=20)
        commands = policy.get_scaling_commands(
            snapshot, heartbeat_ecs, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands), 0)


class TestWaterfallV1Policy(unittest.TestCase):
    """Unit tests for WaterfallV1Policy config parsing and scaling delegation."""

    def setUp(self):
        setup_logger()
        # EvenLoadAllocatePolicy creates an AsyncPriorityQueue which requires an event loop.
        # On Python 3.8, there is no implicit event loop in the main thread, so create one.
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def test_config_parsing_via_factory(self):
        """Verify the factory parses waterfall_v1 policy config correctly."""
        policy = create_policy("waterfall_v1", "1,manager_a,10\n2,manager_b,20")
        self.assertIsInstance(policy, WaterfallV1Policy)

    def test_config_parsing_with_comments(self):
        """Comments and blank lines should be ignored."""
        policy_content = "\n".join(
            [
                "#priority,worker_manager_id,max_task_concurrency",
                "1,manager_a,10",
                "",
                "2,manager_b,20  # overflow tier",
            ]
        )
        policy = WaterfallV1Policy(policy_content)
        self.assertIsInstance(policy, WaterfallV1Policy)

    def test_invalid_config_empty(self):
        """Empty policy content should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("")

    def test_invalid_config_comments_only(self):
        """Policy content with only comments should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("# just a comment\n# another comment")

    def test_invalid_config_wrong_field_count(self):
        """Lines with wrong number of fields should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("1,manager_a")

    def test_invalid_config_non_integer_priority(self):
        """Non-integer priority should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("high,manager_a,10")

    def test_invalid_config_non_integer_max_task_concurrency(self):
        """Non-integer max_task_concurrency should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("1,manager_a,many")

    def test_invalid_config_empty_worker_manager_id(self):
        """Empty worker_manager_id should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("1,,10")

    def test_invalid_config_duplicate_worker_manager_id(self):
        """Duplicate worker_manager_id should raise ValueError."""
        with self.assertRaisesRegex(ValueError, "duplicate worker_manager_id"):
            WaterfallV1Policy("1,mgr_a,10\n2,mgr_a,20")

    def test_policy_delegates_to_scaling_policy(self):
        """Policy controller should delegate scaling commands to its scaling policy."""
        policy = WaterfallV1Policy("1,manager_a,10\n2,manager_b,20")

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        managed_worker_ids: List[WorkerID] = []
        managed_worker_capabilities: Dict[str, int] = {}

        # Manager A heartbeat with manager snapshots showing A has capacity
        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=0)
        }
        commands_a = policy.get_scaling_commands(
            snapshot, heartbeat_a, managed_worker_ids, managed_worker_capabilities, manager_snapshots
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(commands_a[0].command, WorkerManagerCommandType.StartWorkers)

        # Manager B heartbeat: manager A still has room, so B should NOT scale up
        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        manager_snapshots_with_both = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=0),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0),
        }
        commands_b = policy.get_scaling_commands(
            snapshot, heartbeat_b, managed_worker_ids, managed_worker_capabilities, manager_snapshots_with_both
        )
        self.assertEqual(len(commands_b), 0)

    def test_scaling_status(self):
        """get_scaling_status should return a ScalingManagerStatus."""
        policy = WaterfallV1Policy("1,manager_a,10")

        from scaler.protocol.python.status import ScalingManagerStatus

        managed_workers = {b"mgr-1": [WorkerID(b"worker-1")]}
        status = policy.get_scaling_status(managed_workers)
        self.assertIsInstance(status, ScalingManagerStatus)


def _create_mock_task(task_id: TaskID, capabilities: Optional[Dict[str, int]] = None) -> Task:
    client_id = ClientID.generate_client_id()
    return Task.new_msg(
        task_id=task_id,
        source=client_id,
        metadata=b"",
        func_object_id=ObjectID.generate_object_id(client_id),
        function_args=[],
        capabilities=capabilities or {},
    )


def _create_mock_worker_heartbeat(queued_tasks: int = 0) -> WorkerHeartbeat:
    return WorkerHeartbeat.new_msg(
        agent=Resource.new_msg(cpu=1, rss=1000000),
        rss_free=500000,
        queue_size=10,
        queued_tasks=queued_tasks,
        latency_us=100,
        task_lock=False,
        processors=[],
        capabilities={},
        worker_manager_id=b"test",
    )


def _create_worker_manager_heartbeat(
    worker_manager_id: bytes, max_task_concurrency: int = 10
) -> WorkerManagerHeartbeat:
    return WorkerManagerHeartbeat.new_msg(
        max_task_concurrency=max_task_concurrency, capabilities={}, worker_manager_id=worker_manager_id
    )


def _create_manager_snapshot(
    worker_manager_id: bytes, max_task_concurrency: int = 10, worker_count: int = 0, last_seen: Optional[float] = None
) -> WorkerManagerSnapshot:
    return WorkerManagerSnapshot(
        worker_manager_id=worker_manager_id,
        max_task_concurrency=max_task_concurrency,
        worker_count=worker_count,
        last_seen_s=last_seen if last_seen is not None else time.time(),
    )


def _create_tasks(count: int) -> Dict[TaskID, Task]:
    tasks = {}
    for _ in range(count):
        task_id = TaskID.generate_task_id()
        tasks[task_id] = _create_mock_task(task_id)
    return tasks


def _create_workers(count: int, queued_tasks: int = 0) -> Dict[WorkerID, WorkerHeartbeat]:
    workers = {}
    for i in range(count):
        worker_id = WorkerID(f"worker-{i}".encode())
        workers[worker_id] = _create_mock_worker_heartbeat(queued_tasks)
    return workers
