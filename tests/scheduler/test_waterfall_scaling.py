import asyncio
import time
import unittest
from typing import Dict, Optional

from scaler.protocol.python.message import (
    InformationSnapshot,
    Task,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
    WorkerHeartbeat,
)
from scaler.protocol.python.status import Resource
from scaler.scheduler.controllers.policies.advance_policy.advance_policy import AdvancePolicy
from scaler.scheduler.controllers.policies.advance_policy.scaling.types import WaterfallRule
from scaler.scheduler.controllers.policies.advance_policy.scaling.waterfall import WaterfallScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerAdapterSnapshot,
    WorkerGroupCapabilities,
    WorkerGroupState,
)
from scaler.scheduler.controllers.policies.utility import create_scaler_policy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger


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
    )


def _create_adapter_heartbeat(adapter_id: bytes, max_worker_groups: int = 10) -> WorkerAdapterHeartbeat:
    return WorkerAdapterHeartbeat.new_msg(
        max_worker_groups=max_worker_groups, workers_per_group=1, capabilities={}, worker_adapter_id=adapter_id
    )


def _create_adapter_snapshot(
    adapter_id: bytes, max_groups: int = 10, group_count: int = 0, last_seen: Optional[float] = None
) -> WorkerAdapterSnapshot:
    return WorkerAdapterSnapshot(
        worker_adapter_id=adapter_id,
        max_worker_groups=max_groups,
        worker_group_count=group_count,
        last_seen=last_seen if last_seen is not None else time.time(),
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


class TestWaterfallScalingController(unittest.TestCase):
    """Unit tests for WaterfallScalingController with stateless interface."""

    def setUp(self):
        setup_logger()
        self.rules = [
            WaterfallRule(priority=1, worker_adapter_id=b"adapter_a", max_workers=10),
            WaterfallRule(priority=2, worker_adapter_id=b"adapter_b", max_workers=20),
        ]
        self.controller = WaterfallScalingController(self.rules)

    def test_single_priority_scale_up(self):
        """Single adapter with tasks and no workers should scale up."""
        rules = [WaterfallRule(priority=1, worker_adapter_id=b"adapter_a", max_workers=10)]
        controller = WaterfallScalingController(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=10)
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}
        adapter_snapshots = {b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=0)}

        commands = controller.get_scaling_commands(
            snapshot, heartbeat, worker_groups, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)

    def test_priority_cascade_higher_priority_fills_first(self):
        """Lower-priority adapter should not scale up while higher-priority has capacity."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        # Adapter A (priority 1) has capacity remaining
        adapter_snapshots = {
            b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=3),
            b"adapter_b": _create_adapter_snapshot(b"adapter_b", max_groups=20, group_count=0),
        }

        # Heartbeat from adapter_a: should scale up
        heartbeat_a = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=10)
        commands_a = self.controller.get_scaling_commands(
            snapshot, heartbeat_a, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(commands_a[0].command, WorkerAdapterCommandType.StartWorkerGroup)

        # Heartbeat from adapter_b: should NOT scale up (adapter_a still has room)
        heartbeat_b = _create_adapter_heartbeat(b"adapter_b", max_worker_groups=20)
        commands_b = self.controller.get_scaling_commands(
            snapshot, heartbeat_b, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_b), 0)

    def test_overflow_to_lower_priority(self):
        """Lower-priority adapter should scale up when higher-priority is at capacity."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        # Adapter A at full capacity
        adapter_snapshots = {
            b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=10),
            b"adapter_b": _create_adapter_snapshot(b"adapter_b", max_groups=20, group_count=0),
        }

        heartbeat_b = _create_adapter_heartbeat(b"adapter_b", max_worker_groups=20)
        commands = self.controller.get_scaling_commands(
            snapshot, heartbeat_b, worker_groups, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)

    def test_offline_adapter_fallback(self):
        """Lower-priority adapter should scale up when higher-priority is offline (absent from snapshots)."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        # Adapter A is offline (cleaned up by WorkerAdapterController, not in snapshots)
        adapter_snapshots = {b"adapter_b": _create_adapter_snapshot(b"adapter_b", max_groups=20, group_count=0)}

        heartbeat_b = _create_adapter_heartbeat(b"adapter_b", max_worker_groups=20)
        commands = self.controller.get_scaling_commands(
            snapshot, heartbeat_b, worker_groups, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)

    def test_reverse_shutdown_order(self):
        """Higher-priority adapter should not shut down while lower-priority still has workers."""
        workers = _create_workers(5, queued_tasks=0)
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        worker_group_caps: WorkerGroupCapabilities = {}

        # Both adapters have workers
        adapter_snapshots = {
            b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=3),
            b"adapter_b": _create_adapter_snapshot(b"adapter_b", max_groups=20, group_count=2),
        }

        # Heartbeat from adapter_b (lower priority): should shut down
        worker_groups_b: WorkerGroupState = {b"wg-b-1": [WorkerID(b"worker-3")], b"wg-b-2": [WorkerID(b"worker-4")]}
        heartbeat_b = _create_adapter_heartbeat(b"adapter_b", max_worker_groups=20)
        commands_b = self.controller.get_scaling_commands(
            snapshot, heartbeat_b, worker_groups_b, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(commands_b[0].command, WorkerAdapterCommandType.ShutdownWorkerGroup)

        # Heartbeat from adapter_a (higher priority): should NOT shut down (B still has workers)
        worker_groups_a: WorkerGroupState = {
            b"wg-a-1": [WorkerID(b"worker-0")],
            b"wg-a-2": [WorkerID(b"worker-1")],
            b"wg-a-3": [WorkerID(b"worker-2")],
        }
        heartbeat_a = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=10)
        commands_a = self.controller.get_scaling_commands(
            snapshot, heartbeat_a, worker_groups_a, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_a), 0)

    def test_adapter_not_in_config(self):
        """Adapter with unknown worker_adapter_id should receive no commands."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}
        adapter_snapshots = {b"unknown": _create_adapter_snapshot(b"unknown", max_groups=10, group_count=0)}

        heartbeat = _create_adapter_heartbeat(b"unknown", max_worker_groups=10)
        commands = self.controller.get_scaling_commands(
            snapshot, heartbeat, worker_groups, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 0)

    def test_effective_capacity_min_config_and_heartbeat(self):
        """Effective capacity should be min(config max_workers, heartbeat max_worker_groups)."""
        # Rule says max_workers=5, heartbeat says max_worker_groups=3
        rules = [WaterfallRule(priority=1, worker_adapter_id=b"adapter_a", max_workers=5)]
        controller = WaterfallScalingController(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})

        # Already at 3 worker groups (heartbeat limit)
        worker_groups: WorkerGroupState = {
            b"wg-1": [WorkerID(b"w1")],
            b"wg-2": [WorkerID(b"w2")],
            b"wg-3": [WorkerID(b"w3")],
        }
        worker_group_caps: WorkerGroupCapabilities = {}
        adapter_snapshots = {b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=3, group_count=3)}

        heartbeat = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=3)
        commands = controller.get_scaling_commands(
            snapshot, heartbeat, worker_groups, worker_group_caps, adapter_snapshots
        )

        # Should NOT scale up: at effective capacity (min(5, 3) = 3)
        self.assertEqual(len(commands), 0)

    def test_no_workers_no_tasks(self):
        """No tasks and no workers should return no commands."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}
        adapter_snapshots = {b"adapter_a": _create_adapter_snapshot(b"adapter_a")}

        heartbeat = _create_adapter_heartbeat(b"adapter_a")
        commands = self.controller.get_scaling_commands(
            snapshot, heartbeat, worker_groups, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 0)

    def test_same_priority_concurrent_scaling(self):
        """Two adapters at same priority should both be able to scale up concurrently."""
        rules = [
            WaterfallRule(priority=1, worker_adapter_id=b"adapter_a", max_workers=10),
            WaterfallRule(priority=1, worker_adapter_id=b"adapter_b", max_workers=10),
        ]
        controller = WaterfallScalingController(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}
        adapter_snapshots = {
            b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=0),
            b"adapter_b": _create_adapter_snapshot(b"adapter_b", max_groups=10, group_count=0),
        }

        # Both should scale up
        heartbeat_a = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=10)
        commands_a = controller.get_scaling_commands(
            snapshot, heartbeat_a, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(commands_a[0].command, WorkerAdapterCommandType.StartWorkerGroup)

        heartbeat_b = _create_adapter_heartbeat(b"adapter_b", max_worker_groups=10)
        commands_b = controller.get_scaling_commands(
            snapshot, heartbeat_b, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(commands_b[0].command, WorkerAdapterCommandType.StartWorkerGroup)

    def test_scale_down_least_busy_group(self):
        """When shutting down, should select the worker group with fewest queued tasks."""
        workers = {
            WorkerID(b"worker-busy"): _create_mock_worker_heartbeat(queued_tasks=5),
            WorkerID(b"worker-idle"): _create_mock_worker_heartbeat(queued_tasks=0),
        }
        snapshot = InformationSnapshot(tasks={}, workers=workers)

        worker_groups: WorkerGroupState = {
            b"wg-busy": [WorkerID(b"worker-busy")],
            b"wg-idle": [WorkerID(b"worker-idle")],
        }
        worker_group_caps: WorkerGroupCapabilities = {}

        # No lower-priority adapters with workers
        adapter_snapshots = {b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=2)}

        rules = [WaterfallRule(priority=1, worker_adapter_id=b"adapter_a", max_workers=10)]
        controller = WaterfallScalingController(rules)

        heartbeat = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=10)
        commands = controller.get_scaling_commands(
            snapshot, heartbeat, worker_groups, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.ShutdownWorkerGroup)
        self.assertEqual(commands[0].worker_group_id, b"wg-idle")

    def test_higher_priority_adapter_never_seen(self):
        """If higher-priority adapter was never seen, lower-priority should scale up."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        # Only adapter_b in snapshots, adapter_a never seen
        adapter_snapshots = {b"adapter_b": _create_adapter_snapshot(b"adapter_b", max_groups=20, group_count=0)}

        heartbeat_b = _create_adapter_heartbeat(b"adapter_b", max_worker_groups=20)
        commands = self.controller.get_scaling_commands(
            snapshot, heartbeat_b, worker_groups, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)

    def test_shutdown_allowed_when_lower_priority_offline(self):
        """Higher-priority adapter can shut down if lower-priority adapter is offline (absent from snapshots)."""
        workers = _create_workers(3, queued_tasks=0)
        snapshot = InformationSnapshot(tasks={}, workers=workers)

        worker_groups_a: WorkerGroupState = {
            b"wg-a-1": [WorkerID(b"worker-0")],
            b"wg-a-2": [WorkerID(b"worker-1")],
            b"wg-a-3": [WorkerID(b"worker-2")],
        }
        worker_group_caps: WorkerGroupCapabilities = {}

        # Adapter B is offline (cleaned up by WorkerAdapterController, not in snapshots)
        adapter_snapshots = {b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=3)}

        heartbeat_a = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=10)
        commands = self.controller.get_scaling_commands(
            snapshot, heartbeat_a, worker_groups_a, worker_group_caps, adapter_snapshots
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.ShutdownWorkerGroup)

    def test_prefix_matching_with_runtime_ids(self):
        """Adapter IDs like NAT|<pid> should match rules with prefix NAT."""
        rules = [
            WaterfallRule(priority=1, worker_adapter_id=b"NAT", max_workers=10),
            WaterfallRule(priority=2, worker_adapter_id=b"ECS", max_workers=20),
        ]
        controller = WaterfallScalingController(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        adapter_snapshots = {
            b"NAT|12345": _create_adapter_snapshot(b"NAT|12345", max_groups=10, group_count=3),
            b"ECS|67890": _create_adapter_snapshot(b"ECS|67890", max_groups=20, group_count=0),
        }

        # Heartbeat from NAT adapter: should scale up (still has capacity)
        heartbeat_nat = _create_adapter_heartbeat(b"NAT|12345", max_worker_groups=10)
        commands_nat = controller.get_scaling_commands(
            snapshot, heartbeat_nat, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_nat), 1)
        self.assertEqual(commands_nat[0].command, WorkerAdapterCommandType.StartWorkerGroup)

        # Heartbeat from ECS adapter: should NOT scale up (NAT still has room)
        heartbeat_ecs = _create_adapter_heartbeat(b"ECS|67890", max_worker_groups=20)
        commands_ecs = controller.get_scaling_commands(
            snapshot, heartbeat_ecs, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_ecs), 0)

    def test_prefix_matching_multiple_adapters_same_prefix(self):
        """Multiple runtime adapters sharing a prefix should all match the same rule."""
        rules = [
            WaterfallRule(priority=1, worker_adapter_id=b"NAT", max_workers=10),
            WaterfallRule(priority=2, worker_adapter_id=b"ECS", max_workers=20),
        ]
        controller = WaterfallScalingController(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        # Two NAT adapters, both at capacity
        adapter_snapshots = {
            b"NAT|111": _create_adapter_snapshot(b"NAT|111", max_groups=10, group_count=10),
            b"NAT|222": _create_adapter_snapshot(b"NAT|222", max_groups=10, group_count=10),
            b"ECS|333": _create_adapter_snapshot(b"ECS|333", max_groups=20, group_count=0),
        }

        # ECS adapter should scale up since all NAT adapters are at capacity
        heartbeat_ecs = _create_adapter_heartbeat(b"ECS|333", max_worker_groups=20)
        commands = controller.get_scaling_commands(
            snapshot, heartbeat_ecs, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)

    def test_prefix_matching_blocked_when_any_higher_priority_has_room(self):
        """Lower priority should not scale up if any adapter matching a higher-priority prefix has room."""
        rules = [
            WaterfallRule(priority=1, worker_adapter_id=b"NAT", max_workers=10),
            WaterfallRule(priority=2, worker_adapter_id=b"ECS", max_workers=20),
        ]
        controller = WaterfallScalingController(rules)

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        # One NAT adapter full, another still has room
        adapter_snapshots = {
            b"NAT|111": _create_adapter_snapshot(b"NAT|111", max_groups=10, group_count=10),
            b"NAT|222": _create_adapter_snapshot(b"NAT|222", max_groups=10, group_count=5),
            b"ECS|333": _create_adapter_snapshot(b"ECS|333", max_groups=20, group_count=0),
        }

        # ECS should NOT scale up — NAT|222 still has room
        heartbeat_ecs = _create_adapter_heartbeat(b"ECS|333", max_worker_groups=20)
        commands = controller.get_scaling_commands(
            snapshot, heartbeat_ecs, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands), 0)


class TestAdvancePolicy(unittest.TestCase):
    """Unit tests for AdvancePolicy config parsing and delegation."""

    def setUp(self):
        setup_logger()
        # EvenLoadAllocatePolicy creates an AsyncPriorityQueue which requires an event loop.
        # On Python 3.8, there is no implicit event loop in the main thread, so create one.
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def test_config_parsing_via_factory(self):
        """Verify the factory parses advance policy config correctly."""
        policy = create_scaler_policy(
            "advance", "allocate=even_load; scaling=waterfall; 1,adapter_a,10; 2,adapter_b,20"
        )
        self.assertIsInstance(policy, AdvancePolicy)

    def test_invalid_config_missing_keys(self):
        """Missing allocate or scaling key should raise ValueError."""
        with self.assertRaises(ValueError):
            AdvancePolicy({"allocate": "even_load"}, "1,adapter_a,10")

    def test_policy_delegates_to_scaling_controller(self):
        """Policy should delegate scaling commands to its scaling controller."""
        policy = AdvancePolicy({"allocate": "even_load", "scaling": "waterfall"}, "1,adapter_a,10; 2,adapter_b,20")

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        worker_groups: WorkerGroupState = {}
        worker_group_caps: WorkerGroupCapabilities = {}

        # Adapter A heartbeat with adapter snapshots showing A has capacity
        heartbeat_a = _create_adapter_heartbeat(b"adapter_a", max_worker_groups=10)
        adapter_snapshots = {b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=0)}
        commands_a = policy.get_scaling_commands(
            snapshot, heartbeat_a, worker_groups, worker_group_caps, adapter_snapshots
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(commands_a[0].command, WorkerAdapterCommandType.StartWorkerGroup)

        # Adapter B heartbeat: adapter A still has room, so B should NOT scale up
        heartbeat_b = _create_adapter_heartbeat(b"adapter_b", max_worker_groups=20)
        adapter_snapshots_with_both = {
            b"adapter_a": _create_adapter_snapshot(b"adapter_a", max_groups=10, group_count=0),
            b"adapter_b": _create_adapter_snapshot(b"adapter_b", max_groups=20, group_count=0),
        }
        commands_b = policy.get_scaling_commands(
            snapshot, heartbeat_b, worker_groups, worker_group_caps, adapter_snapshots_with_both
        )
        self.assertEqual(len(commands_b), 0)

    def test_scaling_status(self):
        """get_scaling_status should return a ScalingManagerStatus."""
        policy = AdvancePolicy({"allocate": "even_load", "scaling": "waterfall"}, "1,adapter_a,10")

        from scaler.protocol.python.status import ScalingManagerStatus

        worker_groups: WorkerGroupState = {b"wg-1": [WorkerID(b"worker-1")]}
        status = policy.get_scaling_status(worker_groups)
        self.assertIsInstance(status, ScalingManagerStatus)
