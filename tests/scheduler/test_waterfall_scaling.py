import asyncio
import time
import unittest
from typing import Dict, Optional

from scaler.config.defaults import TASKS_PER_WORKER
from scaler.protocol.capnp import Resource, Task, WorkerHeartbeat
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.policies.allocation.capability import CapabilityAllocatePolicy
from scaler.scheduler.policies.scaling.waterfall import WaterfallRule, WaterfallScalingPolicy, parse_waterfall_rules
from scaler.scheduler.policies.scaling.waterfall.policy import (
    allocate,
    lower_priority_tier_above_floor,
    target_worker_count,
)
from scaler.scheduler.policies.scaling.waterfall.types import Tier
from scaler.scheduler.policies.utility import create_policies
from scaler.scheduler.types import WorkerManagerInfo
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID, WorkerManagerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.snapshot import InformationSnapshot


def _generic_request(command):
    """Return the empty-capabilities (generic) DesiredTaskConcurrencyRequest from a command."""
    for r in command.setDesiredTaskConcurrencyRequests:
        if not capabilities_to_dict(r.capabilities):
            return r
    return None


def _capability_requests(command):
    """Return only capability-bearing requests as (caps_dict, taskConcurrency) tuples."""
    return [
        (capabilities_to_dict(r.capabilities), r.taskConcurrency)
        for r in command.setDesiredTaskConcurrencyRequests
        if capabilities_to_dict(r.capabilities)
    ]


class TestWaterfallScalingPolicy(unittest.TestCase):
    """Unit tests for declarative WaterfallScalingPolicy emission."""

    def setUp(self):
        setup_logger()
        self.manager_id_a = WorkerManagerID(b"manager_a")
        self.manager_id_b = WorkerManagerID(b"manager_b")
        self.rules = [
            WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=self.manager_id_b, max_task_concurrency=20),
        ]
        self.policy = WaterfallScalingPolicy(self.rules)

    def test_unknown_manager_emits_no_commands(self):
        """Manager with unknown worker_manager_id receives no scaling commands."""
        manager_id = WorkerManagerID(b"unknown")
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {manager_id: _create_worker_manager_info(manager_id, max_task_concurrency=10)}
        worker_count_by_manager = {manager_id: 0}

        commands = self.policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(len(commands), 0)

    def test_higher_priority_takes_full_load_when_within_capacity(self):
        """When tasks fit in the higher-priority manager's capacity, lower-priority desired is 0."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=10),
            self.manager_id_b: _create_worker_manager_info(self.manager_id_b, max_task_concurrency=20),
        }
        worker_count_by_manager = {self.manager_id_a: 0, self.manager_id_b: 0}

        commands_a = self.policy.get_scaling_commands(
            worker_manager_id=self.manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 1)

        commands_b = self.policy.get_scaling_commands(
            worker_manager_id=self.manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        # Manager B's share is 0 (chain consumes the whole 1 worker at manager_a); emits setDesired(0).
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)

    def test_overflow_to_lower_priority_when_higher_at_capacity(self):
        """When higher priority is at full capacity, the overflow lands on lower-priority desired."""
        # Many tasks (50) so total_desired = ceil(50/10) = 5 spread across managers.
        tasks = _create_tasks(150)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        # manager_a is full so its capacity (10) is consumed; remainder (5) goes to manager_b.
        manager_infos = {
            self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=10),
            self.manager_id_b: _create_worker_manager_info(self.manager_id_b, max_task_concurrency=20),
        }
        worker_count_by_manager = {self.manager_id_a: 10, self.manager_id_b: 0}

        commands_b = self.policy.get_scaling_commands(
            worker_manager_id=self.manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # total_desired = ceil(150/10) = 15. Manager_a's cap is 10 -> consumed 10. Remaining for B = 5.
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 5)

    def test_higher_priority_offline_routes_load_to_lower(self):
        """When the higher-priority manager is offline, a lower-priority manager picks up the load."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {self.manager_id_b: _create_worker_manager_info(self.manager_id_b, max_task_concurrency=20)}
        worker_count_by_manager = {self.manager_id_b: 0}

        commands_b = self.policy.get_scaling_commands(
            worker_manager_id=self.manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # Higher-priority is offline (absent) so its capacity contributes 0; lower picks up all of it.
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 1)

    def test_no_tasks_emits_desired_zero(self):
        """No tasks: desired is 0 -> policy unconditionally emits setDesired(0)."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_infos = {self.manager_id_a: _create_worker_manager_info(self.manager_id_a)}
        worker_count_by_manager = {self.manager_id_a: 0}

        commands = self.policy.get_scaling_commands(
            worker_manager_id=self.manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 0)

    def test_max_task_concurrency_clamps_via_heartbeat(self):
        """The manager's heartbeat-reported maxTaskConcurrency clamps its allocation."""
        rules = [WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=20)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(50)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=3)}
        worker_count_by_manager = {self.manager_id_a: 3}

        commands = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # min(rule cap 20, heartbeat cap 3) = 3 clamps the desired worker count.
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 3)

    def test_same_priority_share_capacity(self):
        """Two managers at the same priority each absorb a portion of the desired load."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=10),
            WaterfallRule(priority=1, worker_manager_id=self.manager_id_b, max_task_concurrency=10),
        ]
        policy = WaterfallScalingPolicy(rules)
        # 100 tasks -> total_desired = 10
        tasks = _create_tasks(100)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=10),
            self.manager_id_b: _create_worker_manager_info(self.manager_id_b, max_task_concurrency=10),
        }
        worker_count_by_manager = {self.manager_id_a: 0, self.manager_id_b: 0}

        # Sorting by priority is stable; with equal priority the iteration order is the rules' input order.
        commands_a = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 10)

        commands_b = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        # First manager consumed all of its capacity; nothing left for the second at same priority.
        # Desired is 0 -> emits setDesired(0).
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)

    def test_lower_priority_drains_first(self):
        """When demand drops to zero, the lower-priority manager drains first;
        the higher-priority manager holds its workers until the lower-priority manager is empty."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=self.manager_id_b, max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_infos = {
            self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=10),
            self.manager_id_b: _create_worker_manager_info(self.manager_id_b, max_task_concurrency=20),
        }
        worker_count_by_manager = {self.manager_id_a: 3, self.manager_id_b: 2}

        # manager_b (lower priority) is told to drain to 0
        commands_b = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)

        # manager_a (higher priority) holds its count (waiting for lower priority to drain) -> emits setDesired(3).
        commands_a = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 3)

    def test_higher_priority_can_drain_when_lower_offline(self):
        """If the lower-priority manager is offline (not in snapshots), the higher-priority
        manager is allowed to drain immediately -- there is no lower priority to wait on."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=self.manager_id_b, max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_infos = {self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=10)}
        worker_count_by_manager = {self.manager_id_a: 3}

        commands_a = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # No lower-priority manager to wait on -- emit setDesired(0) to drain.
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 0)

    def test_unknown_manager_id_returns_no_commands(self):
        """A heartbeat from a worker manager whose ID is not in the waterfall rules
        receives no scaling commands at all (not even an empty setDesired)."""
        rules = [WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        manager_id = WorkerManagerID(b"unknown")
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {manager_id: _create_worker_manager_info(manager_id, max_task_concurrency=10)}
        worker_count_by_manager = {manager_id: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(commands, [])

    def test_exact_matching_with_runtime_ids(self):
        """Worker manager IDs like NAT|12345 must match rules by exact bytes; a heartbeat
        from a manager whose ID matches a rule receives the allocation for that rule."""
        manager_id_nat = WorkerManagerID(b"NAT|12345")
        manager_id_ecs = WorkerManagerID(b"ECS|67890")
        rules = [
            WaterfallRule(priority=1, worker_manager_id=manager_id_nat, max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=manager_id_ecs, max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(50)  # ceil(50/10) = 5; first manager absorbs all of it
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            manager_id_nat: _create_worker_manager_info(manager_id_nat, max_task_concurrency=10),
            manager_id_ecs: _create_worker_manager_info(manager_id_ecs, max_task_concurrency=20),
        }
        worker_count_by_manager = {manager_id_nat: 0, manager_id_ecs: 0}

        # NAT manager gets the allocation since it has capacity and the priority chain
        # consumes it first.
        commands_nat = policy.get_scaling_commands(
            worker_manager_id=manager_id_nat,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(_generic_request(commands_nat[0]).taskConcurrency, 5)

        # ECS manager gets nothing since NAT has absorbed everything; desired=0 -> emits setDesired(0).
        commands_ecs = policy.get_scaling_commands(
            worker_manager_id=manager_id_ecs,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(len(commands_ecs), 1)
        self.assertEqual(_generic_request(commands_ecs[0]).taskConcurrency, 0)

    def test_blocked_when_any_higher_priority_has_room(self):
        """When multiple worker managers share the same higher priority, the lower-priority
        manager is allocated nothing until *all* higher-priority capacity is consumed."""
        manager_id_nat1 = WorkerManagerID(b"NAT|111")
        manager_id_nat2 = WorkerManagerID(b"NAT|222")
        manager_id_ecs = WorkerManagerID(b"ECS|333")
        rules = [
            WaterfallRule(priority=1, worker_manager_id=manager_id_nat1, max_task_concurrency=10),
            WaterfallRule(priority=1, worker_manager_id=manager_id_nat2, max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=manager_id_ecs, max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        # 5 tasks -> total_desired = ceil(5/10) = 1. Chain absorbs the 1 at NAT|111 entirely.
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            manager_id_nat1: _create_worker_manager_info(manager_id_nat1, max_task_concurrency=10),
            manager_id_nat2: _create_worker_manager_info(manager_id_nat2, max_task_concurrency=10),
            manager_id_ecs: _create_worker_manager_info(manager_id_ecs, max_task_concurrency=20),
        }
        worker_count_by_manager = {manager_id_nat1: 10, manager_id_nat2: 5, manager_id_ecs: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id_ecs,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # ECS allocation = 0 (higher-priority chain absorbs all demand); emits setDesired(0).
        self.assertEqual(len(commands), 1)
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 0)

    def test_greedy_shutdown_partial_with_tasks(self):
        """With a small task count and many workers, the policy targets the ratio-based
        minimum and emits a setDesired with that smaller count to drain excess workers."""
        rules = [WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=20)]
        policy = WaterfallScalingPolicy(rules)
        # 5 tasks; ratio yields ceil(5/10) = 1 desired total.
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=20)}
        worker_count_by_manager = {self.manager_id_a: 10}
        commands = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 1)

    def test_declarative_desired_zero_on_non_owning_manager(self):
        """When the priority chain has already absorbed all demand, a lower-priority manager
        that still has connected workers is told setDesired(0) for the generic capset."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=self.manager_id_a, max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=self.manager_id_b, max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            self.manager_id_a: _create_worker_manager_info(self.manager_id_a, max_task_concurrency=10),
            self.manager_id_b: _create_worker_manager_info(self.manager_id_b, max_task_concurrency=20),
        }
        worker_count_by_manager = {self.manager_id_a: 3, self.manager_id_b: 2}

        commands_b = policy.get_scaling_commands(
            worker_manager_id=self.manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # 5 tasks -> total_desired=1, absorbed by manager_a entirely -> manager_b's share=0;
        # but manager_b has 2 connected workers, so the command is not a no-op.
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)


class TestTargetWorkerCount(unittest.TestCase):
    def test_one_worker_per_ten_tasks_rounded_up(self):
        for task_count, target in ((0, 0), (1, 1), (10, 1), (11, 2), (200, 20)):
            with self.subTest(task_count=task_count):
                self.assertEqual(target_worker_count(task_count), target)


class TestAllocate(unittest.TestCase):
    """Floors count as supply first; the rest of the target fills tiers above their floors in priority order."""

    def test_a_floor_holds_with_no_demand(self):
        manager_id_native = WorkerManagerID(b"native")
        manager_id_cloud = WorkerManagerID(b"cloud")
        tiers = [
            _tier(manager_id_native, max_task_concurrency=8, min_task_concurrency=8),
            _tier(manager_id_cloud, max_task_concurrency=100),
        ]
        self.assertEqual(allocate(0, tiers), {manager_id_native: 8, manager_id_cloud: 0})

    def test_demand_under_a_floor_stays_on_the_floor_tier(self):
        manager_id_native = WorkerManagerID(b"native")
        manager_id_cloud = WorkerManagerID(b"cloud")
        tiers = [
            _tier(manager_id_native, max_task_concurrency=8, min_task_concurrency=8),
            _tier(manager_id_cloud, max_task_concurrency=100),
        ]
        self.assertEqual(allocate(5, tiers), {manager_id_native: 8, manager_id_cloud: 0})

    def test_demand_above_a_full_tier_spills_to_the_next(self):
        manager_id_native = WorkerManagerID(b"native")
        manager_id_cloud = WorkerManagerID(b"cloud")
        tiers = [
            _tier(manager_id_native, max_task_concurrency=8, min_task_concurrency=8),
            _tier(manager_id_cloud, max_task_concurrency=100),
        ]
        self.assertEqual(allocate(20, tiers), {manager_id_native: 8, manager_id_cloud: 12})

    def test_a_floor_below_the_cap_is_a_minimum(self):
        manager_id_native = WorkerManagerID(b"native")
        self.assertEqual(
            allocate(10, [_tier(manager_id_native, max_task_concurrency=20, min_task_concurrency=2)]),
            {manager_id_native: 10},
        )

    def test_a_floor_on_a_lower_tier_counts_as_supply_first(self):
        manager_id_burst = WorkerManagerID(b"burst")
        manager_id_warm = WorkerManagerID(b"warm")
        tiers = [
            _tier(manager_id_burst, max_task_concurrency=20),
            _tier(manager_id_warm, max_task_concurrency=20, min_task_concurrency=8),
        ]
        for target, burst, warm in ((5, 0, 8), (20, 12, 8), (30, 20, 10), (50, 20, 20)):
            with self.subTest(target=target):
                self.assertEqual(allocate(target, tiers), {manager_id_burst: burst, manager_id_warm: warm})


class TestDrainOrder(unittest.TestCase):
    def test_a_tier_holds_while_a_lower_priority_tier_is_above_its_floor(self):
        manager_id_warm = WorkerManagerID(b"warm")
        manager_id_burst = WorkerManagerID(b"burst")
        warm_draining = _tier(
            manager_id_warm, max_task_concurrency=8, min_task_concurrency=4, worker_count=6, priority=2
        )
        warm_at_floor = _tier(
            manager_id_warm, max_task_concurrency=8, min_task_concurrency=4, worker_count=4, priority=2
        )
        burst = _tier(manager_id_burst, max_task_concurrency=20, worker_count=20)
        self.assertTrue(lower_priority_tier_above_floor(burst, [burst, warm_draining]))
        self.assertFalse(lower_priority_tier_above_floor(burst, [burst, warm_at_floor]))

    def test_a_tier_of_the_same_priority_does_not_hold_it(self):
        manager_id_burst = WorkerManagerID(b"burst")
        manager_id_peer = WorkerManagerID(b"peer")
        burst = _tier(manager_id_burst, max_task_concurrency=20, worker_count=20)
        peer = _tier(manager_id_peer, max_task_concurrency=8, worker_count=6)
        self.assertFalse(lower_priority_tier_above_floor(burst, [burst, peer]))


class TestWaterfallFloor(unittest.TestCase):
    """The floor reaches a manager through the policy's heartbeat and snapshot handling."""

    def setUp(self):
        setup_logger()

    def test_spill_through_the_policy(self):
        manager_id_native = WorkerManagerID(b"native")
        manager_id_cloud = WorkerManagerID(b"cloud")
        policy = WaterfallScalingPolicy(
            [
                WaterfallRule(
                    priority=1, worker_manager_id=manager_id_native, max_task_concurrency=8, min_task_concurrency=8
                ),
                WaterfallRule(priority=2, worker_manager_id=manager_id_cloud, max_task_concurrency=100),
            ]
        )
        managers = {manager_id_native: 8, manager_id_cloud: 100}
        desired = _desired_by_manager(policy, task_count=20 * TASKS_PER_WORKER, advertised_max_by_manager=managers)
        self.assertEqual(desired, {manager_id_native: 8, manager_id_cloud: 12})

    def test_a_floor_is_clamped_to_the_advertised_maximum(self):
        manager_id_native = WorkerManagerID(b"native")
        policy = WaterfallScalingPolicy(
            [
                WaterfallRule(
                    priority=1, worker_manager_id=manager_id_native, max_task_concurrency=8, min_task_concurrency=8
                )
            ]
        )
        self.assertEqual(_desired_by_manager(policy, 0, {manager_id_native: 3}), {manager_id_native: 3})

    def test_a_floor_does_not_apply_to_capability_requests(self):
        """Only the generic request carries the floor; a capability request stays driven by demand."""
        manager_id_gpu = WorkerManagerID(b"gpu")
        policy = WaterfallScalingPolicy(
            [
                WaterfallRule(
                    priority=1, worker_manager_id=manager_id_gpu, max_task_concurrency=8, min_task_concurrency=4
                )
            ]
        )
        snapshot = InformationSnapshot(tasks=_create_tasks(5, capabilities={"gpu": 1}), workers={})
        manager_infos = {
            manager_id_gpu: _create_worker_manager_info(manager_id_gpu, max_task_concurrency=8, capabilities={"gpu": 1})
        }
        worker_count_by_manager = {manager_id_gpu: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id_gpu,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 4)
        self.assertEqual(_capability_requests(commands[0]), [({"gpu": 1}, 1)])


class TestWaterfallCapabilities(unittest.TestCase):
    """Capability-aware declarative emission for WaterfallScalingPolicy."""

    def setUp(self):
        setup_logger()

    def test_capable_manager_emits_capability_request(self):
        """A capable manager emits a per-capset DesiredTaskConcurrencyRequest with that capset."""
        manager_id = WorkerManagerID(b"manager_gpu")
        rules = [WaterfallRule(priority=1, worker_manager_id=manager_id, max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {manager_id: _create_worker_manager_info(manager_id, capabilities={"gpu": 4})}
        worker_count_by_manager = {manager_id: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        cap_requests = _capability_requests(commands[0])
        self.assertEqual(len(cap_requests), 1)
        self.assertEqual(cap_requests[0], ({"gpu": 1}, 1))

    def test_incapable_manager_emits_no_capability_request(self):
        """A manager that can't satisfy a capset emits no request for that capset."""
        manager_id = WorkerManagerID(b"manager_cpu")
        rules = [WaterfallRule(priority=1, worker_manager_id=manager_id, max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {manager_id: _create_worker_manager_info(manager_id, capabilities={})}
        worker_count_by_manager = {manager_id: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(_capability_requests(commands[0]), [])

    def test_higher_priority_capable_owns_capset(self):
        """When two managers can satisfy the capset, only the higher-priority one allocates it."""
        manager_id_a = WorkerManagerID(b"manager_a")
        manager_id_b = WorkerManagerID(b"manager_b")
        rules = [
            WaterfallRule(priority=1, worker_manager_id=manager_id_a, max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=manager_id_b, max_task_concurrency=10),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            manager_id_a: _create_worker_manager_info(manager_id_a, capabilities={"gpu": 4}),
            manager_id_b: _create_worker_manager_info(manager_id_b, capabilities={"gpu": 4}),
        }
        worker_count_by_manager = {manager_id_a: 0, manager_id_b: 0}

        commands_a = policy.get_scaling_commands(
            worker_manager_id=manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(_capability_requests(commands_a[0]), [({"gpu": 1}, 1)])

        commands_b = policy.get_scaling_commands(
            worker_manager_id=manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        # Higher-priority A owns the gpu capset; B's share is 0 -> emits setDesired(0) for generic capset.
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)
        self.assertEqual(_capability_requests(commands_b[0]), [])

    def test_capset_overflow_to_lower_priority_when_higher_full(self):
        """When the higher-priority capable manager is full for the capset, overflow goes to the next."""
        manager_id_a = WorkerManagerID(b"manager_a")
        manager_id_b = WorkerManagerID(b"manager_b")
        rules = [
            WaterfallRule(priority=1, worker_manager_id=manager_id_a, max_task_concurrency=2),
            WaterfallRule(priority=2, worker_manager_id=manager_id_b, max_task_concurrency=10),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(50, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            manager_id_a: _create_worker_manager_info(manager_id_a, max_task_concurrency=2, capabilities={"gpu": 4}),
            manager_id_b: _create_worker_manager_info(manager_id_b, max_task_concurrency=10, capabilities={"gpu": 4}),
        }
        worker_count_by_manager = {manager_id_a: 2, manager_id_b: 0}

        commands_b = policy.get_scaling_commands(
            worker_manager_id=manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # total_desired = ceil(50/10) = 5. manager_a cap=2 absorbs 2, manager_b allocates remaining 3.
        self.assertEqual(_capability_requests(commands_b[0]), [({"gpu": 1}, 3)])

    def test_capability_request_carries_concrete_dict(self):
        """The request's capabilities field carries the exact dict from the originating tasks."""
        manager_id = WorkerManagerID(b"mgr")
        rules = [WaterfallRule(priority=1, worker_manager_id=manager_id, max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 2, "nvlink": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {manager_id: _create_worker_manager_info(manager_id, capabilities={"gpu": 8, "nvlink": 4})}
        worker_count_by_manager = {manager_id: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(_capability_requests(commands[0]), [({"gpu": 2, "nvlink": 1}, 1)])

    def test_capability_request_emitted_even_when_overall_ratio_is_in_band(self):
        """A capability-specific request must still be allocated for tasks that need
        unique capabilities, even when the cluster-wide task/worker ratio doesn't warrant
        scaling generic workers."""
        manager_id = WorkerManagerID(b"manager_a")
        rules = [WaterfallRule(priority=1, worker_manager_id=manager_id, max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        # 3 generic + 1 gpu task; 3 workers running -> generic ratio is in band (no scale)
        # but a gpu request should still be emitted.
        generic_tasks = _create_tasks(3)
        gpu_tasks = _create_tasks(1, capabilities={"gpu": 1})
        all_tasks = {**generic_tasks, **gpu_tasks}
        workers = _create_workers(3)
        snapshot = InformationSnapshot(tasks=all_tasks, workers=workers)
        manager_infos = {manager_id: _create_worker_manager_info(manager_id, capabilities={"gpu": 4})}
        worker_count_by_manager = {manager_id: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(len(commands), 1)
        cap_requests = _capability_requests(commands[0])
        self.assertIn("gpu", {next(iter(caps.keys()), None) for caps, _ in cap_requests})

    def test_generic_capset_present_with_no_capability_tasks(self):
        """When all tasks have no capabilities, the manager receives a generic request
        (empty capabilities dict) carrying the ratio-derived allocation."""
        manager_id = WorkerManagerID(b"mgr")
        rules = [WaterfallRule(priority=1, worker_manager_id=manager_id, max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {manager_id: _create_worker_manager_info(manager_id)}
        worker_count_by_manager = {manager_id: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        self.assertEqual(len(commands), 1)
        # The generic (empty capset) request is present.
        generic = _generic_request(commands[0])
        self.assertIsNotNone(generic)
        # ceil(5/10) = 1 generic worker desired
        self.assertEqual(generic.taskConcurrency, 1)
        # No capability-specific requests since no capability tasks exist.
        self.assertEqual(_capability_requests(commands[0]), [])

    def test_capability_match_is_key_only(self):
        """Capability matching is name-only: a task requiring {a: 3} is considered serviceable
        by a manager that advertises {a: 5} -- numeric values are not used for subsumption."""
        manager_id = WorkerManagerID(b"mgr")
        rules = [WaterfallRule(priority=1, worker_manager_id=manager_id, max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"a": 3})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        # Manager's advertised "a" value (5) differs from the task's required "a" value (3).
        manager_infos = {manager_id: _create_worker_manager_info(manager_id, capabilities={"a": 5})}
        worker_count_by_manager = {manager_id: 0}

        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )

        # The manager is considered capable despite the value mismatch, so a request for
        # the {a} capset is allocated to it.
        self.assertEqual(len(commands), 1)
        cap_requests = _capability_requests(commands[0])
        self.assertEqual(len(cap_requests), 1)
        self.assertEqual(set(cap_requests[0][0].keys()), {"a"})


class TestWaterfallConfigParsing(unittest.TestCase):
    """Unit tests for waterfall config parsing via create_policies() factory."""

    def setUp(self):
        setup_logger()
        # EvenLoadAllocatePolicy creates an AsyncPriorityQueue which requires an event loop.
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def test_config_parsing_via_factory(self):
        """Verify the factory parses waterfall_v1 policy config correctly."""
        allocate_policy, scaling_policy = create_policies("waterfall_v1", "1,manager_a,10\n2,manager_b,20")
        self.assertIsInstance(allocate_policy, CapabilityAllocatePolicy)
        self.assertIsInstance(scaling_policy, WaterfallScalingPolicy)

    def test_config_parsing_with_comments(self):
        """Comments and blank lines should be ignored."""
        policy_content = "\n".join(
            [
                "#priority,worker_manager_id[,max_task_concurrency]",
                "1,manager_a,10",
                "",
                "2,manager_b,20  # overflow tier",
            ]
        )
        _, scaling_policy = create_policies("waterfall_v1", policy_content)
        self.assertIsInstance(scaling_policy, WaterfallScalingPolicy)

    def test_config_parsing_without_max_task_concurrency(self):
        """max_task_concurrency may be omitted; the rule's field is then None."""
        rules = parse_waterfall_rules("1,manager_a\n2,manager_b,20")
        self.assertIsNone(rules[0].max_task_concurrency)
        self.assertEqual(rules[1].max_task_concurrency, 20)

    def test_config_parsing_with_min_task_concurrency(self):
        """A fourth column sets the rule's floor; it defaults to 0."""
        rules = parse_waterfall_rules("1,native,8,8\n2,cloud,20")
        self.assertEqual(rules[0].min_task_concurrency, 8)
        self.assertEqual(rules[1].min_task_concurrency, 0)

    def test_config_parsing_floor_without_cap(self):
        """An empty max_task_concurrency column leaves the cap unset while a floor is given."""
        rules = parse_waterfall_rules("1,native,,4")
        self.assertIsNone(rules[0].max_task_concurrency)
        self.assertEqual(rules[0].min_task_concurrency, 4)

    def test_invalid_config_floor_above_cap(self):
        """A floor above the rule's cap should raise ValueError."""
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            create_policies("waterfall_v1", "1,native,4,8")

    def test_invalid_config_negative_floor(self):
        """A negative floor should raise ValueError."""
        with self.assertRaisesRegex(ValueError, "cannot be negative"):
            create_policies("waterfall_v1", "1,native,8,-1")

    def test_invalid_config_empty(self):
        """Empty policy content should raise ValueError."""
        with self.assertRaises(ValueError):
            create_policies("waterfall_v1", "")

    def test_invalid_config_comments_only(self):
        """Policy content with only comments should raise ValueError."""
        with self.assertRaises(ValueError):
            create_policies("waterfall_v1", "# just a comment\n# another comment")

    def test_invalid_config_wrong_field_count(self):
        """Lines with too few or too many fields should raise ValueError."""
        with self.assertRaises(ValueError):
            create_policies("waterfall_v1", "manager_a_only")
        with self.assertRaises(ValueError):
            create_policies("waterfall_v1", "1,manager_a,10,2,extra")

    def test_invalid_config_non_integer_priority(self):
        """Non-integer priority should raise ValueError."""
        with self.assertRaises(ValueError):
            create_policies("waterfall_v1", "high,manager_a,10")

    def test_invalid_config_non_integer_max_task_concurrency(self):
        """Non-integer max_task_concurrency should raise ValueError."""
        with self.assertRaises(ValueError):
            create_policies("waterfall_v1", "1,manager_a,many")

    def test_invalid_config_empty_worker_manager_id(self):
        """Empty worker_manager_id should raise ValueError."""
        with self.assertRaises(ValueError):
            create_policies("waterfall_v1", "1,,10")

    def test_invalid_config_duplicate_worker_manager_id(self):
        """Duplicate worker_manager_id should raise ValueError."""
        with self.assertRaisesRegex(ValueError, "duplicate worker_manager_id"):
            create_policies("waterfall_v1", "1,mgr_a,10\n2,mgr_a,20")

    def test_create_policies_builds_waterfall_scaling_policy(self):
        """Scaling policy emits declarative commands based on waterfall rules."""
        _, scaling_policy = create_policies("waterfall_v1", "1,manager_a,10\n2,manager_b,20")

        manager_id_a = WorkerManagerID(b"manager_a")
        manager_id_b = WorkerManagerID(b"manager_b")
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_infos = {
            manager_id_a: _create_worker_manager_info(manager_id_a, max_task_concurrency=10),
            manager_id_b: _create_worker_manager_info(manager_id_b, max_task_concurrency=20),
        }
        worker_count_by_manager = {manager_id_a: 0, manager_id_b: 0}

        commands_a = scaling_policy.get_scaling_commands(
            worker_manager_id=manager_id_a,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 1)

        commands_b = scaling_policy.get_scaling_commands(
            worker_manager_id=manager_id_b,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        # Higher-priority A absorbs all 1 desired worker; B's share is 0 -> emits setDesired(0).
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)


class TestWaterfallAllocationWithCapabilities(unittest.TestCase):
    """Tests that the waterfall allocation policy respects task capabilities."""

    def setUp(self):
        setup_logger()
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def test_task_with_capability_assigned_to_capable_worker(self):
        """Task requiring {"gpu": -1} must land on a worker that has gpu."""
        allocation_policy, _ = create_policies("waterfall_v1", "1,manager_a,10\n2,manager_b,20")

        worker_no_gpu = WorkerID(b"worker-no-gpu")
        worker_gpu = WorkerID(b"worker-gpu")

        # Insertion order matters: the no-gpu worker is queued first, so an
        # even-load allocator will hand it out first when both have count 0.
        self.assertTrue(allocation_policy.add_worker(worker_no_gpu, capabilities={}, queue_size=10))
        self.assertTrue(allocation_policy.add_worker(worker_gpu, capabilities={"gpu": -1}, queue_size=10))

        task = _create_mock_task(TaskID.generate_task_id(), capabilities={"gpu": -1})
        assigned = allocation_policy.assign_task(task)

        self.assertEqual(
            assigned, worker_gpu, f"task requiring gpu was assigned to {assigned!r}, expected {worker_gpu!r}"
        )

    def test_task_with_capability_not_assigned_when_no_capable_worker(self):
        """Task requiring gpu must not be assigned if no worker has gpu."""
        allocation_policy, _ = create_policies("waterfall_v1", "1,manager_a,10")

        worker_no_gpu = WorkerID(b"worker-no-gpu")
        self.assertTrue(allocation_policy.add_worker(worker_no_gpu, capabilities={}, queue_size=10))

        task = _create_mock_task(TaskID.generate_task_id(), capabilities={"gpu": -1})
        assigned = allocation_policy.assign_task(task)

        self.assertEqual(
            assigned,
            WorkerID.invalid_worker_id(),
            f"task requiring gpu was incorrectly assigned to {assigned!r} that has no gpu",
        )

    def test_has_available_worker_respects_capabilities(self):
        """has_available_worker({"gpu": -1}) must be False if no worker has gpu."""
        allocation_policy, _ = create_policies("waterfall_v1", "1,manager_a,10")

        worker_no_gpu = WorkerID(b"worker-no-gpu")
        self.assertTrue(allocation_policy.add_worker(worker_no_gpu, capabilities={}, queue_size=10))

        self.assertFalse(
            allocation_policy.has_available_worker({"gpu": -1}),
            "has_available_worker should return False when no worker has the requested capability",
        )


def _create_mock_task(task_id: TaskID, capabilities: Optional[Dict[str, int]] = None) -> Task:
    client_id = ClientID.generate_client_id()
    return Task(
        taskId=task_id,
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities=capabilities or {},
    )


def _create_mock_worker_heartbeat(
    queued_tasks: int = 0, capabilities: Optional[Dict[str, int]] = None
) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        agent=Resource(cpu=1, rss=1000000),
        rssFree=500000,
        queueSize=10,
        queuedTasks=queued_tasks,
        latencyMicroseconds=100,
        taskLock=False,
        processors=[],
        capabilities=capabilities or {},
        workerManagerID=b"test",
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


def _create_tasks(count: int, capabilities: Optional[Dict[str, int]] = None) -> Dict[TaskID, Task]:
    tasks = {}
    for _ in range(count):
        task_id = TaskID.generate_task_id()
        tasks[task_id] = _create_mock_task(task_id, capabilities=capabilities)
    return tasks


def _create_workers(
    count: int, queued_tasks: int = 0, capabilities: Optional[Dict[str, int]] = None
) -> Dict[WorkerID, WorkerHeartbeat]:
    workers = {}
    for i in range(count):
        worker_id = WorkerID(f"worker-{i}".encode())
        workers[worker_id] = _create_mock_worker_heartbeat(queued_tasks, capabilities=capabilities)
    return workers


def _tier(
    worker_manager_id: WorkerManagerID,
    max_task_concurrency: int,
    min_task_concurrency: int = 0,
    worker_count: int = 0,
    priority: int = 1,
) -> Tier:
    return Tier(worker_manager_id, priority, max_task_concurrency, min_task_concurrency, worker_count)


def _desired_by_manager(
    policy: WaterfallScalingPolicy, task_count: int, advertised_max_by_manager: Dict[WorkerManagerID, int]
) -> Dict[WorkerManagerID, int]:
    """Ask the policy once per manager, as each manager's heartbeat does, with one consistent cluster view."""
    snapshot = InformationSnapshot(tasks=_create_tasks(task_count), workers={})
    manager_infos = {
        manager_id: _create_worker_manager_info(manager_id, max_task_concurrency=advertised_max)
        for manager_id, advertised_max in advertised_max_by_manager.items()
    }
    worker_count_by_manager = {manager_id: 0 for manager_id in advertised_max_by_manager}
    desired = {}
    for manager_id, advertised_max in advertised_max_by_manager.items():
        commands = policy.get_scaling_commands(
            worker_manager_id=manager_id,
            information_snapshot=snapshot,
            worker_manager_infos=manager_infos,
            worker_count_by_manager=worker_count_by_manager,
        )
        desired[manager_id] = _generic_request(commands[0]).taskConcurrency
    return desired
