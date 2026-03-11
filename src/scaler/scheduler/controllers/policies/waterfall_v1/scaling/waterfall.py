import logging
from typing import Dict, List, Optional

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerManagerCommand,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerGroupCapabilities,
    WorkerGroupID,
    WorkerGroupState,
    WorkerManagerSnapshot,
)
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule


class WaterfallScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that cascades worker scaling across prioritized worker managers.

    Priority-1 managers fill first; overflow goes to priority-2, then priority-3, etc.
    Shutdown is reversed: highest priority number (least preferred) drains first.

    Rules specify worker types (e.g. ``native``, ``ecs``). At runtime each worker manager
    generates a full worker manager ID like ``native|<uuid>``. Matching uses worker type as
    prefix: a manager ID matches a rule when ``manager_id == worker_type`` or ``manager_id``
    starts with ``worker_type + b"|"``.

    All configuration (rules, thresholds) is immutable after construction.
    All mutable state is passed as function parameters.
    """

    def __init__(self, rules: List[WaterfallRule]):
        self._rules = sorted(rules, key=lambda r: r.priority)
        # Scale down when tasks/workers < 1 (more workers than tasks, underutilized)
        self._lower_task_ratio = 1
        # Scale up when tasks/workers > 10 (tasks significantly outnumber workers, overloaded)
        self._upper_task_ratio = 10

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        manager_id = worker_manager_heartbeat.worker_manager_id
        rule = self._find_rule(manager_id)

        if rule is None:
            logging.warning("Worker manager %r not found in waterfall rules, skipping scaling", manager_id)
            return []

        if not information_snapshot.workers:
            if information_snapshot.tasks:
                return self._create_start_commands(
                    rule, worker_manager_heartbeat, worker_groups, worker_manager_snapshots
                )
            return []

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)

        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(rule, worker_manager_heartbeat, worker_groups, worker_manager_snapshots)
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(rule, information_snapshot, worker_groups, worker_manager_snapshots)

        return []

    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=worker_groups)

    def _create_start_commands(
        self,
        current_rule: WaterfallRule,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        worker_groups: WorkerGroupState,
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        # Check if higher-priority managers (lower priority number) still have capacity
        for rule in self._rules:
            if rule.priority >= current_rule.priority:
                continue

            matching_snapshots = self._find_matching_snapshots(rule, worker_manager_snapshots)
            if not matching_snapshots:
                # All managers for this worker type are offline or never seen, skip
                continue

            for snapshot in matching_snapshots:
                effective_capacity = min(rule.max_task_concurrency, snapshot.max_worker_groups)
                if snapshot.worker_group_count < effective_capacity:
                    # Higher-priority manager still has room, let it fill first
                    return []

        # Check this manager's effective capacity
        effective_capacity = min(current_rule.max_task_concurrency, worker_manager_heartbeat.max_worker_groups)
        if len(worker_groups) >= effective_capacity:
            return []

        return [WorkerManagerCommand.new_msg(worker_group_id=b"", command=WorkerManagerCommandType.StartWorkerGroup)]

    def _create_shutdown_commands(
        self,
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        worker_groups: WorkerGroupState,
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        if not worker_groups:
            return []

        # Check if lower-priority managers (higher priority number) still have workers to drain first
        for rule in self._rules:
            if rule.priority <= current_rule.priority:
                continue

            for snapshot in self._find_matching_snapshots(rule, worker_manager_snapshots):
                if snapshot.worker_group_count > 0:
                    # Lower-priority manager still has workers, let it drain first
                    return []

        # Find the worker group with fewest queued tasks
        min_worker_group_id: Optional[WorkerGroupID] = None
        min_queued = float("inf")
        for worker_group_id, worker_ids in worker_groups.items():
            total_queued = sum(
                information_snapshot.workers[worker_id].queued_tasks
                for worker_id in worker_ids
                if worker_id in information_snapshot.workers
            )
            if total_queued < min_queued:
                min_queued = total_queued
                min_worker_group_id = worker_group_id

        if min_worker_group_id is None:
            return []
        return [
            WorkerManagerCommand.new_msg(
                worker_group_id=min_worker_group_id, command=WorkerManagerCommandType.ShutdownWorkerGroup
            )
        ]

    @staticmethod
    def _manager_matches_rule(manager_id: bytes, worker_type: bytes) -> bool:
        """Check if a runtime worker manager ID matches a rule's worker type.

        Matches when the manager ID equals the worker type exactly, or starts with
        the worker type followed by ``|`` (the delimiter used by all managers).
        """
        return manager_id == worker_type or manager_id.startswith(worker_type + b"|")

    def _find_rule(self, manager_id: bytes) -> Optional[WaterfallRule]:
        """Find the rule whose worker type matches *manager_id*."""
        for rule in self._rules:
            if self._manager_matches_rule(manager_id, rule.worker_type):
                return rule
        return None

    def _find_matching_snapshots(
        self, rule: WaterfallRule, snapshots: Dict[bytes, WorkerManagerSnapshot]
    ) -> List[WorkerManagerSnapshot]:
        """Return all manager snapshots whose runtime ID matches *rule*'s worker type."""
        return [s for s in snapshots.values() if self._manager_matches_rule(s.worker_manager_id, rule.worker_type)]
