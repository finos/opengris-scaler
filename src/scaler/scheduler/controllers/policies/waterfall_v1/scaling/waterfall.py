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
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule
from scaler.utility.identifiers import WorkerID


class WaterfallScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that cascades worker scaling across prioritized worker managers.

    Priority-1 managers fill first; overflow goes to priority-2, then priority-3, etc.
    Shutdown is reversed: highest priority number (least preferred) drains first.

    Rules reference exact worker manager IDs. Each rule maps to one specific worker manager.

    All configuration (rules, thresholds) is immutable after construction.
    All mutable state is passed as function parameters.
    """

    def __init__(self, rules: List[WaterfallRule]):
        self._rules = sorted(rules, key=lambda r: r.priority)
        self._rule_by_manager_id: Dict[bytes, WaterfallRule] = {r.worker_manager_id: r for r in self._rules}
        # Scale down when tasks/workers < 1 (more workers than tasks, underutilized)
        self._lower_task_ratio = 1
        # Scale up when tasks/workers > 10 (tasks significantly outnumber workers, overloaded)
        self._upper_task_ratio = 10

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        managed_worker_capabilities: Dict[str, int],
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
                    rule, worker_manager_heartbeat, managed_worker_ids, worker_manager_snapshots
                )
            return []

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)

        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(
                rule, worker_manager_heartbeat, managed_worker_ids, worker_manager_snapshots
            )
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(
                rule, information_snapshot, managed_worker_ids, worker_manager_snapshots
            )

        return []

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(managed_workers=managed_workers)

    def _create_start_commands(
        self,
        current_rule: WaterfallRule,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        # Check if higher-priority managers (lower priority number) still have capacity
        for rule in self._rules:
            if rule.priority >= current_rule.priority:
                continue

            snapshot = self._find_matching_snapshot(rule, worker_manager_snapshots)
            if snapshot is None:
                continue  # manager offline or never seen

            effective_capacity = min(rule.max_task_concurrency, snapshot.max_task_concurrency)
            if snapshot.worker_count < effective_capacity:
                # Higher-priority manager still has room, let it fill first
                return []

        # Check this manager's effective capacity
        effective_capacity = min(current_rule.max_task_concurrency, worker_manager_heartbeat.max_task_concurrency)
        if len(managed_worker_ids) >= effective_capacity:
            return []

        return [WorkerManagerCommand.new_msg(worker_ids=[], command=WorkerManagerCommandType.StartWorkers)]

    def _create_shutdown_commands(
        self,
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        if not managed_worker_ids:
            return []

        # Check if lower-priority managers (higher priority number) still have workers to drain first
        for rule in self._rules:
            if rule.priority <= current_rule.priority:
                continue

            snapshot = self._find_matching_snapshot(rule, worker_manager_snapshots)
            if snapshot is not None and snapshot.worker_count > 0:
                # Lower-priority manager still has workers, let it drain first
                return []

        # Find the worker with fewest queued tasks
        least_busy_wid: Optional[WorkerID] = None
        min_queued = float("inf")
        for wid in managed_worker_ids:
            if wid in information_snapshot.workers:
                queued = information_snapshot.workers[wid].queued_tasks
                if queued < min_queued:
                    min_queued = queued
                    least_busy_wid = wid

        if least_busy_wid is None:
            return []
        return [
            WorkerManagerCommand.new_msg(
                worker_ids=[bytes(least_busy_wid)], command=WorkerManagerCommandType.ShutdownWorkers
            )
        ]

    def _find_rule(self, manager_id: bytes) -> Optional[WaterfallRule]:
        """Find the rule whose worker manager ID matches *manager_id*."""
        return self._rule_by_manager_id.get(manager_id)

    def _find_matching_snapshot(
        self, rule: WaterfallRule, snapshots: Dict[bytes, WorkerManagerSnapshot]
    ) -> Optional[WorkerManagerSnapshot]:
        """Return the manager snapshot matching *rule*'s worker manager ID, or None."""
        return snapshots.get(rule.worker_manager_id)
