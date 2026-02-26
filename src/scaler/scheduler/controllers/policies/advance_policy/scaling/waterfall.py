import logging
from typing import Dict, List, Optional

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerAdapterCommand,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.advance_policy.scaling.types import WaterfallRule
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerAdapterSnapshot,
    WorkerGroupCapabilities,
    WorkerGroupID,
    WorkerGroupState,
)


class WaterfallScalingController(ScalingController):
    """
    Stateless scaling controller that cascades worker scaling across prioritized adapters.

    Priority-1 adapters fill first; overflow goes to priority-2, then priority-3, etc.
    Shutdown is reversed: highest priority number (least preferred) drains first.

    Rules specify adapter ID prefixes (e.g. ``NAT``, ``ECS``). At runtime each adapter
    generates a full ID like ``NAT|<pid>``. Matching uses prefix: an adapter ID matches
    a rule when ``adapter_id == rule_prefix`` or ``adapter_id`` starts with
    ``rule_prefix + b"|"``.

    All configuration (rules, thresholds) is immutable after construction.
    All mutable state is passed as function parameters.
    """

    def __init__(self, rules: List[WaterfallRule]):
        self._rules = sorted(rules, key=lambda r: r.priority)
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

    @staticmethod
    def _adapter_matches_rule(adapter_id: bytes, rule_prefix: bytes) -> bool:
        """Check if a runtime adapter ID matches a rule's prefix.

        Matches when the adapter ID equals the prefix exactly, or starts with
        the prefix followed by ``|`` (the delimiter used by all adapters).
        """
        return adapter_id == rule_prefix or adapter_id.startswith(rule_prefix + b"|")

    def _find_rule(self, adapter_id: bytes) -> Optional[WaterfallRule]:
        """Find the rule whose prefix matches *adapter_id*."""
        for rule in self._rules:
            if self._adapter_matches_rule(adapter_id, rule.worker_adapter_id):
                return rule
        return None

    def _find_matching_snapshots(
        self, rule: WaterfallRule, snapshots: Dict[bytes, WorkerAdapterSnapshot]
    ) -> List[WorkerAdapterSnapshot]:
        """Return all adapter snapshots whose runtime ID matches *rule*'s prefix."""
        return [
            s for s in snapshots.values() if self._adapter_matches_rule(s.worker_adapter_id, rule.worker_adapter_id)
        ]

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        adapter_heartbeat: WorkerAdapterHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
        worker_adapter_snapshots: Dict[bytes, WorkerAdapterSnapshot],
    ) -> List[WorkerAdapterCommand]:
        adapter_id = adapter_heartbeat.worker_adapter_id
        rule = self._find_rule(adapter_id)

        if rule is None:
            logging.warning("Adapter %r not found in waterfall rules, skipping scaling", adapter_id)
            return []

        if not information_snapshot.workers:
            if information_snapshot.tasks:
                return self._create_start_commands(rule, adapter_heartbeat, worker_groups, worker_adapter_snapshots)
            return []

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)

        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(rule, adapter_heartbeat, worker_groups, worker_adapter_snapshots)
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(rule, information_snapshot, worker_groups, worker_adapter_snapshots)

        return []

    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=worker_groups)

    def _create_start_commands(
        self,
        current_rule: WaterfallRule,
        adapter_heartbeat: WorkerAdapterHeartbeat,
        worker_groups: WorkerGroupState,
        worker_adapter_snapshots: Dict[bytes, WorkerAdapterSnapshot],
    ) -> List[WorkerAdapterCommand]:
        # Check if higher-priority adapters (lower priority number) still have capacity
        for rule in self._rules:
            if rule.priority >= current_rule.priority:
                continue

            matching_snapshots = self._find_matching_snapshots(rule, worker_adapter_snapshots)
            if not matching_snapshots:
                # All adapters for this prefix are offline or never seen, skip
                continue

            for snapshot in matching_snapshots:
                effective_capacity = min(rule.max_workers, snapshot.max_worker_groups)
                if snapshot.worker_group_count < effective_capacity:
                    # Higher-priority adapter still has room, let it fill first
                    return []

        # Check this adapter's effective capacity
        effective_capacity = min(current_rule.max_workers, adapter_heartbeat.max_worker_groups)
        if len(worker_groups) >= effective_capacity:
            return []

        return [WorkerAdapterCommand.new_msg(worker_group_id=b"", command=WorkerAdapterCommandType.StartWorkerGroup)]

    def _create_shutdown_commands(
        self,
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        worker_groups: WorkerGroupState,
        worker_adapter_snapshots: Dict[bytes, WorkerAdapterSnapshot],
    ) -> List[WorkerAdapterCommand]:
        if not worker_groups:
            return []

        # Check if lower-priority adapters (higher priority number) still have workers to drain first
        for rule in self._rules:
            if rule.priority <= current_rule.priority:
                continue

            for snapshot in self._find_matching_snapshots(rule, worker_adapter_snapshots):
                if snapshot.worker_group_count > 0:
                    # Lower-priority adapter still has workers, let it drain first
                    return []

        # Find the worker group with fewest queued tasks
        worker_group_task_counts: Dict[WorkerGroupID, int] = {}
        for worker_group_id, worker_ids in worker_groups.items():
            total_queued = sum(
                information_snapshot.workers[worker_id].queued_tasks
                for worker_id in worker_ids
                if worker_id in information_snapshot.workers
            )
            worker_group_task_counts[worker_group_id] = total_queued

        if not worker_group_task_counts:
            return []

        worker_group_id = min(worker_group_task_counts, key=lambda k: worker_group_task_counts[k])
        return [
            WorkerAdapterCommand.new_msg(
                worker_group_id=worker_group_id, command=WorkerAdapterCommandType.ShutdownWorkerGroup
            )
        ]
