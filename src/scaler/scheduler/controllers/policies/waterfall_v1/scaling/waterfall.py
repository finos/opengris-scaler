import logging
from math import ceil
from typing import Dict, FrozenSet, List, Optional, Tuple

from scaler.protocol.capnp import (
    ScalingManagerStatus,
    WorkerManagerCommand,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
)
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


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
        manager_id = worker_manager_heartbeat.workerManagerID
        rule = self._find_rule(manager_id)

        if rule is None:
            logging.warning("Worker manager %r not found in waterfall rules, skipping scaling", manager_id)
            return []

        imperative = self._build_imperative(
            rule, information_snapshot, worker_manager_heartbeat, managed_worker_ids, worker_manager_snapshots
        )
        desired_per_capset = self._compute_desired_per_capset(
            rule, information_snapshot, worker_manager_heartbeat, managed_worker_ids, imperative
        )
        declarative = build_set_desired_command(desired_per_capset)
        return [declarative] + imperative

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

    def _build_imperative(
        self,
        rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        # Check for tasks with capabilities that no existing worker can handle
        capability_commands = self._create_capability_start_commands(
            rule, information_snapshot, worker_manager_heartbeat, managed_worker_ids, worker_manager_snapshots
        )
        if capability_commands:
            return capability_commands

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

    def _create_capability_start_commands(
        self,
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        """Create start commands for tasks whose capabilities are not met by any existing worker."""
        unmet = self._find_unmet_capabilities(information_snapshot)
        if not unmet:
            return []

        commands: List[WorkerManagerCommand] = []

        for required_keys, capability_dict in unmet.items():
            command = self._resolve_capability_start(
                current_rule,
                required_keys,
                capability_dict,
                managed_worker_ids,
                worker_manager_heartbeat,
                worker_manager_snapshots,
            )
            if command is not None:
                commands.append(command)

        return commands

    def _find_unmet_capabilities(
        self, information_snapshot: InformationSnapshot
    ) -> Dict[FrozenSet[str], Dict[str, int]]:
        """Return capability sets required by tasks that no existing worker can handle.

        Only considers tasks with non-empty capability requirements. Returns a dict mapping
        each unmet capability key-set to a representative capability dict from one such task.
        """
        worker_capability_sets: List[FrozenSet[str]] = [
            frozenset(worker_hb.capabilities.keys()) for worker_hb in information_snapshot.workers.values()
        ]

        unmet: Dict[FrozenSet[str], Dict[str, int]] = {}
        for task in information_snapshot.tasks.values():
            if not task.capabilities:
                continue
            required_keys = frozenset(task.capabilities.keys())
            if required_keys in unmet:
                continue
            if not any(required_keys <= wc for wc in worker_capability_sets):
                unmet[required_keys] = dict(task.capabilities)

        return unmet

    def _resolve_capability_start(
        self,
        current_rule: WaterfallRule,
        required_keys: FrozenSet[str],
        capability_dict: Dict[str, int],
        managed_worker_ids: List[WorkerID],
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> Optional[WorkerManagerCommand]:
        """Walk the waterfall priority chain and return a start command if this manager should handle it."""
        for rule in self._rules:
            snapshot = self._find_matching_snapshot(rule, worker_manager_snapshots)
            if snapshot is None:
                continue

            if not required_keys <= frozenset(snapshot.capabilities.keys()):
                continue

            effective_capacity = min(rule.max_task_concurrency, snapshot.max_task_concurrency)
            if snapshot.worker_count >= effective_capacity:
                continue

            # This is the highest-priority capable manager with capacity
            if rule.worker_manager_id == current_rule.worker_manager_id:
                local_capacity = min(current_rule.max_task_concurrency, worker_manager_heartbeat.maxTaskConcurrency)
                if len(managed_worker_ids) >= local_capacity:
                    return None
                return WorkerManagerCommand(
                    workerIDs=[], command=WorkerManagerCommandType.startWorkers, capabilities=capability_dict
                )
            else:
                # A higher-priority capable manager should handle it
                return None

        return None

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
        effective_capacity = min(current_rule.max_task_concurrency, worker_manager_heartbeat.maxTaskConcurrency)
        if len(managed_worker_ids) >= effective_capacity:
            return []

        return [WorkerManagerCommand(workerIDs=[], command=WorkerManagerCommandType.startWorkers, capabilities={})]

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

        # Partition managed workers: prefer shutting down workers without capabilities first
        no_cap_candidates: List[Tuple[WorkerID, int]] = []
        has_cap_candidates: List[Tuple[WorkerID, int]] = []
        for wid in managed_worker_ids:
            if wid in information_snapshot.workers:
                worker_hb = information_snapshot.workers[wid]
                if worker_hb.capabilities:
                    has_cap_candidates.append((wid, worker_hb.queuedTasks))
                else:
                    no_cap_candidates.append((wid, worker_hb.queuedTasks))

        no_cap_candidates.sort(key=lambda x: x[1])
        has_cap_candidates.sort(key=lambda x: x[1])
        workers_with_load = no_cap_candidates + has_cap_candidates

        if not workers_with_load:
            return []

        task_count = len(information_snapshot.tasks)
        if task_count == 0:
            min_keep = 0
        else:
            min_keep = max(1, ceil(task_count / self._upper_task_ratio))

        to_shutdown = len(workers_with_load) - min_keep
        if to_shutdown <= 0:
            return []

        shutdown_ids = [bytes(wid) for wid, _ in workers_with_load[:to_shutdown]]
        return [
            WorkerManagerCommand(
                workerIDs=shutdown_ids, command=WorkerManagerCommandType.shutdownWorkers, capabilities={}
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

    def _compute_desired_per_capset(
        self,
        rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        imperative_commands: List[WorkerManagerCommand],
    ) -> List[Tuple[Dict[str, int], int]]:
        """Compute desired per capability set using current + imperative delta.

        A single request with empty capabilities covers the generic (non-capability)
        demand for this manager. Capability-specific requests mirror imperative
        startWorkers commands carrying non-empty capabilities (i.e. capsets this
        manager has been asked to scale up for this heartbeat).
        """
        current = len(managed_worker_ids)

        start_delta = sum(1 for c in imperative_commands if c.command == WorkerManagerCommandType.startWorkers)
        shutdown_delta = sum(
            len(c.workerIDs) for c in imperative_commands if c.command == WorkerManagerCommandType.shutdownWorkers
        )
        desired_generic = max(0, current + start_delta - shutdown_delta)
        effective_capacity = min(rule.max_task_concurrency, worker_manager_heartbeat.maxTaskConcurrency)
        if effective_capacity >= 0:
            desired_generic = min(desired_generic, effective_capacity)

        result: List[Tuple[Dict[str, int], int]] = [({}, desired_generic)]

        for command in imperative_commands:
            if command.command != WorkerManagerCommandType.startWorkers:
                continue
            caps = command.capabilities
            caps_dict = dict(caps) if isinstance(caps, dict) else {c.name: c.value for c in caps}
            if not caps_dict:
                continue
            # Capability-specific capset: +1 worker requested this tick.
            cap_desired = 1
            if effective_capacity >= 0:
                cap_desired = min(cap_desired, effective_capacity)
            result.append((caps_dict, cap_desired))

        return result
