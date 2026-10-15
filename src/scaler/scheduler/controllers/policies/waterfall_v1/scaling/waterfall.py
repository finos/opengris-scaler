import logging
from math import ceil
from typing import Dict, FrozenSet, List, Optional, Tuple

from scaler.config.defaults import TASKS_PER_WORKER
from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import Tier, WaterfallRule
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot

logger = logging.getLogger(__name__)


class WaterfallScalingPolicy(ScalingPolicy):
    """
    Stateless declarative scaling policy that cascades worker allocation across prioritized
    worker managers.

    For each capability set (including the generic empty set) the total target worker count
    is computed from observed task volume, then assigned to managers in priority order.
    Higher-priority managers absorb capacity first; overflow spills to the next priority.

    Capability-aware allocation: only managers whose advertised capabilities are a superset
    of a task capset participate in that capset's allocation chain.
    """

    def __init__(self, rules: List[WaterfallRule]):
        self._rules = sorted(rules, key=lambda r: r.priority)
        self._rule_by_manager_id: Dict[bytes, WaterfallRule] = {r.worker_manager_id: r for r in self._rules}

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        manager_id = worker_manager_heartbeat.workerManagerID
        rule = self._find_rule(manager_id)

        if rule is None:
            logger.warning("Worker manager %r not found in waterfall rules, skipping scaling", manager_id)
            return []

        desired_per_capset = self._compute_desired_per_capset(
            rule, information_snapshot, worker_manager_heartbeat, worker_manager_snapshots
        )
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

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
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[Tuple[Dict[str, int], int]]:
        """Compute desired worker count per capability set for this manager only.

        Generic (empty-cap) tasks fill higher-priority managers first; this manager's share is
        whatever overflows from higher priorities, clamped by its effective capacity, and never below its floor.

        Capability-specific requests are emitted only for capsets this manager owns: the
        highest-priority manager whose advertised capabilities are a superset of the capset.
        """
        result: List[Tuple[Dict[str, int], int]] = []

        generic_desired = self._allocate_generic_desired(
            current_rule, information_snapshot, current_heartbeat, snapshots
        )
        result.append(({}, generic_desired))

        for required_keys, capability_dict in self._tasks_by_capability(information_snapshot).items():
            cap_desired = self._allocate_capset_desired(
                current_rule, required_keys, information_snapshot, current_heartbeat, snapshots
            )
            if cap_desired > 0:
                result.append((capability_dict, cap_desired))

        return result

    def _allocate_generic_desired(
        self,
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> int:
        """Allocate this manager's share of the cluster-wide generic worker target."""
        tiers = self._online_tiers(current_rule, current_heartbeat, snapshots)
        current_tier = next(tier for tier in tiers if tier.worker_manager_id == current_rule.worker_manager_id)
        target = target_worker_count(len(information_snapshot.tasks))
        if target == 0 and lower_priority_tier_above_floor(current_tier, tiers):
            return max(current_tier.floor, current_tier.worker_count)
        return allocate(target, tiers)[current_tier.worker_manager_id]

    def _online_tiers(
        self,
        current_rule: WaterfallRule,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[Tier]:
        """The rules of the online managers in priority order, with their caps and floors resolved."""
        tiers = []
        for rule in self._rules:
            cap = self._effective_capacity(rule, current_rule, current_heartbeat, snapshots)
            if cap is None:
                continue
            snap = snapshots.get(rule.worker_manager_id)
            worker_count = snap.worker_count if snap is not None else 0
            floor = min(rule.min_task_concurrency, cap)
            tiers.append(Tier(rule.worker_manager_id, rule.priority, cap, floor, worker_count))
        return tiers

    def _allocate_capset_desired(
        self,
        current_rule: WaterfallRule,
        required_keys: FrozenSet[str],
        information_snapshot: InformationSnapshot,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> int:
        """Allocate this manager's share of the target for one capability set."""
        task_count = sum(
            1 for task in information_snapshot.tasks.values() if frozenset(task.capabilities.keys()) == required_keys
        )
        remaining = target_worker_count(task_count)
        for rule in self._rules:
            snap = self._find_matching_snapshot(rule, snapshots)
            if rule.worker_manager_id == current_rule.worker_manager_id:
                manager_capabilities = frozenset(current_heartbeat.capabilities.keys())
            elif snap is not None:
                manager_capabilities = frozenset(snap.capabilities.keys())
            else:
                continue
            if not required_keys <= manager_capabilities:
                continue

            cap = self._effective_capacity(rule, current_rule, current_heartbeat, snapshots)
            if cap is None:
                continue
            if rule.worker_manager_id == current_rule.worker_manager_id:
                return min(remaining, cap)
            remaining = max(0, remaining - cap)
        return 0

    def _effective_capacity(
        self,
        rule: WaterfallRule,
        current_rule: WaterfallRule,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> Optional[int]:
        """Return effective worker capacity for *rule*.

        When the rule specifies a cap, returns min(cap, heartbeat max). When omitted, returns the
        heartbeat-reported max directly. Returns None if the manager is offline.
        """
        if rule.worker_manager_id == current_rule.worker_manager_id:
            reported = current_heartbeat.maxTaskConcurrency
            return min(rule.max_task_concurrency, reported) if rule.max_task_concurrency is not None else reported
        snap = self._find_matching_snapshot(rule, snapshots)
        if snap is None:
            return None
        return (
            min(rule.max_task_concurrency, snap.max_task_concurrency)
            if rule.max_task_concurrency is not None
            else snap.max_task_concurrency
        )

    def _tasks_by_capability(self, information_snapshot: InformationSnapshot) -> Dict[FrozenSet[str], Dict[str, int]]:
        """Group tasks with non-empty capabilities, mapping capset keys to a representative dict."""
        result: Dict[FrozenSet[str], Dict[str, int]] = {}
        for task in information_snapshot.tasks.values():
            if not task.capabilities:
                continue
            required_keys = frozenset(task.capabilities.keys())
            if required_keys not in result:
                result[required_keys] = dict(task.capabilities)
        return result


def target_worker_count(task_count: int) -> int:
    """The workers the cluster needs for `task_count` tasks, rounded up."""
    return ceil(task_count / TASKS_PER_WORKER)


def allocate(target: int, tiers: List[Tier]) -> Dict[bytes, int]:
    """Split `target` workers over `tiers`, given in priority order.

    Every floor counts as supply first; the rest fills each tier above its floor, up to its cap, in order."""
    remaining = max(0, target - sum(tier.floor for tier in tiers))
    allocation = {}
    for tier in tiers:
        share = min(remaining, tier.cap - tier.floor)
        allocation[tier.worker_manager_id] = tier.floor + share
        remaining -= share
    return allocation


def lower_priority_tier_above_floor(tier: Tier, tiers: List[Tier]) -> bool:
    """With no tasks, `tier` keeps its workers while a lower-priority tier still drains toward its floor."""
    return any(other.priority > tier.priority and other.worker_count > other.floor for other in tiers)
