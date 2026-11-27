import logging
from math import ceil
from typing import Dict, FrozenSet, List, Mapping, Optional, Tuple

from scaler.config.defaults import TASKS_PER_WORKER
from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.utility import build_set_desired_command
from scaler.scheduler.policies.scaling.waterfall.types import Tier, WaterfallRule
from scaler.scheduler.types import ClusterState
from scaler.utility.identifiers import WorkerManagerID

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
        self._rule_by_manager_id: Dict[WorkerManagerID, WaterfallRule] = {r.worker_manager_id: r for r in self._rules}

    def get_scaling_commands(
        self, worker_manager_id: WorkerManagerID, cluster_state: ClusterState
    ) -> List[WorkerManagerCommand]:
        rule = self._rule_by_manager_id.get(worker_manager_id)

        if rule is None:
            logger.warning(f"{worker_manager_id!r} not found in waterfall rules, skipping scaling")
            return []

        desired_per_capset = self._compute_desired_per_capset(rule, cluster_state)
        return [build_set_desired_command(desired_per_capset)]

    def _compute_desired_per_capset(
        self, current_rule: WaterfallRule, cluster_state: ClusterState
    ) -> List[Tuple[Mapping[str, int], int]]:
        """Compute desired worker count per capability set for this manager only.

        Generic (empty-cap) tasks fill higher-priority managers first; this manager's share is
        whatever overflows from higher priorities, clamped by its effective capacity, and never below its floor.

        Capability-specific requests are emitted only for capsets this manager owns: the
        highest-priority manager whose advertised capabilities are a superset of the capset.
        """
        result: List[Tuple[Mapping[str, int], int]] = []

        generic_desired = self._allocate_generic_desired(current_rule, cluster_state)
        result.append(({}, generic_desired))

        for capset in cluster_state.n_tasks_by_capset:
            if not capset:
                continue  # the generic capset is allocated above
            cap_desired = self._allocate_capset_desired(current_rule, capset, cluster_state)
            if cap_desired > 0:
                result.append((cluster_state.capset_representatives[capset], cap_desired))

        return result

    def _allocate_generic_desired(self, current_rule: WaterfallRule, cluster_state: ClusterState) -> int:
        """Allocate this manager's share of the cluster-wide generic worker target."""
        tiers = self._online_tiers(current_rule, cluster_state)
        current_tier = next(tier for tier in tiers if tier.worker_manager_id == current_rule.worker_manager_id)
        target = target_worker_count(cluster_state.n_tasks)
        if target == 0 and lower_priority_tier_above_floor(current_tier, tiers):
            return max(current_tier.min_task_concurrency, current_tier.worker_count)
        return allocate(target, tiers)[current_tier.worker_manager_id]

    def _online_tiers(self, current_rule: WaterfallRule, cluster_state: ClusterState) -> List[Tier]:
        """The rules of the online managers in priority order, with their concurrency limits resolved."""
        tiers = []
        for rule in self._rules:
            max_task_concurrency = self._effective_capacity(rule, current_rule, cluster_state)
            if max_task_concurrency is None:
                continue
            info = cluster_state.managers.get(rule.worker_manager_id)
            worker_count = info.worker_count if info is not None else 0
            min_task_concurrency = min(rule.min_task_concurrency, max_task_concurrency)
            tiers.append(
                Tier(rule.worker_manager_id, rule.priority, max_task_concurrency, min_task_concurrency, worker_count)
            )
        return tiers

    def _allocate_capset_desired(
        self, current_rule: WaterfallRule, required_keys: FrozenSet[str], cluster_state: ClusterState
    ) -> int:
        """Allocate this manager's share of the target for one capability set."""
        task_count = cluster_state.n_tasks_by_capset.get(required_keys, 0)
        remaining = target_worker_count(task_count)
        for rule in self._rules:
            info = cluster_state.managers.get(rule.worker_manager_id)
            if info is None:
                continue
            manager_capabilities = frozenset(info.capabilities.keys())
            if not required_keys <= manager_capabilities:
                continue

            cap = self._effective_capacity(rule, current_rule, cluster_state)
            if cap is None:
                continue
            if rule.worker_manager_id == current_rule.worker_manager_id:
                return min(remaining, cap)
            remaining = max(0, remaining - cap)
        return 0

    def _effective_capacity(
        self, rule: WaterfallRule, current_rule: WaterfallRule, cluster_state: ClusterState
    ) -> Optional[int]:
        """Return effective worker capacity for *rule*.

        When the rule specifies a cap, returns min(cap, info max). When omitted, returns the
        info-reported max directly. Returns None if the manager is offline.
        """
        if rule.worker_manager_id == current_rule.worker_manager_id:
            reported = cluster_state.managers[current_rule.worker_manager_id].max_task_concurrency
            return min(rule.max_task_concurrency, reported) if rule.max_task_concurrency is not None else reported
        info = cluster_state.managers.get(rule.worker_manager_id)
        if info is None:
            return None
        reported = info.max_task_concurrency
        return min(rule.max_task_concurrency, reported) if rule.max_task_concurrency is not None else reported


def target_worker_count(task_count: int) -> int:
    """The workers the cluster needs for `task_count` tasks, rounded up."""
    return ceil(task_count / TASKS_PER_WORKER)


def allocate(target: int, tiers: List[Tier]) -> Dict[WorkerManagerID, int]:
    """Split `target` workers over `tiers`, given in priority order.

    Every floor counts as supply first; the rest fills each tier above its floor, up to its cap, in order."""
    remaining = max(0, target - sum(tier.min_task_concurrency for tier in tiers))
    allocation = {}
    for tier in tiers:
        share = min(remaining, tier.max_task_concurrency - tier.min_task_concurrency)
        allocation[tier.worker_manager_id] = tier.min_task_concurrency + share
        remaining -= share
    return allocation


def lower_priority_tier_above_floor(tier: Tier, tiers: List[Tier]) -> bool:
    """With no tasks, `tier` keeps its workers while a lower-priority tier still drains toward its floor."""
    return any(other.priority > tier.priority and other.worker_count > other.min_task_concurrency for other in tiers)
