from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple


@dataclass(frozen=True)
class ManagerInfo:
    """Per-manager state. Not tied to a heartbeat — just the manager's current state."""

    manager_id: bytes
    max_concurrency: Optional[int]  # None = unlimited
    worker_count: int
    capabilities: Dict[str, int]


@dataclass(frozen=True)
class ClusterState:
    """Cluster-wide snapshot for scaling estimation."""

    n_tasks: int
    n_workers: int
    n_tasks_by_capset: Dict[FrozenSet[str], int] = field(default_factory=dict)
    capset_representatives: Dict[FrozenSet[str], Dict[str, int]] = field(default_factory=dict)
    managers: Dict[bytes, ManagerInfo] = field(default_factory=dict)


class ScalingEstimator(ABC):
    """Estimates desired worker counts from cluster state."""

    @abstractmethod
    def update(self, cluster_state: ClusterState) -> None:
        """Receive new cluster state; may use it for later estimation."""
        raise NotImplementedError()

    @abstractmethod
    def estimate(self, manager_id: bytes) -> List[Tuple[Dict[str, int], int]]:
        """Return per-capset desired worker counts as (capabilities_dict, desired) tuples."""
        raise NotImplementedError()

    @abstractmethod
    def forget(self, manager_id: bytes) -> None:
        """Clean up state for a departed manager."""
        raise NotImplementedError()
