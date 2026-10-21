from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple


@dataclass(frozen=True)
class ClusterState:
    """Snapshot of cluster state for scaling estimation."""

    n_tasks: int
    n_workers: int
    max_concurrency: Optional[int]  # None = unlimited
    n_tasks_by_capset: Dict[FrozenSet[str], int] = field(default_factory=dict)
    capset_representatives: Dict[FrozenSet[str], Dict[str, int]] = field(default_factory=dict)
    n_workers_by_manager: Dict[bytes, int] = field(default_factory=dict)


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
