from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

from scaler.scheduler.types import ClusterState
from scaler.utility.identifiers import WorkerManagerID


class ScalingEstimator(ABC):
    """Estimates desired worker counts from cluster state."""

    @abstractmethod
    def update(self, cluster_state: ClusterState) -> None:
        """Receive new cluster state; may use it for later estimation."""
        raise NotImplementedError()

    @abstractmethod
    def estimate(self, manager_id: WorkerManagerID) -> List[Tuple[Dict[str, int], int]]:
        """Return per-capset desired worker counts as (capabilities_dict, desired) tuples."""
        raise NotImplementedError()

    @abstractmethod
    def forget(self, manager_id: WorkerManagerID) -> None:
        """Clean up state for a departed manager."""
        raise NotImplementedError()
