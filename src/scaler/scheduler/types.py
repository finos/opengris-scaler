import dataclasses
from typing import Dict, FrozenSet, Optional

from scaler.utility.identifiers import WorkerManagerID


@dataclasses.dataclass(frozen=True)
class WorkerManagerInfo:
    """Immutable snapshot of a worker manager's state."""

    manager_id: WorkerManagerID
    max_task_concurrency: Optional[int]  # None = unlimited
    worker_count: int
    capabilities: Dict[str, int]


@dataclasses.dataclass(frozen=True)
class ClusterState:
    """Cluster-wide snapshot for scaling estimation."""

    n_tasks: int
    n_workers: int
    n_tasks_by_capset: Dict[FrozenSet[str], int] = dataclasses.field(default_factory=dict)
    capset_representatives: Dict[FrozenSet[str], Dict[str, int]] = dataclasses.field(default_factory=dict)
    managers: Dict[WorkerManagerID, WorkerManagerInfo] = dataclasses.field(default_factory=dict)
