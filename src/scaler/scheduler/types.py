import dataclasses
from typing import FrozenSet, Mapping

from scaler.utility.identifiers import WorkerManagerID


@dataclasses.dataclass(frozen=True)
class WorkerManagerInfo:
    """Immutable snapshot of a worker manager's state."""

    manager_id: WorkerManagerID
    max_task_concurrency: int  # NO_LIMIT = unlimited
    worker_count: int
    capabilities: Mapping[str, int]


@dataclasses.dataclass(frozen=True)
class ClusterState:
    """Cluster-wide snapshot for scaling estimation."""

    n_tasks: int
    n_workers: int
    n_draining_workers: int
    n_tasks_by_capset: Mapping[FrozenSet[str], int] = dataclasses.field(default_factory=dict)
    capset_representatives: Mapping[FrozenSet[str], Mapping[str, int]] = dataclasses.field(default_factory=dict)
    managers: Mapping[WorkerManagerID, WorkerManagerInfo] = dataclasses.field(default_factory=dict)
