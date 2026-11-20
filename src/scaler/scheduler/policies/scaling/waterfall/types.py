import dataclasses
from typing import Optional

from scaler.utility.identifiers import WorkerManagerID


@dataclasses.dataclass(frozen=True)
class WaterfallRule:
    """A single rule in the waterfall config, parsed from
    'priority,worker_manager_id[,max_task_concurrency[,min_task_concurrency]]'."""

    priority: int
    worker_manager_id: WorkerManagerID
    max_task_concurrency: Optional[int]
    min_task_concurrency: int = 0


@dataclasses.dataclass(frozen=True)
class Tier:
    """An online worker manager's rule, with its concurrency limits resolved against the maximum it advertises."""

    worker_manager_id: WorkerManagerID
    priority: int
    max_task_concurrency: int
    min_task_concurrency: int
    worker_count: int
