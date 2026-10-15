import dataclasses
from typing import Optional


@dataclasses.dataclass(frozen=True)
class WaterfallRule:
    """A single rule in the waterfall config, parsed from
    'priority,worker_manager_id[,max_task_concurrency[,min_task_concurrency]]'."""

    priority: int
    worker_manager_id: bytes
    max_task_concurrency: Optional[int]
    min_task_concurrency: int = 0


@dataclasses.dataclass(frozen=True)
class Tier:
    """An online worker manager's rule, with its cap and floor resolved against the maximum it advertises."""

    worker_manager_id: bytes
    priority: int
    cap: int
    floor: int
    worker_count: int
