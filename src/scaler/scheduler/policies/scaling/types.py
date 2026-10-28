import dataclasses
from typing import Optional


@dataclasses.dataclass(frozen=True)
class WorkerManagerBounds:
    """The range a scaling policy keeps the desired task concurrency of one worker manager in."""

    max_task_concurrency: Optional[int]
    min_task_concurrency: int = 0
