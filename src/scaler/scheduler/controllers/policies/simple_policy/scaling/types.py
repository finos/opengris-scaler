import dataclasses
import enum
from typing import Dict, Optional


@dataclasses.dataclass(frozen=True)
class WorkerManagerSnapshot:
    """Immutable snapshot of a worker manager's state, passed to stateless scaling policies."""

    worker_manager_id: bytes
    max_task_concurrency: int
    worker_count: int
    last_seen_at: float  # time.time() epoch seconds of the last heartbeat
    capabilities: Dict[str, int] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class WorkerManagerBounds:
    """The range a scaling policy keeps the desired task concurrency of one worker manager in."""

    max_task_concurrency: Optional[int]
    min_task_concurrency: int = 0


class ScalingPolicyStrategy(enum.Enum):
    STATIC = "static"
    VANILLA = "vanilla"
    CAPABILITY = "capability"

    def __str__(self):
        return self.name
