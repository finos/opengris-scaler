import dataclasses
from typing import Dict


@dataclasses.dataclass(frozen=True)
class WorkerManagerSnapshot:
    """Immutable snapshot of a worker manager's state, passed to stateless scaling policies."""

    worker_manager_id: bytes
    max_task_concurrency: int
    worker_count: int
    last_seen_at: float  # time.time() epoch seconds of the last heartbeat
    capabilities: Dict[str, int] = dataclasses.field(default_factory=dict)
