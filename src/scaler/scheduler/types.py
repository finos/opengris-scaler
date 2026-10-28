import dataclasses
from typing import Dict

from scaler.utility.identifiers import WorkerManagerID


@dataclasses.dataclass
class WorkerManagerInfo:
    """Per-manager state tracked by the WorkerManagerController."""

    manager_id: WorkerManagerID
    source: bytes  # network identity (ZMQ/YMQ socket address)
    last_seen_at: float  # time.time() of last heartbeat
    capabilities: Dict[str, int]
    max_task_concurrency: int
    last_desired_total: int = 0
