import dataclasses
import enum
from typing import Dict, List

from scaler.utility.identifiers import WorkerID

WorkerGroupID = bytes


@dataclasses.dataclass
class WorkerGroupInfo:
    worker_ids: List[WorkerID]
    capabilities: Dict[str, int]


# Type aliases for state owned by WorkerManagerController
WorkerGroupState = Dict[WorkerGroupID, List[WorkerID]]
WorkerGroupCapabilities = Dict[WorkerGroupID, Dict[str, int]]


@dataclasses.dataclass(frozen=True)
class WorkerManagerSnapshot:
    """Immutable snapshot of a worker manager's state, passed to stateless scaling policies."""

    worker_manager_id: bytes
    max_worker_groups: int
    worker_group_count: int
    last_seen_s: float  # time.time() epoch seconds of last heartbeat


class ScalingPolicyStrategy(enum.Enum):
    NO = "no"
    VANILLA = "vanilla"
    FIXED_ELASTIC = "fixed_elastic"
    CAPABILITY = "capability"

    def __str__(self):
        return self.name
