import dataclasses
from typing import Dict, List

from scaler.utility.identifiers import WorkerID

WorkerGroupID = bytes


@dataclasses.dataclass
class WorkerGroupInfo:
    worker_ids: List[WorkerID]
    capabilities: Dict[str, int]


# Type aliases for state owned by WorkerAdapterController
WorkerGroupState = Dict[WorkerGroupID, List[WorkerID]]
WorkerGroupCapabilities = Dict[WorkerGroupID, Dict[str, int]]


@dataclasses.dataclass(frozen=True)
class WorkerAdapterSnapshot:
    """Immutable snapshot of an adapter's state, passed to stateless scaling policies."""

    worker_manager_adapter_id: bytes
    max_worker_groups: int
    worker_group_count: int
    last_seen_s: float  # time.time() epoch seconds of last heartbeat
