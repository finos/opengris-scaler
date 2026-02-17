import dataclasses
import enum
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


class ScalingControllerStrategy(enum.Enum):
    NO = "no"
    VANILLA = "vanilla"
    FIXED_ELASTIC = "fixed_elastic"
    CAPABILITY = "capability"

    def __str__(self):
        return self.name
