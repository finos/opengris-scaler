from typing import Dict, List

from scaler.protocol.capnp import WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.policies.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class NoScalingPolicy(ScalingPolicy):
    def __init__(self):
        pass

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        return []
