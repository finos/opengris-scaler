from typing import Dict, List

from scaler.protocol.python.message import InformationSnapshot, WorkerAdapterCommand, WorkerAdapterHeartbeat
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerAdapterSnapshot,
    WorkerGroupCapabilities,
    WorkerGroupState,
)


class NoScalingController(ScalingController):
    def __init__(self):
        pass

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        adapter_heartbeat: WorkerAdapterHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
        worker_adapter_snapshots: Dict[bytes, WorkerAdapterSnapshot],
    ) -> List[WorkerAdapterCommand]:
        return []

    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups={})
