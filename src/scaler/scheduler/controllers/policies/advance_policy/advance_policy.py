from typing import Dict, List, Optional, Set

from scaler.protocol.python.message import InformationSnapshot, Task, WorkerAdapterCommand, WorkerAdapterHeartbeat
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.advance_policy.scaling.types import AdvanceScalingControllerStrategy
from scaler.scheduler.controllers.policies.advance_policy.scaling.utility import create_advance_scaling_controller
from scaler.scheduler.controllers.policies.mixins import ScalerPolicy
from scaler.scheduler.controllers.policies.simple_policy.allocation.types import AllocatePolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.allocation.utility import create_allocate_policy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerAdapterSnapshot,
    WorkerGroupCapabilities,
    WorkerGroupState,
)
from scaler.utility.identifiers import TaskID, WorkerID


class AdvancePolicy(ScalerPolicy):
    """
    Policy that supports cross-adapter scaling strategies such as waterfall.

    Cross-adapter state (worker_adapter_snapshots) is built by WorkerAdapterController
    and passed through the call chain.
    """

    def __init__(self, policy_kv: Dict[str, str], scaling_config: str):
        required_keys = {"allocate", "scaling"}
        missing_keys = required_keys - policy_kv.keys()
        if missing_keys:
            raise ValueError(f"AdvancePolicy requires keys {required_keys}, missing: {missing_keys}")

        self._allocation_policy = create_allocate_policy(AllocatePolicyStrategy(policy_kv["allocate"]))
        self._scaling_controller = create_advance_scaling_controller(
            AdvanceScalingControllerStrategy(policy_kv["scaling"]), scaling_config
        )

    def add_worker(self, worker: WorkerID, capabilities: Dict[str, int], queue_size: int) -> bool:
        return self._allocation_policy.add_worker(worker, capabilities, queue_size)

    def remove_worker(self, worker: WorkerID) -> List[TaskID]:
        return self._allocation_policy.remove_worker(worker)

    def get_worker_ids(self) -> Set[WorkerID]:
        return self._allocation_policy.get_worker_ids()

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._allocation_policy.get_worker_by_task_id(task_id)

    def balance(self) -> Dict[WorkerID, List[TaskID]]:
        return self._allocation_policy.balance()

    def assign_task(self, task: Task) -> WorkerID:
        return self._allocation_policy.assign_task(task)

    def remove_task(self, task_id: TaskID) -> WorkerID:
        return self._allocation_policy.remove_task(task_id)

    def has_available_worker(self, capabilities: Optional[Dict[str, int]] = None) -> bool:
        return self._allocation_policy.has_available_worker(capabilities)

    def statistics(self) -> Dict:
        return self._allocation_policy.statistics()

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        adapter_heartbeat: WorkerAdapterHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
        worker_adapter_snapshots: Dict[bytes, WorkerAdapterSnapshot],
    ) -> List[WorkerAdapterCommand]:
        return self._scaling_controller.get_scaling_commands(
            information_snapshot, adapter_heartbeat, worker_groups, worker_group_capabilities, worker_adapter_snapshots
        )

    def get_scaling_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return self._scaling_controller.get_status(worker_groups)
