from typing import Dict, List, Optional, Set

from scaler.protocol.python.message import InformationSnapshot, Task, WorkerAdapterCommand, WorkerAdapterHeartbeat
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.mixins import PolicyController
from scaler.scheduler.controllers.policies.library.types import (
    WorkerAdapterSnapshot,
    WorkerGroupCapabilities,
    WorkerGroupState,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.types import AllocatePolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.allocation.utility import create_allocate_policy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingPolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.scaling.utility import create_scaling_policy
from scaler.utility.identifiers import TaskID, WorkerID


class VanillaPolicyController(PolicyController):
    def __init__(self, policy_content: str):
        policy_kv = {
            k.strip(): v.strip()
            for item in policy_content.split(";")
            if "=" in item
            for k, v in [item.split("=", 1)]
        }

        allocate = "allocate"
        scaling = "scaling"
        if policy_kv.keys() != {allocate, scaling}:
            raise ValueError(f"VanillaPolicyController only supports {allocate} and {scaling}, got {policy_kv.keys()}")

        self._allocation_policy = create_allocate_policy(AllocatePolicyStrategy(policy_kv[allocate]))
        self._scaling_policy = create_scaling_policy(ScalingPolicyStrategy(policy_kv[scaling]))

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
        return self._scaling_policy.get_scaling_commands(
            information_snapshot, adapter_heartbeat, worker_groups, worker_group_capabilities, worker_adapter_snapshots
        )

    def get_scaling_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return self._scaling_policy.get_status(worker_groups)
