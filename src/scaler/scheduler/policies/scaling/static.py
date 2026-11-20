from typing import Dict, List, Optional

from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.utility import build_set_desired_command
from scaler.scheduler.types import ClusterState
from scaler.utility.identifiers import WorkerManagerID


class StaticScalingPolicy(ScalingPolicy):
    """Requests a fixed task concurrency from each worker manager, whatever the load.

    A manager named in `task_concurrency_by_manager` gets its own count, any other gets `task_concurrency`.
    With no count, a manager is asked for the maximum task concurrency it advertises, which also caps every count.
    """

    def __init__(self, task_concurrency: Optional[int], task_concurrency_by_manager: Dict[WorkerManagerID, int]):
        self._task_concurrency = task_concurrency
        self._task_concurrency_by_manager = task_concurrency_by_manager

    def get_scaling_commands(
        self, worker_manager_id: WorkerManagerID, cluster_state: ClusterState
    ) -> List[WorkerManagerCommand]:
        info = cluster_state.managers[worker_manager_id]
        desired = info.max_task_concurrency or -1
        task_concurrency = self._task_concurrency_by_manager.get(worker_manager_id, self._task_concurrency)
        if task_concurrency is not None:
            desired = min(task_concurrency, desired)
        return [build_set_desired_command([({}, desired)])]
