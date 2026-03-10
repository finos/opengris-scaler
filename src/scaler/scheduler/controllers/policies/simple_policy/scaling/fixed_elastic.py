import logging
from typing import Dict, List

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerManagerCommand,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerGroupCapabilities,
    WorkerGroupID,
    WorkerGroupState,
)


class FixedElasticScalingPolicy(ScalingPolicy):
    """
    Scaling policy that identifies adapters by their max_worker_groups:
    - Primary adapter: max_worker_groups == 1, starts once and never shuts down
    - Secondary adapter: max_worker_groups > 1, elastic (starts/shuts down based on load)

    Note: this policy is not fully stateless due to ``_primary_started``
    tracking whether the primary adapter has already been started.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

        # Track if primary adapter has been scaled
        self._primary_started: bool = False

    def _is_primary_adapter(self, worker_manager_heartbeat: WorkerManagerHeartbeat) -> bool:
        return worker_manager_heartbeat.max_worker_groups == 1

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
    ) -> List[WorkerManagerCommand]:
        if not information_snapshot.workers:
            if information_snapshot.tasks:
                return self._create_start_commands(worker_groups, worker_manager_heartbeat)
            return []

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(worker_groups, worker_manager_heartbeat)
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(information_snapshot, worker_groups, worker_manager_heartbeat)

        return []

    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=worker_groups)

    def _create_start_commands(
        self, worker_groups: WorkerGroupState, worker_manager_heartbeat: WorkerManagerHeartbeat
    ) -> List[WorkerManagerCommand]:
        if self._is_primary_adapter(worker_manager_heartbeat):
            # Primary adapter: start once, never again
            if self._primary_started:
                return []
            self._primary_started = True
        else:
            # Secondary adapter: use adapter's max_worker_groups
            if len(worker_groups) >= worker_manager_heartbeat.max_worker_groups:
                logging.warning("Secondary adapter capacity reached, cannot start new worker group.")
                return []

        return [WorkerManagerCommand.new_msg(worker_group_id=b"", command=WorkerManagerCommandType.StartWorkerGroup)]

    def _create_shutdown_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_groups: WorkerGroupState,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
    ) -> List[WorkerManagerCommand]:
        # Primary adapter never shuts down
        if self._is_primary_adapter(worker_manager_heartbeat):
            return []

        worker_group_task_counts: Dict[WorkerGroupID, int] = {}
        for worker_group_id, worker_ids in worker_groups.items():
            total_queued = sum(
                information_snapshot.workers[wid].queued_tasks
                for wid in worker_ids
                if wid in information_snapshot.workers
            )
            worker_group_task_counts[worker_group_id] = total_queued

        if not worker_group_task_counts:
            return []

        # Shut down the group with fewest queued tasks
        worker_group_id = min(worker_group_task_counts, key=worker_group_task_counts.get)

        return [
            WorkerManagerCommand.new_msg(
                worker_group_id=worker_group_id, command=WorkerManagerCommandType.ShutdownWorkerGroup
            )
        ]
