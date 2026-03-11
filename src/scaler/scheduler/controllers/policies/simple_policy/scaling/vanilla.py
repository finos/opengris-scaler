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
    WorkerManagerSnapshot,
)


class VanillaScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that scales worker groups based on task-to-worker ratio.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        if not information_snapshot.workers:
            if information_snapshot.tasks:
                return self._create_start_commands(worker_groups, worker_manager_heartbeat)
            return []

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(worker_groups, worker_manager_heartbeat)
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(information_snapshot, worker_groups)

        return []

    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=worker_groups)

    def _create_start_commands(
        self, worker_groups: WorkerGroupState, worker_manager_heartbeat: WorkerManagerHeartbeat
    ) -> List[WorkerManagerCommand]:
        if len(worker_groups) >= worker_manager_heartbeat.max_worker_groups:
            return []
        return [WorkerManagerCommand.new_msg(worker_group_id=b"", command=WorkerManagerCommandType.StartWorkerGroup)]

    def _create_shutdown_commands(
        self, information_snapshot: InformationSnapshot, worker_groups: WorkerGroupState
    ) -> List[WorkerManagerCommand]:
        worker_group_task_counts: Dict[WorkerGroupID, int] = {}
        for worker_group_id, worker_ids in worker_groups.items():
            total_queued = sum(
                information_snapshot.workers[worker_id].queued_tasks
                for worker_id in worker_ids
                if worker_id in information_snapshot.workers
            )
            worker_group_task_counts[worker_group_id] = total_queued

        if not worker_group_task_counts:
            logging.warning("No worker groups available to shut down. There might be statically provisioned workers.")
            return []

        worker_group_id = min(worker_group_task_counts, key=worker_group_task_counts.get)
        return [
            WorkerManagerCommand.new_msg(
                worker_group_id=worker_group_id, command=WorkerManagerCommandType.ShutdownWorkerGroup
            )
        ]
