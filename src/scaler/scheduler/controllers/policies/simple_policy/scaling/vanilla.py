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
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import WorkerID


class VanillaScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that scales workers based on task-to-worker ratio.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        managed_worker_capabilities: Dict[str, int],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        if not information_snapshot.workers:
            if information_snapshot.tasks:
                return self._create_start_commands(managed_worker_ids, worker_manager_heartbeat)
            return []

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(managed_worker_ids, worker_manager_heartbeat)
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(information_snapshot, managed_worker_ids)

        return []

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(managed_workers=managed_workers)

    def _create_start_commands(
        self, managed_worker_ids: List[WorkerID], worker_manager_heartbeat: WorkerManagerHeartbeat
    ) -> List[WorkerManagerCommand]:
        if len(managed_worker_ids) >= worker_manager_heartbeat.max_workers:
            return []
        return [WorkerManagerCommand.new_msg(worker_ids=[], command=WorkerManagerCommandType.StartWorkers)]

    def _create_shutdown_commands(
        self, information_snapshot: InformationSnapshot, managed_worker_ids: List[WorkerID]
    ) -> List[WorkerManagerCommand]:
        if not managed_worker_ids:
            logging.warning("No workers available to shut down. There might be statically provisioned workers.")
            return []

        # Find the individual worker with fewest queued tasks
        least_busy_wid = None
        min_queued = float("inf")
        for wid in managed_worker_ids:
            if wid in information_snapshot.workers:
                queued = information_snapshot.workers[wid].queued_tasks
                if queued < min_queued:
                    min_queued = queued
                    least_busy_wid = wid

        if least_busy_wid is None:
            return []

        return [
            WorkerManagerCommand.new_msg(
                worker_ids=[bytes(least_busy_wid)], command=WorkerManagerCommandType.ShutdownWorkers
            )
        ]
