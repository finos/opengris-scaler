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


class FixedElasticScalingPolicy(ScalingPolicy):
    """
    Scaling policy that identifies managers by their max_workers:
    - Primary manager: max_workers == 1, starts once and never shuts down
    - Secondary manager: max_workers > 1, elastic (starts/shuts down based on load)

    Note: this policy is not fully stateless due to ``_primary_started``
    tracking whether the primary manager has already been started.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

        # Track if primary manager has been scaled
        self._primary_started: bool = False

    def _is_primary_manager(self, worker_manager_heartbeat: WorkerManagerHeartbeat) -> bool:
        return worker_manager_heartbeat.max_workers == 1

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
            return self._create_shutdown_commands(information_snapshot, managed_worker_ids, worker_manager_heartbeat)

        return []

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(managed_workers=managed_workers)

    def _create_start_commands(
        self, managed_worker_ids: List[WorkerID], worker_manager_heartbeat: WorkerManagerHeartbeat
    ) -> List[WorkerManagerCommand]:
        if self._is_primary_manager(worker_manager_heartbeat):
            # Primary manager: start once, never again
            if self._primary_started:
                return []
            self._primary_started = True
        else:
            # Secondary manager: use manager's max_workers
            if len(managed_worker_ids) >= worker_manager_heartbeat.max_workers:
                logging.warning("Secondary manager capacity reached, cannot start new worker.")
                return []

        return [WorkerManagerCommand.new_msg(worker_ids=[], command=WorkerManagerCommandType.StartWorkers)]

    def _create_shutdown_commands(
        self,
        information_snapshot: InformationSnapshot,
        managed_worker_ids: List[WorkerID],
        worker_manager_heartbeat: WorkerManagerHeartbeat,
    ) -> List[WorkerManagerCommand]:
        # Primary manager never shuts down
        if self._is_primary_manager(worker_manager_heartbeat):
            return []

        if not managed_worker_ids:
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
