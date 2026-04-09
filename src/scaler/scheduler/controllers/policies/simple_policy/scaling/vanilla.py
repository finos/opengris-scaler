from math import ceil
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
        snapshot = worker_manager_snapshots.get(worker_manager_heartbeat.worker_manager_id)
        pending = snapshot.pending_worker_count if snapshot else 0

        if not information_snapshot.workers:
            if information_snapshot.tasks:
                return self._create_start_commands(managed_worker_ids, worker_manager_heartbeat, pending)
            return []

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(managed_worker_ids, worker_manager_heartbeat, pending)
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(information_snapshot, managed_worker_ids)

        return []

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(managed_workers=managed_workers)

    def _create_start_commands(
        self,
        managed_worker_ids: List[WorkerID],
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        pending_worker_count: int = 0,
    ) -> List[WorkerManagerCommand]:
        max_concurrency = worker_manager_heartbeat.max_task_concurrency
        if max_concurrency != -1 and len(managed_worker_ids) + pending_worker_count >= max_concurrency:
            return []
        return [WorkerManagerCommand.new_msg(worker_ids=[], command=WorkerManagerCommandType.StartWorkers)]

    def _create_shutdown_commands(
        self, information_snapshot: InformationSnapshot, managed_worker_ids: List[WorkerID]
    ) -> List[WorkerManagerCommand]:
        if not managed_worker_ids:
            return []

        workers_with_load = []
        for wid in managed_worker_ids:
            if wid in information_snapshot.workers:
                workers_with_load.append((wid, information_snapshot.workers[wid].queued_tasks))
        workers_with_load.sort(key=lambda x: x[1])

        if not workers_with_load:
            return []

        task_count = len(information_snapshot.tasks)
        if task_count == 0:
            min_keep = 0
        else:
            min_keep = max(1, ceil(task_count / self._upper_task_ratio))

        to_shutdown = len(workers_with_load) - min_keep
        if to_shutdown <= 0:
            return []

        shutdown_ids = [bytes(wid) for wid, _ in workers_with_load[:to_shutdown]]
        return [WorkerManagerCommand.new_msg(worker_ids=shutdown_ids, command=WorkerManagerCommandType.ShutdownWorkers)]
