import logging
from typing import Dict, List, Tuple

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerAdapterCommand,
    WorkerAdapterCommandResponse,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerGroupCapabilities,
    WorkerGroupID,
    WorkerGroupState,
)
from scaler.utility.identifiers import WorkerID


class FixedElasticScalingController(ScalingController):
    """
    Scaling controller that identifies adapters by their max_worker_groups:
    - Primary adapter: max_worker_groups == 1, starts once and never shuts down
    - Secondary adapter: max_worker_groups > 1, elastic (starts/shuts down based on load)
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

        # Track if primary adapter has been scaled
        self._primary_started: bool = False

    def _is_primary_adapter(self, adapter_heartbeat: WorkerAdapterHeartbeat) -> bool:
        return adapter_heartbeat.max_worker_groups == 1

    def get_scaling_commands(
        self,
        snapshot: InformationSnapshot,
        adapter_heartbeat: WorkerAdapterHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
    ) -> List[WorkerAdapterCommand]:
        if not snapshot.workers:
            if snapshot.tasks:
                return self._create_start_commands(worker_groups, adapter_heartbeat)
            return []

        task_ratio = len(snapshot.tasks) / len(snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            return self._create_start_commands(worker_groups, adapter_heartbeat)
        elif task_ratio < self._lower_task_ratio:
            return self._create_shutdown_commands(snapshot, worker_groups, adapter_heartbeat)

        return []

    def on_scaling_command_response(
        self,
        response: WorkerAdapterCommandResponse,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
    ) -> Tuple[WorkerGroupState, WorkerGroupCapabilities]:
        updated_groups = dict(worker_groups)
        updated_capabilities = dict(worker_group_capabilities)

        if response.command == WorkerAdapterCommandType.StartWorkerGroup:
            if response.status == WorkerAdapterCommandResponse.Status.WorkerGroupTooMuch:
                logging.warning("Capacity exceeded, cannot start new worker group.")
                return updated_groups, updated_capabilities
            if response.status == WorkerAdapterCommandResponse.Status.Success:
                worker_group_id = WorkerGroupID(response.worker_group_id)
                updated_groups[worker_group_id] = [WorkerID(wid) for wid in response.worker_ids]
                updated_capabilities[worker_group_id] = response.capabilities
                logging.info(f"Started worker group: {worker_group_id.decode()}")

        elif response.command == WorkerAdapterCommandType.ShutdownWorkerGroup:
            if response.status == WorkerAdapterCommandResponse.Status.WorkerGroupIDNotFound:
                logging.error(f"Worker group {response.worker_group_id!r} not found.")
                return updated_groups, updated_capabilities
            if response.status == WorkerAdapterCommandResponse.Status.Success:
                worker_group_id = WorkerGroupID(response.worker_group_id)
                updated_groups.pop(worker_group_id, None)
                updated_capabilities.pop(worker_group_id, None)
                logging.info(f"Shutdown worker group: {worker_group_id.decode()}")

        return updated_groups, updated_capabilities

    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=worker_groups)

    def _create_start_commands(
        self, worker_groups: WorkerGroupState, adapter_heartbeat: WorkerAdapterHeartbeat
    ) -> List[WorkerAdapterCommand]:
        if self._is_primary_adapter(adapter_heartbeat):
            # Primary adapter: start once, never again
            if self._primary_started:
                return []
            self._primary_started = True
        else:
            # Secondary adapter: use adapter's max_worker_groups
            if len(worker_groups) >= adapter_heartbeat.max_worker_groups:
                logging.warning("Secondary adapter capacity reached, cannot start new worker group.")
                return []

        return [WorkerAdapterCommand.new_msg(worker_group_id=b"", command=WorkerAdapterCommandType.StartWorkerGroup)]

    def _create_shutdown_commands(
        self, snapshot: InformationSnapshot, worker_groups: WorkerGroupState, adapter_heartbeat: WorkerAdapterHeartbeat
    ) -> List[WorkerAdapterCommand]:
        # Primary adapter never shuts down
        if self._is_primary_adapter(adapter_heartbeat):
            return []

        worker_group_task_counts: Dict[WorkerGroupID, int] = {}
        for worker_group_id, worker_ids in worker_groups.items():
            total_queued = sum(snapshot.workers[wid].queued_tasks for wid in worker_ids if wid in snapshot.workers)
            worker_group_task_counts[worker_group_id] = total_queued

        if not worker_group_task_counts:
            return []

        # Shut down the group with fewest queued tasks
        worker_group_id = min(worker_group_task_counts, key=worker_group_task_counts.get)

        return [
            WorkerAdapterCommand.new_msg(
                worker_group_id=worker_group_id, command=WorkerAdapterCommandType.ShutdownWorkerGroup
            )
        ]
