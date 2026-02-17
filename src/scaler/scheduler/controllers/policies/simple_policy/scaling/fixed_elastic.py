import logging
from typing import Dict, Optional, Set

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerAdapterCommand,
    WorkerAdapterCommandResponse,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import CommandSender, ScalingController
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

        # State owned by this controller
        self._worker_groups: WorkerGroupState = {}
        self._worker_group_capabilities: WorkerGroupCapabilities = {}
        self._worker_group_is_secondary: Set[WorkerGroupID] = set()

        # Command sender callback
        self._send_command: Optional[CommandSender] = None

    def _is_primary_adapter(self, adapter_heartbeat: WorkerAdapterHeartbeat) -> bool:
        return adapter_heartbeat.max_worker_groups == 1

    def register_command_sender(self, sender: CommandSender) -> None:
        self._send_command = sender

    async def on_snapshot(
        self, information_snapshot: InformationSnapshot, adapter_heartbeat: WorkerAdapterHeartbeat
    ) -> None:
        if self._send_command is None:
            return

        is_primary = self._is_primary_adapter(adapter_heartbeat)

        if not information_snapshot.workers:
            if information_snapshot.tasks:
                await self._scale_up(adapter_heartbeat, is_primary)
            return

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            await self._scale_up(adapter_heartbeat, is_primary)
        elif task_ratio < self._lower_task_ratio:
            await self._scale_down(information_snapshot, is_primary)

    def on_command_response(self, response: WorkerAdapterCommandResponse) -> None:
        if response.command == WorkerAdapterCommandType.StartWorkerGroup:
            if response.status == WorkerAdapterCommandResponse.Status.WorkerGroupTooMuch:
                logging.warning("Capacity exceeded, cannot start new worker group.")
                return
            if response.status == WorkerAdapterCommandResponse.Status.Success:
                worker_group_id = WorkerGroupID(response.worker_group_id)
                self._worker_groups[worker_group_id] = [WorkerID(wid) for wid in response.worker_ids]
                self._worker_group_capabilities[worker_group_id] = response.capabilities
                logging.info(f"Started worker group: {worker_group_id.decode()}")
                return

        elif response.command == WorkerAdapterCommandType.ShutdownWorkerGroup:
            if response.status == WorkerAdapterCommandResponse.Status.WorkerGroupIDNotFound:
                logging.error(f"Worker group {response.worker_group_id!r} not found.")
                return
            if response.status == WorkerAdapterCommandResponse.Status.Success:
                worker_group_id = WorkerGroupID(response.worker_group_id)
                self._worker_groups.pop(worker_group_id, None)
                self._worker_group_capabilities.pop(worker_group_id, None)
                self._worker_group_is_secondary.discard(worker_group_id)
                logging.info(f"Shutdown worker group: {worker_group_id.decode()}")
                return

    def get_status(self) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=self._worker_groups)

    async def _scale_up(self, adapter_heartbeat: WorkerAdapterHeartbeat, is_primary: bool) -> None:
        if is_primary:
            # Primary adapter: start once, never again
            if self._primary_started:
                return
            self._primary_started = True
        else:
            # Secondary adapter: use adapter's max_worker_groups
            if len(self._worker_groups) >= adapter_heartbeat.max_worker_groups:
                logging.warning("Secondary adapter capacity reached, cannot start new worker group.")
                return

        command = WorkerAdapterCommand.new_msg(worker_group_id=b"", command=WorkerAdapterCommandType.StartWorkerGroup)
        await self._send_command(command)

    async def _scale_down(self, information_snapshot: InformationSnapshot, is_primary: bool) -> None:
        # Primary adapter never shuts down
        if is_primary:
            return

        worker_group_task_counts: Dict[WorkerGroupID, int] = {}
        for worker_group_id, worker_ids in self._worker_groups.items():
            # Skip primary worker groups
            if worker_group_id not in self._worker_group_is_secondary:
                continue
            total_queued = sum(
                information_snapshot.workers[wid].queued_tasks
                for wid in worker_ids
                if wid in information_snapshot.workers
            )
            worker_group_task_counts[worker_group_id] = total_queued

        if not worker_group_task_counts:
            return

        # Shut down the group with fewest queued tasks
        worker_group_id = min(worker_group_task_counts, key=worker_group_task_counts.get)

        command = WorkerAdapterCommand.new_msg(
            worker_group_id=worker_group_id, command=WorkerAdapterCommandType.ShutdownWorkerGroup
        )
        await self._send_command(command)
