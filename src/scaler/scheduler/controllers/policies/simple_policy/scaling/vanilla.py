import logging
from typing import Dict, Optional

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


class VanillaScalingController(ScalingController):
    """
    Stateful scaling controller that scales worker groups based on task-to-worker ratio.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

        # State owned by this controller
        self._worker_groups: WorkerGroupState = {}
        self._worker_group_capabilities: WorkerGroupCapabilities = {}

        # Command sender callback
        self._send_command: Optional[CommandSender] = None

    def register_command_sender(self, sender: CommandSender) -> None:
        self._send_command = sender

    async def on_snapshot(
        self, information_snapshot: InformationSnapshot, adapter_heartbeat: WorkerAdapterHeartbeat
    ) -> None:
        if self._send_command is None:
            return

        if not information_snapshot.workers:
            if information_snapshot.tasks:
                await self._scale_up(adapter_heartbeat)
            return

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            await self._scale_up(adapter_heartbeat)
        elif task_ratio < self._lower_task_ratio:
            await self._scale_down(information_snapshot)

    def on_command_response(self, response: WorkerAdapterCommandResponse) -> None:
        if response.command == WorkerAdapterCommandType.StartWorkerGroup:
            if response.status == WorkerAdapterCommandResponse.Status.WorkerGroupTooMuch:
                logging.warning("Capacity exceeded, cannot start new worker group.")
                return
            if response.status == WorkerAdapterCommandResponse.Status.Success:
                worker_group_id = WorkerGroupID(response.worker_group_id)
                self._worker_groups[worker_group_id] = [WorkerID(worker_id) for worker_id in response.worker_ids]
                self._worker_group_capabilities[worker_group_id] = response.capabilities
                logging.info(f"Started worker group: {worker_group_id.decode()}")
                return

        if response.command == WorkerAdapterCommandType.ShutdownWorkerGroup:
            if response.status == WorkerAdapterCommandResponse.Status.WorkerGroupIDNotFound:
                logging.error(f"Worker group with ID {response.worker_group_id!r} not found in adapter.")
                return
            if response.status == WorkerAdapterCommandResponse.Status.Success:
                worker_group_id = WorkerGroupID(response.worker_group_id)
                self._worker_groups.pop(worker_group_id, None)
                self._worker_group_capabilities.pop(worker_group_id, None)
                logging.info(f"Shutdown worker group: {worker_group_id.decode()}")
                return

        raise ValueError("Unknown Action")

    def get_status(self) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=self._worker_groups)

    async def _scale_up(self, adapter_heartbeat: WorkerAdapterHeartbeat) -> None:
        if len(self._worker_groups) >= adapter_heartbeat.max_worker_groups:
            return
        command = WorkerAdapterCommand.new_msg(worker_group_id=b"", command=WorkerAdapterCommandType.StartWorkerGroup)
        await self._send_command(command)

    async def _scale_down(self, information_snapshot: InformationSnapshot) -> None:
        worker_group_task_counts: Dict[WorkerGroupID, int] = {}
        for worker_group_id, worker_ids in self._worker_groups.items():
            total_queued = sum(
                information_snapshot.workers[worker_id].queued_tasks
                for worker_id in worker_ids
                if worker_id in information_snapshot.workers
            )
            worker_group_task_counts[worker_group_id] = total_queued

        if not worker_group_task_counts:
            logging.warning("No worker groups available to shut down. There might be statically provisioned workers.")
            return

        worker_group_id = min(worker_group_task_counts, key=worker_group_task_counts.get)
        command = WorkerAdapterCommand.new_msg(
            worker_group_id=worker_group_id, command=WorkerAdapterCommandType.ShutdownWorkerGroup
        )
        await self._send_command(command)
