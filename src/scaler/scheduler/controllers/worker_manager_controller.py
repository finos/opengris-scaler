import logging
import time
from typing import Dict, Optional, Tuple

from scaler.config.defaults import DEFAULT_WORKER_MANAGER_TIMEOUT_SECONDS
from scaler.io.mixins import AsyncBinder
from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import PolicyController, TaskController, WorkerController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerGroupID,
    WorkerGroupInfo,
    WorkerGroupState,
)
from scaler.utility.identifiers import WorkerID
from scaler.utility.mixins import Looper, Reporter


class WorkerManagerController(Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController, policy_controller: PolicyController):
        self._config_controller = config_controller
        self._policy_controller = policy_controller

        self._binder: Optional[AsyncBinder] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None

        # Track worker manager heartbeats: source -> (last_seen_time, heartbeat)
        self._manager_alive_since: Dict[bytes, Tuple[float, WorkerManagerHeartbeat]] = {}

        # Track last command sent to each source
        self._pending_commands: Dict[bytes, WorkerManagerCommand] = {}

        # Track worker groups per worker manager: source -> (worker_group_id -> info)
        self._manager_worker_groups: Dict[bytes, Dict[WorkerGroupID, WorkerGroupInfo]] = {}

    def register(self, binder: AsyncBinder, task_controller: TaskController, worker_controller: WorkerController):
        self._binder = binder
        self._task_controller = task_controller
        self._worker_controller = worker_controller

    async def on_heartbeat(self, source: bytes, heartbeat: WorkerManagerHeartbeat):
        if source not in self._manager_alive_since:
            logging.info(f"WorkerManager {source!r} connected")
            self._manager_worker_groups[source] = {}

        self._manager_alive_since[source] = (time.time(), heartbeat)

        await self._binder.send(source, WorkerManagerHeartbeatEcho.new_msg())

        information_snapshot = self._build_snapshot()

        # Get worker groups for this worker manager
        worker_manager_groups = self._manager_worker_groups[source]
        worker_groups = {gid: info.worker_ids for gid, info in worker_manager_groups.items()}
        worker_group_capabilities = {gid: info.capabilities for gid, info in worker_manager_groups.items()}

        commands = self._policy_controller.get_scaling_commands(
            information_snapshot, heartbeat, worker_groups, worker_group_capabilities
        )

        for command in commands:
            await self._send_command(source, command)

    async def on_command_response(self, source: bytes, response: WorkerManagerCommandResponse):
        """Called by scheduler event loop when WorkerManagerCommandResponse is received."""
        pending = self._pending_commands.pop(source, None)
        if pending is None:
            logging.warning(f"Received response from {source!r} but no pending command found")

        if response.command == WorkerManagerCommandType.StartWorkerGroup:
            if response.status == WorkerManagerCommandResponse.Status.Success:
                self._manager_worker_groups[source][bytes(response.worker_group_id)] = WorkerGroupInfo(
                    worker_ids=[WorkerID(wid) for wid in response.worker_ids], capabilities=dict(response.capabilities)
                )
            else:
                logging.warning(f"StartWorkerGroup failed: {response.status.name}")

        elif response.command == WorkerManagerCommandType.ShutdownWorkerGroup:
            if response.status == WorkerManagerCommandResponse.Status.Success:
                self._manager_worker_groups[source].pop(bytes(response.worker_group_id), None)
            else:
                logging.warning(f"ShutdownWorkerGroup failed: {response.status.name}")

    async def routine(self):
        await self._clean_managers()

    def get_status(self) -> ScalingManagerStatus:
        return self._policy_controller.get_scaling_status(self.get_worker_groups())

    def get_worker_groups(self) -> WorkerGroupState:
        """Return aggregated worker groups from all worker managers."""
        result: WorkerGroupState = {}
        for worker_manager_groups in self._manager_worker_groups.values():
            for gid, info in worker_manager_groups.items():
                result[gid] = info.worker_ids
        return result

    async def _send_command(self, source: bytes, command: WorkerManagerCommand):
        self._pending_commands[source] = command
        await self._binder.send(source, command)

    def _build_snapshot(self) -> InformationSnapshot:
        tasks = self._task_controller._task_id_to_task  # type: ignore # noqa
        workers = {
            worker_id: worker_heartbeat
            for worker_id, (_, worker_heartbeat) in self._worker_controller._worker_alive_since.items()  # type: ignore # noqa
        }
        return InformationSnapshot(tasks=tasks, workers=workers)

    async def _clean_managers(self):
        """Clean up dead worker managers that have not sent heartbeats."""
        now = time.time()
        timeout_seconds = DEFAULT_WORKER_MANAGER_TIMEOUT_SECONDS
        dead_managers = [
            source
            for source, (alive_since, _) in self._manager_alive_since.items()
            if now - alive_since > timeout_seconds
        ]
        for dead_manager in dead_managers:
            await self._disconnect_manager(dead_manager)

    async def _disconnect_manager(self, source: bytes):
        if source not in self._manager_alive_since:
            return

        logging.info(f"WorkerManager {source!r} disconnected")
        self._manager_alive_since.pop(source)
        self._pending_commands.pop(source, None)
        self._manager_worker_groups.pop(source, None)
