import logging
import time
from typing import Dict, Optional, Tuple

from scaler.config.defaults import DEFAULT_WORKER_ADAPTER_TIMEOUT_SECONDS
from scaler.io.mixins import AsyncBinder
from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerAdapterCommand,
    WorkerAdapterCommandResponse,
    WorkerAdapterHeartbeat,
    WorkerAdapterHeartbeatEcho,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import TaskController, WorkerController
from scaler.scheduler.controllers.policies.mixins import ScalerPolicy
from scaler.utility.mixins import Looper, Reporter


class WorkerAdapterController(Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController, scaler_policy: ScalerPolicy):
        self._config_controller = config_controller
        self._scaler_policy = scaler_policy

        self._binder: Optional[AsyncBinder] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None

        # Track adapter heartbeats: source -> (last_seen_time, heartbeat)
        self._adapter_alive_since: Dict[bytes, Tuple[float, WorkerAdapterHeartbeat]] = {}

        # Track last command sent to each source
        self._pending_commands: Dict[bytes, WorkerAdapterCommand] = {}

        # Track current source for routing responses
        self._current_source: Optional[bytes] = None

    def register(self, binder: AsyncBinder, task_controller: TaskController, worker_controller: WorkerController):
        self._binder = binder
        self._task_controller = task_controller
        self._worker_controller = worker_controller

        # Register the command sender with the scaling policy
        self._scaler_policy.register_command_sender(self._send_command_to_current_source)

    async def _send_command_to_current_source(self, command: WorkerAdapterCommand) -> None:
        """Send a command to the current adapter source. Used as callback by ScalingController."""
        if self._current_source is None:
            logging.warning("No current source set, cannot send command")
            return
        await self._send_command(self._current_source, command)

    async def on_heartbeat(self, source: bytes, heartbeat: WorkerAdapterHeartbeat):
        if source not in self._adapter_alive_since:
            logging.info(f"WorkerAdapter {source!r} connected")

        self._adapter_alive_since[source] = (time.time(), heartbeat)

        await self._binder.send(source, WorkerAdapterHeartbeatEcho.new_msg())

        information_snapshot = self._build_snapshot()

        # Set current source for the callback
        self._current_source = source
        await self._scaler_policy.on_snapshot(information_snapshot, heartbeat)
        self._current_source = None

    async def on_command_response(self, source: bytes, response: WorkerAdapterCommandResponse):
        """Called by scheduler event loop when WorkerAdapterCommandResponse is received."""
        pending = self._pending_commands.pop(source, None)
        if pending is None:
            logging.warning(f"Received response from {source!r} but no pending command found")

        # Delegate to the policy to update its internal state
        self._scaler_policy.on_command_response(response)

    async def routine(self):
        await self._clean_adapters()

    def get_status(self) -> ScalingManagerStatus:
        return self._scaler_policy.get_scaling_status()

    async def _send_command(self, source: bytes, command: WorkerAdapterCommand):
        self._pending_commands[source] = command
        await self._binder.send(source, command)

    def _build_snapshot(self) -> InformationSnapshot:
        tasks = self._task_controller._task_id_to_task  # type: ignore # noqa
        workers = {
            worker_id: worker_heartbeat
            for worker_id, (_, worker_heartbeat) in self._worker_controller._worker_alive_since.items()  # type: ignore # noqa
        }
        return InformationSnapshot(tasks=tasks, workers=workers)

    async def _clean_adapters(self):
        """Clean up dead adapters that have not sent heartbeats."""
        now = time.time()
        timeout_seconds = DEFAULT_WORKER_ADAPTER_TIMEOUT_SECONDS
        dead_adapters = [
            source
            for source, (alive_since, _) in self._adapter_alive_since.items()
            if now - alive_since > timeout_seconds
        ]
        for dead_adapter in dead_adapters:
            await self._disconnect_adapter(dead_adapter)

    async def _disconnect_adapter(self, source: bytes):
        if source not in self._adapter_alive_since:
            return

        logging.info(f"WorkerAdapter {source!r} disconnected")
        self._adapter_alive_since.pop(source)
        self._pending_commands.pop(source, None)
