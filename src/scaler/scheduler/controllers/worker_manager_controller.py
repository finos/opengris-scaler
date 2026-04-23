import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from scaler.config.defaults import DEFAULT_WORKER_MANAGER_TIMEOUT_SECONDS
from scaler.io.mixins import AsyncBinder
from scaler.protocol.capnp import (
    ScalingManagerStatus,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import PolicyController, TaskController, WorkerController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status
from scaler.utility.identifiers import WorkerID
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.snapshot import InformationSnapshot


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

        # Track capabilities per manager: source -> capabilities dict
        self._manager_capabilities: Dict[bytes, Dict[str, int]] = defaultdict(dict)

        # Reverse map: worker_manager_id -> source (for duplicate detection)
        self._manager_id_to_source: Dict[bytes, bytes] = {}

        # Pending worker tracking: workers launched (StartWorkers success) but not yet connected.
        # _pending_worker_count[source] is decremented as workers appear in the worker controller.
        self._pending_worker_count: Dict[bytes, int] = {}
        self._last_worker_count: Dict[bytes, int] = {}

    def register(self, binder: AsyncBinder, task_controller: TaskController, worker_controller: WorkerController):
        self._binder = binder
        self._task_controller = task_controller
        self._worker_controller = worker_controller

    async def on_heartbeat(self, source: bytes, heartbeat: WorkerManagerHeartbeat):
        heartbeat.capabilities = capabilities_to_dict(heartbeat.capabilities)
        if source not in self._manager_alive_since:
            manager_id = heartbeat.workerManagerID
            existing_source = self._manager_id_to_source.get(manager_id)
            if existing_source is not None and existing_source != source:
                logging.warning(
                    f"Duplicate worker_manager_id {manager_id!r}: source {source!r} rejected, "
                    f"already registered by source {existing_source!r}"
                )
                return
            self._manager_id_to_source[manager_id] = source

            logging.info(f"WorkerManager {manager_id!r} connected")

        self._manager_alive_since[source] = (time.time(), heartbeat)

        await self._binder.send(source, WorkerManagerHeartbeatEcho())

        information_snapshot = self._build_snapshot()

        # Get managed worker IDs from worker controller (heartbeat-based live truth)
        managed_worker_ids = self._worker_controller.get_workers_by_manager_id(heartbeat.workerManagerID)

        # Update pending worker count: decrement for each worker that newly connected.
        worker_count = len(managed_worker_ids)
        prev_count = self._last_worker_count.get(source, 0)
        newly_connected = max(0, worker_count - prev_count)
        if newly_connected > 0:
            self._pending_worker_count[source] = max(0, self._pending_worker_count.get(source, 0) - newly_connected)
        self._last_worker_count[source] = worker_count

        managed_worker_capabilities = self._manager_capabilities[source]

        # Build cross-manager snapshots from all known managers
        worker_manager_snapshots = self._build_manager_snapshots()

        # Wait for the previous command to complete before sending another.
        # Worker managers can take a long time to fulfill commands (e.g. ORB polls for instance IDs),
        # so sending a new command before the response arrives causes duplicate work and errors.
        if source in self._pending_commands:
            return

        commands = self._policy_controller.get_scaling_commands(
            information_snapshot, heartbeat, managed_worker_ids, managed_worker_capabilities, worker_manager_snapshots
        )

        for command in commands:
            await self._send_command(source, command)

    async def on_command_response(self, source: bytes, response: WorkerManagerCommandResponse):
        """Called by scheduler event loop when WorkerManagerCommandResponse is received."""
        response_capabilities = capabilities_to_dict(getattr(response, "capabilities", {}))
        pending = self._pending_commands.pop(source, None)
        if pending is None:
            logging.warning(f"Received response from {source!r} but no pending command found")

        if response.command == WorkerManagerCommandType.startWorkers:
            if response.status == WorkerManagerCommandResponse.Status.success:
                if response_capabilities:
                    self._manager_capabilities[source] = response_capabilities
                self._pending_worker_count[source] = self._pending_worker_count.get(source, 0) + 1
            else:
                logging.warning(f"StartWorkers failed: {response.status._as_str()}")

        elif response.command == WorkerManagerCommandType.shutdownWorkers:
            if response.status != WorkerManagerCommandResponse.Status.success:
                logging.warning(f"ShutdownWorkers failed: {response.status._as_str()}")

    async def routine(self):
        await self._clean_managers()

    def get_status(self) -> ScalingManagerStatus:
        managed_workers = self.get_managed_workers()

        now = time.time()
        details = []
        for source, (last_seen, heartbeat) in self._manager_alive_since.items():
            caps = heartbeat.capabilities
            caps_str = " ".join(sorted(capabilities_to_dict(caps).keys())) if caps else ""
            details.append(
                {
                    "worker_manager_id": heartbeat.workerManagerID,
                    "identity": source.decode(errors="replace"),
                    "last_seen_s": min(int(now - last_seen), 255),
                    "max_task_concurrency": heartbeat.maxTaskConcurrency,
                    "capabilities": caps_str,
                    "pending_workers": self._pending_worker_count.get(source, 0),
                }
            )

        return build_scaling_manager_status(managed_workers, details)

    def get_managed_workers(self) -> Dict[bytes, List[WorkerID]]:
        """Return managed workers keyed by worker_manager_id (from heartbeat)."""
        result: Dict[bytes, List[WorkerID]] = {}
        for source, (_, heartbeat) in self._manager_alive_since.items():
            manager_id = heartbeat.workerManagerID
            result[manager_id] = self._worker_controller.get_workers_by_manager_id(manager_id)
        return result

    async def _send_command(self, source: bytes, command: WorkerManagerCommand):
        # setDesiredTaskConcurrency is declarative and worker managers may silently ignore it
        # during the transition. No response is guaranteed, so it must bypass the single-
        # in-flight gate; otherwise the gate would wedge and block all subsequent scaling.
        if command.command != WorkerManagerCommandType.setDesiredTaskConcurrency:
            self._pending_commands[source] = command
        await self._binder.send(source, command)

    def _build_manager_snapshots(self) -> Dict[bytes, WorkerManagerSnapshot]:
        """Build cross-manager snapshots from all known managers, keyed by worker_manager_id."""
        snapshots: Dict[bytes, WorkerManagerSnapshot] = {}
        for source, (last_seen, heartbeat) in self._manager_alive_since.items():
            manager_id = heartbeat.workerManagerID
            worker_count = len(self._worker_controller.get_workers_by_manager_id(manager_id))
            snapshots[manager_id] = WorkerManagerSnapshot(
                worker_manager_id=manager_id,
                max_task_concurrency=heartbeat.maxTaskConcurrency,
                worker_count=worker_count,
                pending_worker_count=self._pending_worker_count.get(source, 0),
                last_seen_s=last_seen,
                capabilities=heartbeat.capabilities,
            )
        return snapshots

    def _build_snapshot(self) -> InformationSnapshot:
        tasks = self._task_controller._task_id_to_task  # type: ignore # noqa
        workers = {
            worker_id: worker_heartbeat
            for worker_id, (
                _,
                worker_heartbeat,
            ) in self._worker_controller._worker_alive_since.items()  # type: ignore # noqa
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

        _, heartbeat = self._manager_alive_since[source]
        manager_id = heartbeat.workerManagerID
        self._manager_id_to_source.pop(manager_id, None)

        logging.info(f"WorkerManager {source!r} disconnected")
        self._manager_alive_since.pop(source)
        self._pending_commands.pop(source, None)
        self._manager_capabilities.pop(source, None)
        self._pending_worker_count.pop(source, None)
        self._last_worker_count.pop(source, None)
