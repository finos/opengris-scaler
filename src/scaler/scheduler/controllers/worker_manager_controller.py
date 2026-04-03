import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

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
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
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

        # Track capabilities per manager: source -> capabilities dict
        self._manager_capabilities: Dict[bytes, Dict[str, int]] = defaultdict(dict)

        # Reverse map: worker_manager_id -> source (for duplicate detection)
        self._manager_id_to_source: Dict[bytes, bytes] = {}

        # Sources that have reported TooManyWorkers: maps source -> worker count at the time
        # TooManyWorkers was received. Suppress new StartWorkers until the scheduler's view of
        # managed workers grows beyond that baseline, meaning at least one booting instance has
        # sent its first heartbeat and the ORB adapter slot is no longer occupied by a pending boot.
        self._at_capacity_baseline: Dict[bytes, int] = {}

    def register(self, binder: AsyncBinder, task_controller: TaskController, worker_controller: WorkerController):
        self._binder = binder
        self._task_controller = task_controller
        self._worker_controller = worker_controller

    async def on_heartbeat(self, source: bytes, heartbeat: WorkerManagerHeartbeat):
        if source not in self._manager_alive_since:
            manager_id = heartbeat.worker_manager_id
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

        await self._binder.send(source, WorkerManagerHeartbeatEcho.new_msg())

        information_snapshot = self._build_snapshot()

        # Get managed worker IDs from worker controller (heartbeat-based live truth)
        managed_worker_ids = self._worker_controller.get_workers_by_manager_id(heartbeat.worker_manager_id)
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

        # If this manager previously reported TooManyWorkers, suppress new StartWorkers requests
        # until the scheduler's worker count grows beyond the baseline recorded at that time.
        # This handles the visibility gap where the ORB adapter has created instances that have
        # not yet sent their first heartbeat to the scheduler.
        # ShutdownWorkers commands must never be suppressed, the baseline only applies to scale-up.
        if source in self._at_capacity_baseline:
            if len(managed_worker_ids) > self._at_capacity_baseline[source]:
                del self._at_capacity_baseline[source]
            else:
                commands = [c for c in commands if c.command != WorkerManagerCommandType.StartWorkers]

        for command in commands:
            await self._send_command(source, command)

    async def on_command_response(self, source: bytes, response: WorkerManagerCommandResponse):
        """Called by scheduler event loop when WorkerManagerCommandResponse is received."""
        pending = self._pending_commands.pop(source, None)
        if pending is None:
            logging.warning(f"Received response from {source!r} but no pending command found")

        if response.command == WorkerManagerCommandType.StartWorkers:
            if response.status == WorkerManagerCommandResponse.Status.Success:
                if response.capabilities:
                    self._manager_capabilities[source] = dict(response.capabilities)
            else:
                logging.warning(f"StartWorkers failed: {response.status.name}")
                if response.status == WorkerManagerCommandResponse.Status.TooManyWorkers:
                    manager_entry = self._manager_alive_since.get(source)
                    if manager_entry is not None:
                        _, hb = manager_entry
                        baseline = len(self._worker_controller.get_workers_by_manager_id(hb.worker_manager_id))
                        self._at_capacity_baseline[source] = baseline

        elif response.command == WorkerManagerCommandType.ShutdownWorkers:
            if response.status != WorkerManagerCommandResponse.Status.Success:
                logging.warning(f"ShutdownWorkers failed: {response.status.name}")
            else:
                # Successful shutdown changes the capacity situation; clear any TooManyWorkers baseline
                # so subsequent StartWorkers requests are no longer suppressed.
                self._at_capacity_baseline.pop(source, None)

    async def routine(self):
        await self._clean_managers()

    def get_status(self) -> ScalingManagerStatus:
        managed_workers = self.get_managed_workers()
        base_status = self._policy_controller.get_scaling_status(managed_workers)

        now = time.time()
        details = []
        for source, (last_seen, heartbeat) in self._manager_alive_since.items():
            caps = heartbeat.capabilities
            caps_str = " ".join(sorted(caps.keys())) if caps else ""
            details.append(
                {
                    "worker_manager_id": heartbeat.worker_manager_id,
                    "identity": source.decode(errors="replace"),
                    "last_seen_s": min(int(now - last_seen), 255),
                    "max_task_concurrency": heartbeat.max_task_concurrency,
                    "capabilities": caps_str,
                }
            )

        return ScalingManagerStatus.new_msg(managed_workers=base_status.managed_workers, worker_manager_details=details)

    def get_managed_workers(self) -> Dict[bytes, List[WorkerID]]:
        """Return managed workers keyed by worker_manager_id (from heartbeat)."""
        result: Dict[bytes, List[WorkerID]] = {}
        for source, (_, heartbeat) in self._manager_alive_since.items():
            manager_id = heartbeat.worker_manager_id
            result[manager_id] = self._worker_controller.get_workers_by_manager_id(manager_id)
        return result

    async def _send_command(self, source: bytes, command: WorkerManagerCommand):
        self._pending_commands[source] = command
        await self._binder.send(source, command)

    def _build_manager_snapshots(self) -> Dict[bytes, WorkerManagerSnapshot]:
        """Build cross-manager snapshots from all known managers, keyed by worker_manager_id."""
        snapshots: Dict[bytes, WorkerManagerSnapshot] = {}
        for source, (last_seen, heartbeat) in self._manager_alive_since.items():
            manager_id = heartbeat.worker_manager_id
            worker_count = len(self._worker_controller.get_workers_by_manager_id(manager_id))
            snapshots[manager_id] = WorkerManagerSnapshot(
                worker_manager_id=manager_id,
                max_task_concurrency=heartbeat.max_task_concurrency,
                worker_count=worker_count,
                last_seen_s=last_seen,
                capabilities=heartbeat.capabilities,
            )
        return snapshots

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

        _, heartbeat = self._manager_alive_since[source]
        manager_id = heartbeat.worker_manager_id
        self._manager_id_to_source.pop(manager_id, None)

        logging.info(f"WorkerManager {source!r} disconnected")
        self._manager_alive_since.pop(source)
        self._pending_commands.pop(source, None)
        self._manager_capabilities.pop(source, None)
        self._at_capacity_baseline.pop(source, None)
