import dataclasses
import logging
import time
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional

from scaler.config.defaults import DEFAULT_WORKER_MANAGER_TIMEOUT_SECONDS
from scaler.io.mixins import AsyncBinder
from scaler.protocol.capnp import (
    ScalingManagerStatus,
    WorkerManagerCommand,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import TaskController, WorkerController
from scaler.scheduler.controllers.worker_manager_utilities import build_scaling_manager_status
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.utility import build_cluster_state
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import WorkerID, WorkerManagerID
from scaler.utility.mixins import Looper, Reporter

logger = logging.getLogger(__name__)

UINT16_MAX = 2**16 - 1


@dataclasses.dataclass
class _WorkerManagerHolder:
    manager_id: WorkerManagerID
    source: bytes  # network identity (ZMQ/YMQ socket address)
    last_seen_at: float  # time.monotonic() of last heartbeat
    capabilities: Dict[str, int]
    max_task_concurrency: Optional[int]  # None = unlimited
    last_desired_total: int = 0


class WorkerManagerController(Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController, scaling_policy: ScalingPolicy):
        self._config_controller = config_controller
        self._scaling_policy = scaling_policy

        self._binder: Optional[AsyncBinder] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None

        self._managers: Dict[WorkerManagerID, _WorkerManagerHolder] = {}

    def register(self, binder: AsyncBinder, task_controller: TaskController, worker_controller: WorkerController):
        self._binder = binder
        self._task_controller = task_controller
        self._worker_controller = worker_controller

    async def on_heartbeat(self, source: bytes, heartbeat: WorkerManagerHeartbeat):
        manager_id = heartbeat.workerManagerID
        capabilities = capabilities_to_dict(heartbeat.capabilities)
        max_task_concurrency = heartbeat.maxTaskConcurrency if heartbeat.maxTaskConcurrency != -1 else None

        if manager_id not in self._managers:
            holder = _WorkerManagerHolder(
                manager_id=manager_id,
                source=source,
                last_seen_at=time.monotonic(),
                capabilities=capabilities,
                max_task_concurrency=max_task_concurrency,
            )
            self._managers[manager_id] = holder
            logger.info(f"{manager_id!r} connected")
        else:
            holder = self._managers[manager_id]
            if holder.source != source:
                logger.warning(
                    f"Duplicate {manager_id!r}: source {source!r} rejected, "
                    f"already registered by source {holder.source!r}"
                )
                return
            holder.last_seen_at = time.monotonic()
            holder.capabilities = capabilities
            holder.max_task_concurrency = max_task_concurrency

        await self._binder.send(source, WorkerManagerHeartbeatEcho(), detached=True)

        cluster_state = self._build_cluster_state()

        commands = self._scaling_policy.get_scaling_commands(worker_manager_id=manager_id, cluster_state=cluster_state)

        for command in commands:
            await self._send_command(source, command)

        holder.last_desired_total = _sum_desired_for_manager(commands, capabilities)

    async def routine(self):
        await self._clean_managers()

    def get_status(self) -> ScalingManagerStatus:
        managed_workers = self.get_managed_workers()

        now = time.monotonic()
        details = []
        for manager_id, holder in self._managers.items():
            caps_str = " ".join(sorted(holder.capabilities.keys())) if holder.capabilities else ""
            connected = len(self._worker_controller.get_workers_by_manager_id(manager_id))
            pending = max(0, holder.last_desired_total - connected)
            max_task_concurrency = holder.max_task_concurrency if holder.max_task_concurrency is not None else -1
            details.append(
                {
                    "worker_manager_id": manager_id,
                    "identity": holder.source.decode(errors="replace"),
                    "last_seen_seconds": min(int(now - holder.last_seen_at), UINT16_MAX),
                    "max_task_concurrency": max_task_concurrency,
                    "capabilities": caps_str,
                    "pending_workers": pending,
                }
            )

        return build_scaling_manager_status(managed_workers, details)

    def get_managed_workers(self) -> Dict[WorkerManagerID, List[WorkerID]]:
        """Return managed workers keyed by worker_manager_id (from heartbeat)."""
        result: Dict[WorkerManagerID, List[WorkerID]] = {}
        for manager_id in self._managers:
            result[manager_id] = self._worker_controller.get_workers_by_manager_id(manager_id)
        return result

    async def _send_command(self, source: bytes, command: WorkerManagerCommand):
        await self._binder.send(source, command, detached=True)

    def _build_worker_manager_infos(self, workers: Mapping[WorkerID, Any]) -> Dict[WorkerManagerID, WorkerManagerInfo]:
        """Build frozen manager summaries with read-only views of the holders' shared capability dictionaries."""
        return {
            manager_id: WorkerManagerInfo(
                manager_id=manager_id,
                max_task_concurrency=holder.max_task_concurrency,
                worker_count=sum(
                    1
                    for worker_id in self._worker_controller.get_workers_by_manager_id(manager_id)
                    if not workers[worker_id].draining
                ),
                capabilities=MappingProxyType(holder.capabilities),
            )
            for manager_id, holder in self._managers.items()
        }

    def _build_cluster_state(self) -> ClusterState:
        tasks = self._task_controller._task_id_to_task  # type: ignore # noqa
        workers = {
            worker_id: worker_heartbeat
            for worker_id, (_, worker_heartbeat) in self._worker_controller._worker_alive_since.items()  # type: ignore # noqa
        }
        worker_manager_infos = self._build_worker_manager_infos(workers)
        return build_cluster_state(tasks, workers, worker_manager_infos)

    async def _clean_managers(self):
        """Clean up dead worker managers that have not sent heartbeats."""
        now = time.monotonic()
        timeout_seconds = DEFAULT_WORKER_MANAGER_TIMEOUT_SECONDS
        dead_managers = [
            manager_id for manager_id, holder in self._managers.items() if now - holder.last_seen_at > timeout_seconds
        ]
        for dead_manager in dead_managers:
            await self._disconnect_manager(dead_manager)

    async def _disconnect_manager(self, manager_id: WorkerManagerID):
        if manager_id not in self._managers:
            return

        logger.info(f"{manager_id!r} disconnected")
        self._managers.pop(manager_id, None)


def _sum_desired_for_manager(commands: List[WorkerManagerCommand], manager_capabilities: Dict[str, int]) -> int:
    """Sum taskConcurrency across requests whose capability set is a subset of the manager's capabilities.

    Mirrors the rule the worker manager itself applies to translate the declarative command into a
    concrete worker count. An empty capability set on a request acts as a wildcard.
    """
    manager_items = manager_capabilities.items()
    total = 0
    for command in commands:
        for request in getattr(command, "setDesiredTaskConcurrencyRequests", ()):
            req_caps = capabilities_to_dict(request.capabilities)
            if req_caps.items() <= manager_items:
                total += request.taskConcurrency
    return total
