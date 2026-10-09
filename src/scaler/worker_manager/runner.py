import asyncio
import logging
import os
import pathlib
from typing import Dict, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_WORKER_MANAGER_PARENT_TIMEOUT_SECONDS,
    DEFAULT_WORKER_MANAGER_RESTART_BACKOFF_SECONDS,
    WORKER_EXIT_NOTIFICATION_TIMEOUT_SECONDS,
)
from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.mixins import AsyncBinder, AsyncConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    WorkerDisconnectNotification,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
    WorkerManagerCommand,
    WorkerManagerDisconnectNotification,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
    WorkerManagerShutdown,
    WorkerShutdown,
)
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.event_loop import create_async_loop_routine, run_task_forever
from scaler.utility.signal_handler import install_async_shutdown_handler
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager
from scaler.worker_manager.desired_concurrency import extract_desired_count
from scaler.worker_manager.mixins import UnitProvisioner
from scaler.worker_manager.unit_controller import UnitController

logger = logging.getLogger(__name__)

# The unit routine reads only state that messages wrote, and runs provisioner calls in the background.
UNIT_CONTROLLER_INTERVAL_SECONDS = 1


class WorkerManagerRunner:
    """Connects a unit controller to its parent (the scheduler, or a parent worker manager) and to its units.

    Every link is a heartbeat and an echo. The runner answers each unit heartbeat from the state of the controller:
    a unit that must drain is told so again on every heartbeat, so a lost message costs one heartbeat.
    """

    def __init__(
        self,
        name: str,
        worker_manager_config: WorkerManagerConfig,
        heartbeat_interval_seconds: int,
        capabilities: Dict[str, int],
        provisioner: UnitProvisioner,
        children_address: AddressConfig,
        io_threads: int = 1,
        security_config: Optional[SecurityConfig] = None,
        ready_file: Optional[str] = None,
    ) -> None:
        self._name = name
        self._parent_address = worker_manager_config.parent_address or worker_manager_config.scheduler_address
        self._unit_id = worker_manager_config.unit_id
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._capabilities = capabilities
        self._worker_manager_id = worker_manager_config.worker_manager_id.encode()
        self._provisioner = provisioner
        self._children_address = children_address
        self._io_threads = io_threads
        self._security_config = security_config
        self._ready_file = pathlib.Path(ready_file) if ready_file is not None else None

        self._unit_controller = UnitController(
            provisioner,
            scale_down_cooldown_seconds=worker_manager_config.scale_down_cooldown_seconds,
            unit_timeout_seconds=worker_manager_config.unit_timeout_seconds,
            drain_timeout_seconds=worker_manager_config.drain_timeout_seconds,
            restart_backoff_seconds=DEFAULT_WORKER_MANAGER_RESTART_BACKOFF_SECONDS,
        )
        self._parent_timeout_manager = VanillaTimeoutManager(
            death_timeout_seconds=DEFAULT_WORKER_MANAGER_PARENT_TIMEOUT_SECONDS, on_timeout=self._on_parent_timeout
        )

        self._backend: Optional[NetworkBackend] = None
        self._connector_parent: Optional[AsyncConnector] = None
        self._binder_children: Optional[AsyncBinder] = None
        self._ident: bytes = b""
        self._task: Optional[asyncio.Task] = None

    async def _initialize_network(self) -> None:
        # A child manager is known to its parent by the unit id the parent chose.
        self._ident = self._unit_id.encode() if self._unit_id is not None else generate_identity_from_name(self._name)
        self._backend = get_network_backend_from_env(io_threads=self._io_threads)
        self._connector_parent = self._backend.create_async_connector(
            identity=self._ident, callback=self._on_receive_parent
        )
        self._binder_children = self._backend.create_async_binder(identity=self._ident, callback=self._on_receive_child)

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self.cleanup)

    async def run_in_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Run using an externally-managed loop. The caller is responsible for catching asyncio.CancelledError."""
        self._loop = loop
        await self._run()

    def cleanup(self) -> None:
        if self._connector_parent is not None:
            self._connector_parent.destroy()
        if self._binder_children is not None:
            self._binder_children.destroy()

    def _destroy(self) -> None:
        logger.info(f"Worker manager {self._ident!r} received signal, shutting down")
        self._task.cancel()

    def _register_signal(self) -> None:
        install_async_shutdown_handler(self._loop, self._destroy)

    async def _run(self) -> None:
        self._task = self._loop.create_task(self._get_loops())
        await self._task

    async def _send_heartbeat(self) -> None:
        status = self._unit_controller.get_status()
        await self._connector_parent.send(
            WorkerManagerHeartbeat(
                maxTaskConcurrency=self._provisioner.max_units() * self._provisioner.task_concurrency_per_unit(),
                capabilities=dict_to_capabilities(self._capabilities),
                workerManagerID=self._worker_manager_id,
                activeTaskConcurrency=status.active_task_concurrency,
                occupancy=status.occupancy,
                activeUnits=status.active_units,
                pendingUnits=status.pending_units,
                drainingUnits=status.draining_units,
            ),
            detached=True,
        )

    async def _get_loops(self) -> None:
        self._remove_ready_file()  # a file left by a killed manager must not mark this one ready
        await self._initialize_network()
        await self._connector_parent.connect(
            self._parent_address, ConnectorRemoteType.Binder, security_config=self._security_config
        )
        await self._binder_children.bind(self._children_address, security_config=self._security_config)
        self._register_signal()

        # swallow_routine_errors=True: a manager serves many units, and a defect in one routine must not take the
        # whole fleet down with it.
        loops = [
            self._receive_from_parent(),
            create_async_loop_routine(self._binder_children.routine, 0, swallow_routine_errors=True),
            create_async_loop_routine(self._send_heartbeat, self._heartbeat_interval_seconds),
            create_async_loop_routine(self._parent_timeout_manager.routine, 1),
            create_async_loop_routine(self._routine, UNIT_CONTROLLER_INTERVAL_SECONDS, swallow_routine_errors=True),
        ]

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except ymq.YMQException as e:
            logger.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")
        except Exception:
            logger.exception(f"{self._ident!r}: failed with unhandled exception")

        # Nothing is left after a drain. After a signal, whatever still runs is destroyed without one.
        await self._unit_controller.terminate()
        self._remove_ready_file()

    async def _receive_from_parent(self) -> None:
        """Receive from the parent until it closes the link, then drain the fleet."""
        try:
            while True:
                await self._connector_parent.routine()
        except ymq.YMQException as e:
            if e.code != ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                raise
            logger.warning(f"{self._ident!r}: the parent closed the link, draining the fleet")
            self._unit_controller.begin_shutdown()

    def _on_parent_timeout(self) -> None:
        logger.warning(f"{self._ident!r}: no heartbeat echo from the parent, draining the fleet")
        self._unit_controller.begin_shutdown()

    async def _routine(self) -> None:
        await self._unit_controller.routine()
        if not self._unit_controller.is_shut_down():
            return

        logger.info(f"{self._ident!r}: the fleet is gone, quitting")
        # Before the notification: the parent may tear the resource down as soon as it arrives.
        self._remove_ready_file()
        await self._notify_parent_of_exit()
        self._task.cancel()

    async def _notify_parent_of_exit(self) -> None:
        """Send the notification, but never let a parent that is gone block the exit."""
        try:
            await asyncio.wait_for(
                self._connector_parent.send(WorkerManagerDisconnectNotification(), detached=False),
                WORKER_EXIT_NOTIFICATION_TIMEOUT_SECONDS,
            )
        except (ymq.YMQException, asyncio.TimeoutError) as e:
            logger.warning(f"{self._ident!r}: could not notify the parent of exit, quitting anyway: {e!r}")

    async def _on_receive_parent(self, message: BaseMessage) -> None:
        try:
            if isinstance(message, WorkerManagerCommand):
                await self._handle_command(message)
            elif isinstance(message, WorkerManagerHeartbeatEcho):
                self._parent_timeout_manager.update_last_seen_time()
            elif isinstance(message, WorkerManagerShutdown):
                self._unit_controller.begin_shutdown()
            else:
                logger.warning(f"Unknown action: received unrecognized message type {type(message).__name__!r}")
        except Exception:
            logger.exception(f"Unhandled exception while processing message {type(message).__name__}")

    async def _handle_command(self, command: WorkerManagerCommand) -> None:
        requests = getattr(command, "setDesiredTaskConcurrencyRequests", None)
        if requests is None:
            logger.warning("Unknown action: received WorkerManagerCommand with no recognized payload")
            return
        self._unit_controller.set_desired_task_concurrency(extract_desired_count(list(requests), self._capabilities))
        self._write_ready_file()

    def _write_ready_file(self) -> None:
        """Mark this manager in service once its parent commands it. A manager that is shutting down never is."""
        if self._ready_file is None or self._ready_file.exists() or self._unit_controller.is_shutting_down():
            return

        # Write then rename, so a probe never reads a partial pid.
        partial_file = self._ready_file.with_name(self._ready_file.name + ".partial")
        partial_file.write_text(f"{os.getpid()}\n")
        os.replace(partial_file, self._ready_file)
        logger.info(f"{self._ident!r}: wrote ready file {str(self._ready_file)!r}")

    def _remove_ready_file(self) -> None:
        if self._ready_file is None or not self._ready_file.exists():
            return

        self._ready_file.unlink(missing_ok=True)
        logger.info(f"{self._ident!r}: removed ready file {str(self._ready_file)!r}")

    async def _on_receive_child(self, source: bytes, message: BaseMessage) -> None:
        unit_id = source.decode()  # each unit dials with the unit id it was created with as its identity

        if isinstance(message, WorkerHeartbeat):
            busy_processors = sum(processor.hasTask for processor in message.processors)
            self._unit_controller.on_unit_heartbeat(
                unit_id, self._provisioner.task_concurrency_per_unit(), message.queuedTasks + busy_processors
            )
            await self._binder_children.send(source, WorkerHeartbeatEcho(), detached=True)
            if not self._unit_controller.is_unit_serving(unit_id):
                await self._binder_children.send(source, WorkerShutdown(), detached=True)
            return

        if isinstance(message, WorkerManagerHeartbeat):
            self._unit_controller.on_unit_heartbeat(unit_id, message.activeTaskConcurrency, message.occupancy)
            await self._binder_children.send(source, WorkerManagerHeartbeatEcho(), detached=True)
            await self._binder_children.send(source, self._instruction_for(unit_id), detached=True)
            return

        if isinstance(message, (WorkerDisconnectNotification, WorkerManagerDisconnectNotification)):
            self._unit_controller.on_unit_disconnect(unit_id)
            return

        logger.error(f"unknown message from unit {unit_id!r}: {message}")

    def _instruction_for(self, unit_id: str) -> BaseMessage:
        if not self._unit_controller.is_unit_serving(unit_id):
            return WorkerManagerShutdown()

        desired = self._unit_controller.get_unit_desired_task_concurrency(unit_id)
        return WorkerManagerCommand(
            setDesiredTaskConcurrencyRequests=[
                WorkerManagerCommand.DesiredTaskConcurrencyRequest(taskConcurrency=desired, capabilities=[])
            ]
        )
