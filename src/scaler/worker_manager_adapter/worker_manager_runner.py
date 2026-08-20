import asyncio
import logging
from typing import Dict, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io import ymq
from scaler.io.mixins import AsyncBinder, AsyncConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    WorkerHeartbeat,
    WorkerManagerCommand,
    WorkerManagerDisconnectNotification,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
    WorkerManagerShutdown,
)
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.event_loop import create_async_loop_routine, run_task_forever
from scaler.utility.signal_handler import install_async_shutdown_handler
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.unit import UnitState
from scaler.worker_manager_adapter.unit_controller import UnitController
from scaler.worker_manager_adapter.unit_provisioner import UnitProvisioner

logger = logging.getLogger(__name__)

DEFAULT_DRAIN_TIMEOUT_SECONDS = 60.0
DEFAULT_RESTART_BACKOFF_SECONDS = 1.0


class WorkerManagerRunner:
    def __init__(
        self,
        address: AddressConfig,
        name: str,
        heartbeat_interval_seconds: int,
        capabilities: Dict[str, int],
        max_provisioner_units: int,
        worker_manager_id: bytes,
        worker_provisioner: UnitProvisioner,
        io_threads: int = 1,
        workers_per_provisioner_unit: int = 1,
        security_config: Optional[SecurityConfig] = None,
        children_bind_address: Optional[AddressConfig] = None,
        drain_timeout_seconds: float = DEFAULT_DRAIN_TIMEOUT_SECONDS,
        restart_backoff_seconds: float = DEFAULT_RESTART_BACKOFF_SECONDS,
    ) -> None:
        self._address = address
        self._name = name
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._capabilities = capabilities
        self._max_provisioner_units = max_provisioner_units
        self._worker_manager_id = worker_manager_id
        self._io_threads = io_threads
        self._workers_per_provisioner_unit = workers_per_provisioner_unit
        self._security_config = security_config
        self._configured_children_address = children_bind_address

        self._unit_provisioner = worker_provisioner
        self._unit_controller = UnitController(worker_provisioner, drain_timeout_seconds, restart_backoff_seconds)

        self._backend: Optional[NetworkBackend] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._binder_children: Optional[AsyncBinder] = None
        self._children_address: Optional[AddressConfig] = None
        self._ident: bytes = b""
        self._task: Optional[asyncio.Task] = None

    async def _initialize_network(self) -> None:
        self._ident = generate_identity_from_name(self._name)
        self._backend = get_network_backend_from_env(io_threads=self._io_threads)
        self._connector_external = self._backend.create_async_connector(
            identity=self._ident, callback=self._on_receive_external
        )

        # Children always dial parents, so this manager binds and its units connect in. The
        # address is loopback TCP rather than a Unix socket, because this manager runs on
        # Windows too.
        self._binder_children = self._backend.create_async_binder(identity=self._ident, callback=self._on_receive_child)

        # A remote unit cannot reach a loopback port, so a manager whose units are remote resources
        # must be told an address they can reach. A local unit dials whatever the bind settles on.
        # Port 0 rather than a port chosen here: choosing one means probing for a free port and
        # closing the probe socket, which leaves that port free for anyone else to take in the
        # meantime, including a scheduler in the same process tree that has not bound yet.
        self._children_address = self._configured_children_address or AddressConfig(SocketType.tcp, "127.0.0.1", 0)

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self.cleanup)

    async def run_in_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Run using an externally-managed loop. The caller is responsible for catching asyncio.CancelledError."""
        self._loop = loop
        await self._run()

    def cleanup(self) -> None:
        if self._connector_external is not None:
            self._connector_external.destroy()
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
        # One status call feeds both the heartbeat and the monitor, the way the scheduler's
        # information controller collects its reporters.
        status = self._unit_controller.get_status()

        await self._connector_external.send(
            WorkerManagerHeartbeat(
                maxTaskConcurrency=self._max_provisioner_units * self._workers_per_provisioner_unit,
                capabilities=dict_to_capabilities(self._capabilities),
                workerManagerID=self._worker_manager_id,
                activeTaskConcurrency=status["active_task_concurrency"],
                occupancy=status["occupancy"],
                activeUnits=status[UnitState.active.name],
                pendingUnits=status[UnitState.pending.name],
                drainingUnits=status[UnitState.draining.name] + status[UnitState.stopping.name],
            ),
            detached=True,
        )

    async def _get_loops(self) -> None:
        await self._initialize_network()
        await self._connector_external.connect(
            self._address, ConnectorRemoteType.Binder, security_config=self._security_config
        )
        self._register_signal()

        await self._binder_children.bind(self._children_address, security_config=self._security_config)

        # The binder resolves port 0 to a real port, and that is the address units must dial.
        bound_address = self._binder_children.address
        if bound_address is not None:
            self._children_address = bound_address
        self._unit_provisioner.register(self._binder_children, self._children_address)

        loops = [
            create_async_loop_routine(self._connector_external.routine, 0),
            create_async_loop_routine(self._binder_children.routine, 0),
            create_async_loop_routine(self._send_heartbeat, self._heartbeat_interval_seconds),
            create_async_loop_routine(
                self._unit_controller.routine,
                self._unit_provisioner.poll_interval_seconds(),
                swallow_routine_errors=True,
            ),
        ]

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except ymq.YMQException as e:
            if e.code == ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                pass
            else:
                logger.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")
        except Exception:
            logger.exception(f"{self._ident!r}: failed with unhandled exception")

        # The loops have stopped here, so no drain command could be delivered and no unit could
        # report back. Tear the fleet down directly; a graceful fleet drain belongs to the
        # WorkerManagerShutdown path, which runs while the loops are still up.
        await self._unit_controller.destroy_all()

    async def _on_receive_child(self, source: bytes, message: BaseMessage) -> None:
        """A unit reporting its own state. The manager never asks the scheduler about its units."""
        if isinstance(message, WorkerHeartbeat):
            self._unit_controller.on_unit_report(
                source.decode(),
                active_task_concurrency=1 if not message.taskLock else 0,
                occupancy=message.queuedTasks + (0 if message.taskLock else 1),
            )
            return

        logger.warning(f"unknown message from unit {source!r}: {type(message).__name__}")

    async def _on_receive_external(self, message: BaseMessage) -> None:
        try:
            if isinstance(message, WorkerManagerCommand):
                await self._handle_command(message)
            elif isinstance(message, WorkerManagerShutdown):
                await self._handle_shutdown()
            elif isinstance(message, WorkerManagerHeartbeatEcho):
                pass
            else:
                logger.warning(f"Unknown action: received unrecognized message type {type(message).__name__!r}")
        except Exception:
            logger.exception(f"Unhandled exception while processing message {type(message).__name__}")

    async def _handle_shutdown(self) -> None:
        """Drain the whole fleet, tell the parent it is gone, then stop.

        This runs while the loops are still up, so the drain commands reach the units and their
        reports come back. The notification is what lets a parent destroy the resource at the
        moment the fleet is idle, rather than at a time it has to guess in advance.
        """
        logger.info("worker manager asked to shut down, draining the fleet")

        await self._unit_controller.drain_all()
        await self._connector_external.send(WorkerManagerDisconnectNotification(), detached=False)

        if self._task is not None:
            self._task.cancel()

    async def _handle_command(self, command: WorkerManagerCommand) -> None:
        requests = getattr(command, "setDesiredTaskConcurrencyRequests", None)
        if requests is None:
            logger.warning("Unknown action: received WorkerManagerCommand with no recognized payload")
            return

        # The capability match happens once, here, rather than in every provisioner.
        self._unit_controller.set_desired_task_concurrency(extract_desired_count(list(requests), self._capabilities))
