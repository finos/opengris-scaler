import asyncio
import logging
from typing import Dict, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
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
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.event_loop import create_async_loop_routine, run_task_forever
from scaler.utility.signal_handler import install_async_shutdown_handler
from scaler.worker_manager.mixins import DeclarativeWorkerProvisioner

logger = logging.getLogger(__name__)


class WorkerManagerRunner:
    """Connects a worker provisioner to its parent (the scheduler, or a parent worker manager) and to its units.

    Every unit dials the children address with its unit id as identity: a worker, or a child worker manager in a
    resource. Each unit heartbeat gets an echo, and a child manager is asked for the task concurrency of one whole unit.
    """

    def __init__(
        self,
        name: str,
        worker_manager_config: WorkerManagerConfig,
        heartbeat_interval_seconds: int,
        capabilities: Dict[str, int],
        max_provisioner_units: int,
        worker_provisioner: DeclarativeWorkerProvisioner,
        children_address: AddressConfig,
        io_threads: int = 1,
        workers_per_provisioner_unit: int = 1,
        security_config: Optional[SecurityConfig] = None,
        parent_address: Optional[AddressConfig] = None,
        unit_id: Optional[str] = None,
    ) -> None:
        self._name = name
        self._parent_address = parent_address or worker_manager_config.scheduler_address
        self._unit_id = unit_id
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._capabilities = capabilities
        self._max_provisioner_units = max_provisioner_units
        self._worker_manager_id = worker_manager_config.worker_manager_id.encode()
        self._worker_provisioner = worker_provisioner
        self._children_address = children_address
        self._io_threads = io_threads
        self._workers_per_provisioner_unit = workers_per_provisioner_unit
        self._security_config = security_config

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
        await self._connector_parent.send(
            WorkerManagerHeartbeat(
                maxTaskConcurrency=self._max_provisioner_units * self._workers_per_provisioner_unit,
                capabilities=dict_to_capabilities(self._capabilities),
                workerManagerID=self._worker_manager_id,
            ),
            detached=True,
        )

    async def _get_loops(self) -> None:
        await self._initialize_network()
        await self._connector_parent.connect(
            self._parent_address, ConnectorRemoteType.Binder, security_config=self._security_config
        )
        await self._binder_children.bind(self._children_address, security_config=self._security_config)
        self._register_signal()

        # swallow_routine_errors=True: a manager serves many units, and a defect in handling one must not take the
        # whole fleet down with it.
        loops = [
            create_async_loop_routine(self._connector_parent.routine, 0),
            create_async_loop_routine(self._binder_children.routine, 0, swallow_routine_errors=True),
            create_async_loop_routine(self._send_heartbeat, self._heartbeat_interval_seconds),
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

        await self._worker_provisioner.terminate()

    async def _on_receive_parent(self, message: BaseMessage) -> None:
        try:
            if isinstance(message, WorkerManagerCommand):
                await self._handle_command(message)
            elif isinstance(message, WorkerManagerHeartbeatEcho):
                pass
            else:
                logger.warning(f"Unknown action: received unrecognized message type {type(message).__name__!r}")
        except Exception:
            logger.exception(f"Unhandled exception while processing message {type(message).__name__}")

    async def _handle_command(self, command: WorkerManagerCommand) -> None:
        requests = getattr(command, "setDesiredTaskConcurrencyRequests", None)
        if requests is None:
            logger.warning("Unknown action: received WorkerManagerCommand with no recognized payload")
            return
        await self._worker_provisioner.set_desired_task_concurrency(list(requests))

    async def _on_receive_child(self, source: bytes, message: BaseMessage) -> None:
        unit_id = source.decode()  # each unit dials with the unit id it was created with as its identity

        if isinstance(message, WorkerHeartbeat):
            await self._binder_children.send(source, WorkerHeartbeatEcho(), detached=True)
            return

        if isinstance(message, WorkerManagerHeartbeat):
            await self._binder_children.send(source, WorkerManagerHeartbeatEcho(), detached=True)
            await self._binder_children.send(source, self._unit_command(), detached=True)
            return

        if isinstance(message, WorkerDisconnectNotification):
            logger.info(f"unit {unit_id!r} is exiting")
            return

        logger.error(f"unknown message from unit {unit_id!r}: {message}")

    def _unit_command(self) -> WorkerManagerCommand:
        return WorkerManagerCommand(
            setDesiredTaskConcurrencyRequests=[
                WorkerManagerCommand.DesiredTaskConcurrencyRequest(
                    taskConcurrency=self._workers_per_provisioner_unit, capabilities=[]
                )
            ]
        )
