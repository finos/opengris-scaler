import asyncio
import logging
from multiprocessing.synchronize import Event as MultiprocessingEvent
from typing import Dict, List, Optional, Union

from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.event_loop import create_async_loop_routine, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.utility.process_signal import register_async_shutdown
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner, ImperativeWorkerProvisioner

Status = WorkerManagerCommandResponse.Status


class WorkerManagerRunner:
    def __init__(
        self,
        address: AddressConfig,
        name: str,
        heartbeat_interval_seconds: int,
        capabilities: Dict[str, int],
        max_provisioner_units: int,
        worker_manager_id: bytes,
        worker_provisioner: Union[ImperativeWorkerProvisioner, DeclarativeWorkerProvisioner],
        io_threads: int = 1,
        workers_per_provisioner_unit: int = 1,
        shutdown_event: Optional[MultiprocessingEvent] = None,
    ) -> None:
        self._address = address
        self._name = name
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._capabilities = capabilities
        self._max_provisioner_units = max_provisioner_units
        self._worker_manager_id = worker_manager_id
        self._worker_provisioner = worker_provisioner
        self._io_threads = io_threads
        self._workers_per_provisioner_unit = workers_per_provisioner_unit
        self._shutdown_event = shutdown_event

        self._backend: Optional[NetworkBackend] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._ident: bytes = b""
        self._task: Optional[asyncio.Task] = None

    async def _initialize_network(self) -> None:
        self._ident = generate_identity_from_name(self._name)
        self._backend = get_network_backend_from_env(io_threads=self._io_threads)
        self._connector_external = self._backend.create_async_connector(
            identity=self._ident, callback=self._on_receive_external
        )

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

    def _destroy(self) -> None:
        logging.info(f"Worker manager {self._ident!r} received signal, shutting down")
        self._task.cancel()

    def _register_signal(self) -> None:
        register_async_shutdown(self._loop, self._destroy, shutdown_event=self._shutdown_event)

    async def _run(self) -> None:
        self._task = self._loop.create_task(self._get_loops())
        await self._task

    async def _send_heartbeat(self) -> None:
        await self._connector_external.send(
            WorkerManagerHeartbeat(
                maxTaskConcurrency=self._max_provisioner_units * self._workers_per_provisioner_unit,
                capabilities=dict_to_capabilities(self._capabilities),
                workerManagerID=self._worker_manager_id,
            )
        )

    async def _get_loops(self) -> None:
        await self._initialize_network()
        await self._connector_external.connect(self._address, ConnectorRemoteType.Binder)
        self._register_signal()

        loops = [
            create_async_loop_routine(self._connector_external.routine, 0),
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
                logging.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")
        except Exception:
            logging.exception(f"{self._ident!r}: failed with unhandled exception")

    async def _on_receive_external(self, message: BaseMessage) -> None:
        try:
            if isinstance(message, WorkerManagerCommand):
                await self._handle_command(message)
            elif isinstance(message, WorkerManagerHeartbeatEcho):
                pass
            else:
                logging.warning(f"Received unknown message type: {type(message)}")
        except Exception:
            logging.exception(f"Unhandled exception while processing message {type(message).__name__}")

    async def _handle_command(self, command: WorkerManagerCommand) -> None:
        cmd_type = command.command
        response_status: Status = Status.success
        worker_ids: List[WorkerID] = []
        capabilities: Dict[str, int] = {}

        if cmd_type == WorkerManagerCommandType.startWorkers:
            if isinstance(self._worker_provisioner, ImperativeWorkerProvisioner):
                worker_ids, response_status = await self._worker_provisioner.start_worker()
                if response_status == Status.success:
                    capabilities = self._capabilities
            else:
                # declarative provisioners send a no-op success so the scheduler's
                # single-in-flight gate is not blocked waiting for a response that never comes
                logging.debug(f"Ignoring unimplemented WorkerManagerCommand: {cmd_type!r}")
        elif cmd_type == WorkerManagerCommandType.shutdownWorkers:
            if isinstance(self._worker_provisioner, ImperativeWorkerProvisioner):
                worker_ids, response_status = await self._worker_provisioner.shutdown_workers(
                    [WorkerID(wid) for wid in command.workerIDs]
                )
            else:
                logging.debug(f"Ignoring unimplemented WorkerManagerCommand: {cmd_type!r}")
        elif cmd_type == WorkerManagerCommandType.setDesiredTaskConcurrency:
            if isinstance(self._worker_provisioner, DeclarativeWorkerProvisioner):
                await self._worker_provisioner.set_desired_task_concurrency(
                    list(command.setDesiredTaskConcurrencyRequests)
                )
            else:
                logging.debug(f"Ignoring unimplemented WorkerManagerCommand: {cmd_type!r}")
            return
        else:
            logging.debug(f"Ignoring unimplemented WorkerManagerCommand: {cmd_type!r}")
            return

        await self._connector_external.send(
            WorkerManagerCommandResponse(
                command=cmd_type,
                status=response_status,
                workerIDs=worker_ids,
                capabilities=dict_to_capabilities(capabilities),
            )
        )
