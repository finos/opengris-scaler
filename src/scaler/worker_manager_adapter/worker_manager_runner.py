import asyncio
import logging
import time
from typing import Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import BaseMessage, WorkerManagerCommand, WorkerManagerHeartbeat, WorkerManagerHeartbeatEcho
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.event_loop import create_async_loop_routine, run_task_forever
from scaler.utility.signal_handler import install_async_shutdown_handler
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner

logger = logging.getLogger(__name__)


class WorkerManagerRunner:
    def __init__(
        self,
        address: AddressConfig,
        name: str,
        heartbeat_interval_seconds: int,
        capabilities: Dict[str, int],
        max_provisioner_units: int,
        worker_manager_id: bytes,
        worker_provisioner: DeclarativeWorkerProvisioner,
        io_threads: int = 1,
        workers_per_provisioner_unit: int = 1,
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
        logger.info(f"Worker manager {self._ident!r} received signal, shutting down")
        self._task.cancel()

    def _register_signal(self) -> None:
        install_async_shutdown_handler(self._loop, self._destroy)

    async def _run(self) -> None:
        self._task = self._loop.create_task(self._get_loops())
        await self._task

    async def _send_heartbeat(self) -> None:
        max_concurrency = self._max_provisioner_units * self._workers_per_provisioner_unit
        print(
            f"[WM-RUNNER][_send_heartbeat] ts={time.time():.3f} "
            f"ident={self._ident!r} "
            f"worker_manager_id={self._worker_manager_id!r} "
            f"max_provisioner_units={self._max_provisioner_units} "
            f"workers_per_unit={self._workers_per_provisioner_unit} "
            f"=> maxTaskConcurrency={max_concurrency} "
            f"capabilities={self._capabilities}",
            flush=True,
        )
        await self._connector_external.send(
            WorkerManagerHeartbeat(
                maxTaskConcurrency=max_concurrency,
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
                logger.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")
        except Exception:
            logger.exception(f"{self._ident!r}: failed with unhandled exception")

        await self._worker_provisioner.terminate()

    async def _on_receive_external(self, message: BaseMessage) -> None:
        print(
            f"[WM-RUNNER][_on_receive_external] ts={time.time():.3f} "
            f"ident={self._ident!r} "
            f"message_type={type(message).__name__!r}",
            flush=True,
        )
        try:
            if isinstance(message, WorkerManagerCommand):
                await self._handle_command(message)
            elif isinstance(message, WorkerManagerHeartbeatEcho):
                print(
                    f"[WM-RUNNER][_on_receive_external]   received HeartbeatEcho (scheduler alive)",
                    flush=True,
                )
            else:
                print(
                    f"[WM-RUNNER][_on_receive_external]   UNKNOWN message type={type(message).__name__!r}",
                    flush=True,
                )
                logger.warning(f"Unknown action: received unrecognized message type {type(message).__name__!r}")
        except Exception:
            logger.exception(f"Unhandled exception while processing message {type(message).__name__}")

    async def _handle_command(self, command: WorkerManagerCommand) -> None:
        requests = getattr(command, "setDesiredTaskConcurrencyRequests", None)
        if requests is None:
            print(
                f"[WM-RUNNER][_handle_command] WARNING: WorkerManagerCommand has no recognized payload. "
                f"command fields={dir(command)}",
                flush=True,
            )
            logger.warning("Unknown action: received WorkerManagerCommand with no recognized payload")
            return

        requests_list = list(requests)
        print(
            f"[WM-RUNNER][_handle_command] ts={time.time():.3f} "
            f"ident={self._ident!r} "
            f"received setDesiredTaskConcurrency with {len(requests_list)} request(s):",
            flush=True,
        )
        for i, req in enumerate(requests_list):
            req_caps = {entry.key: entry.value for entry in req.capabilities} if hasattr(req, "capabilities") else {}
            print(
                f"[WM-RUNNER][_handle_command]   request[{i}]: caps={req_caps} taskConcurrency={req.taskConcurrency}",
                flush=True,
            )
        print(
            f"[WM-RUNNER][_handle_command]   forwarding to provisioner.set_desired_task_concurrency ...",
            flush=True,
        )
        await self._worker_provisioner.set_desired_task_concurrency(requests_list)
