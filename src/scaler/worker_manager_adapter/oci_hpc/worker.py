"""
OCI Container Instance Worker.

Connects to the Scaler scheduler via ZMQ streaming and forwards tasks
to OCI Container Instances for execution via the TaskManager. This is the
main process that bridges the scheduler stream to OCI Container Instances.

Follows the same pattern as AWSBatchWorker for consistency.

OCI Service Mapping:
    - AWS Batch job queue       → OCI Subnet (where container instances are launched)
    - AWS Batch job definition  → OCI Container Image (from OCIR)
    - Amazon S3 bucket          → OCI Object Storage bucket
    - AWS Region                → OCI Region
"""

import asyncio
import logging
import multiprocessing
import signal
from collections import deque
from typing import Dict, Optional

import zmq.asyncio

from scaler.config.types.network_backend import NetworkBackend
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.io import uv_ymq
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.io.utility import (
    create_async_connector,
    create_async_object_storage_connector,
    get_scaler_network_backend_from_env,
)
from scaler.io.ymq import ymq
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    ObjectInstruction,
    Task,
    TaskCancel,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.mixins import Message
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager
from scaler.worker_manager_adapter.oci_hpc.heartbeat_manager import OCIContainerInstanceHeartbeatManager
from scaler.worker_manager_adapter.oci_hpc.task_manager import OCIHPCTaskManager

_SpawnProcess = multiprocessing.get_context("spawn").Process


class OCIContainerInstanceWorker(_SpawnProcess):  # type: ignore[valid-type, misc]
    """
    OCI Container Instance Worker that receives tasks from the scheduler stream
    and submits them to OCI Container Instances via the TaskManager.

    Follows the same pattern as AWSBatchWorker for consistency.

    Args:
        name: Worker name (used to derive the worker identity).
        address: ZMQ address to connect to the scheduler.
        object_storage_address: Optional address for the Scaler object storage server.
        compartment_id: OCI Compartment OCID where resources are created.
        availability_domain: OCI Availability Domain (e.g., ``"AD-1"``).
        subnet_id: OCI Subnet OCID for container instance network interfaces.
        container_image: OCIR image URI (e.g., ``"<region>.ocir.io/<ns>/<repo>:latest"``).
        oci_region: OCI region identifier (e.g., ``"us-ashburn-1"``).
        object_storage_namespace: OCI Object Storage tenancy namespace.
        object_storage_bucket: OCI Object Storage bucket name for task data.
        object_storage_prefix: Object key prefix for task inputs/results.
        instance_shape: OCI Container Instance shape (default: ``"CI.Standard.E4.Flex"``).
        instance_ocpus: Number of OCPUs per container instance.
        instance_memory_gb: Memory in GB per container instance.
        capabilities: Optional scheduler capability tags for this worker.
        base_concurrency: Maximum number of concurrently running container instances.
        heartbeat_interval_seconds: Interval between scheduler heartbeats.
        death_timeout_seconds: Timeout before assuming scheduler is dead.
        task_queue_size: Reported task queue capacity to the scheduler.
        io_threads: Number of ZMQ IO threads.
        event_loop: Event loop backend (``"builtin"`` or ``"uvloop"``).
        job_timeout_seconds: Maximum runtime for a single container instance.
        oci_config_profile: OCI config file profile name (default: ``"DEFAULT"``).
        oci_auth_type: OCI auth mode (``"config_file"`` or ``"instance_principal"``).
    """

    def __init__(
        self,
        name: str,
        address: ZMQConfig,
        object_storage_address: Optional[ObjectStorageAddressConfig],
        compartment_id: str,
        availability_domain: str,
        subnet_id: str,
        container_image: str,
        oci_region: str,
        object_storage_namespace: str,
        object_storage_bucket: str,
        object_storage_prefix: str = "scaler-tasks",
        instance_shape: str = "CI.Standard.E4.Flex",
        instance_ocpus: float = 1.0,
        instance_memory_gb: float = 6.0,
        capabilities: Optional[Dict[str, int]] = None,
        base_concurrency: int = 100,
        heartbeat_interval_seconds: int = 1,
        death_timeout_seconds: int = 30,
        task_queue_size: int = 1000,
        io_threads: int = 2,
        event_loop: str = "builtin",
        job_timeout_seconds: int = 3600,
        oci_config_profile: str = "DEFAULT",
        oci_auth_type: str = "config_file",
    ) -> None:
        multiprocessing.Process.__init__(self, name="OCIContainerInstanceWorker")

        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._object_storage_address = object_storage_address
        self._capabilities = capabilities or {}
        self._io_threads = io_threads

        self._ident = WorkerID.generate_worker_id(name)

        self._compartment_id = compartment_id
        self._availability_domain = availability_domain
        self._subnet_id = subnet_id
        self._container_image = container_image
        self._oci_region = oci_region
        self._object_storage_namespace = object_storage_namespace
        self._object_storage_bucket = object_storage_bucket
        self._object_storage_prefix = object_storage_prefix
        self._instance_shape = instance_shape
        self._instance_ocpus = instance_ocpus
        self._instance_memory_gb = instance_memory_gb
        self._base_concurrency = base_concurrency
        self._job_timeout_seconds = job_timeout_seconds
        self._oci_config_profile = oci_config_profile
        self._oci_auth_type = oci_auth_type

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._task_queue_size = task_queue_size

        self._context: Optional[zmq.asyncio.Context] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._task_manager: Optional[OCIHPCTaskManager] = None
        self._heartbeat_manager: Optional[OCIContainerInstanceHeartbeatManager] = None
        self._timeout_manager: Optional[VanillaTimeoutManager] = None

        # Backoff queue for messages received before the first heartbeat echo
        self._heartbeat_received: bool = False
        self._backoff_message_queue: deque = deque()

    @property
    def identity(self) -> WorkerID:
        return self._ident

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    async def _run(self) -> None:
        self.__initialize()

        self._task = self._loop.create_task(self.__get_loops())
        self.__register_signal()
        await self._task

    def _cleanup(self) -> None:
        if self._connector_storage is not None:
            self._connector_storage.destroy()

    def __initialize(self) -> None:
        setup_logger()
        register_event_loop(self._event_loop)

        self._context = zmq.asyncio.Context(io_threads=self._io_threads)
        self._connector_external = create_async_connector(
            self._context,
            name=self._name,
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
            identity=self._ident,
        )

        self._connector_storage = create_async_object_storage_connector()

        self._heartbeat_manager = OCIContainerInstanceHeartbeatManager(
            object_storage_address=self._object_storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._task_queue_size,
        )

        self._task_manager = OCIHPCTaskManager(
            base_concurrency=self._base_concurrency,
            compartment_id=self._compartment_id,
            availability_domain=self._availability_domain,
            subnet_id=self._subnet_id,
            container_image=self._container_image,
            oci_region=self._oci_region,
            object_storage_namespace=self._object_storage_namespace,
            object_storage_bucket=self._object_storage_bucket,
            object_storage_prefix=self._object_storage_prefix,
            instance_shape=self._instance_shape,
            instance_ocpus=self._instance_ocpus,
            instance_memory_gb=self._instance_memory_gb,
            job_timeout_seconds=self._job_timeout_seconds,
            oci_config_profile=self._oci_config_profile,
            oci_auth_type=self._oci_auth_type,
        )

        self._timeout_manager = VanillaTimeoutManager(death_timeout_seconds=self._death_timeout_seconds)

        self._heartbeat_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            worker_task_manager=self._task_manager,
            timeout_manager=self._timeout_manager,
        )
        self._task_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            heartbeat_manager=self._heartbeat_manager,
        )

    async def __on_receive_external(self, message: Message) -> None:
        """Handle incoming messages from the scheduler."""
        if not self._heartbeat_received and not isinstance(message, WorkerHeartbeatEcho):
            self._backoff_message_queue.append(message)
            return

        if isinstance(message, WorkerHeartbeatEcho):
            await self._heartbeat_manager.on_heartbeat_echo(message)
            self._heartbeat_received = True

            while self._backoff_message_queue:
                backoff_message = self._backoff_message_queue.popleft()
                await self.__on_receive_external(backoff_message)
            return

        if isinstance(message, Task):
            await self._task_manager.on_task_new(message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_cancel_task(message)
            return

        if isinstance(message, ObjectInstruction):
            await self._task_manager.on_object_instruction(message)
            return

        if isinstance(message, ClientDisconnect):
            if message.disconnect_type == ClientDisconnect.DisconnectType.Shutdown:
                raise ClientShutdownException("received client shutdown, quitting")
            logging.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        if isinstance(message, DisconnectResponse):
            logging.error("Worker initiated DisconnectRequest got replied")
            self._task.cancel()
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self) -> None:
        """Run all async loops."""
        if self._object_storage_address is not None:
            await self._connector_storage.connect(self._object_storage_address.host, self._object_storage_address.port)

        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._task_manager.process_task, 0),
                create_async_loop_routine(self._task_manager.resolve_tasks, 0),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as exc:
            logging.info(f"{self.identity!r}: {str(exc)}")
        except Exception as exc:
            logging.exception(f"{self.identity!r}: failed with unhandled exception:\n{exc}")

        if get_scaler_network_backend_from_env() == NetworkBackend.tcp_zmq:
            await self.__graceful_shutdown()

        self._connector_external.destroy()
        logging.info(f"{self.identity!r}: quit")

    def __register_signal(self) -> None:
        backend = get_scaler_network_backend_from_env()
        if backend == NetworkBackend.tcp_zmq:
            self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
            self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)
        elif backend in {NetworkBackend.ymq, NetworkBackend.uv_ymq}:
            self._loop.add_signal_handler(signal.SIGINT, lambda: asyncio.ensure_future(self.__graceful_shutdown()))
            self._loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(self.__graceful_shutdown()))

    async def __graceful_shutdown(self) -> None:
        try:
            await self._connector_external.send(DisconnectRequest.new_msg(self.identity))
        except (ymq.YMQException, uv_ymq.UVYMQException):
            pass

    def __destroy(self) -> None:
        self._task.cancel()
