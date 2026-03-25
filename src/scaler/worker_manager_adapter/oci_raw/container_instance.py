"""
OCI Raw Worker Adapter — Container Instance backend.

Manages Scaler worker processes as OCI Container Instances. Each worker group
maps to a single Container Instance. Supports both config-file and
instance-principal authentication for running on local machines or OCI VMs.

All blocking OCI SDK calls are offloaded to a thread executor so the asyncio
event loop is never blocked.
"""

import asyncio
import functools
import logging
import signal
import uuid
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

import zmq

from scaler.config.section.oci_raw_worker_adapter import OCIRawWorkerAdapterConfig
from scaler.io import uv_ymq
from scaler.io.utility import create_async_connector, create_async_simple_context
from scaler.io.ymq import ymq
from scaler.protocol.python.message import (
    Message,
    WorkerAdapterCommand,
    WorkerAdapterCommandResponse,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
    WorkerAdapterHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.common import WorkerGroupID, format_capabilities

Status = WorkerAdapterCommandResponse.Status


@dataclass
class WorkerGroupInfo:
    worker_ids: Set[WorkerID]
    instance_id: str


class OCIContainerInstanceWorkerAdapter:
    """
    Worker adapter that launches OCI Container Instances to run Scaler worker groups.

    Each worker group corresponds to one OCI Container Instance running
    ``scaler_cluster``, spinning up ``instance_ocpus`` workers inside the container.

    OCI Service Mapping (AWS → OCI):
        - Amazon ECS Fargate     → OCI Container Instances
        - ECS Task Definition    → OCI Container Image (from OCIR)
        - ECS Cluster            → OCI Compartment + Subnet
        - AWS IAM Role           → OCI Dynamic Group + IAM Policies
        - AWS Subnets            → OCI Subnet OCID
    """

    def __init__(self, config: OCIRawWorkerAdapterConfig):
        self._address = config.worker_adapter_config.scheduler_address
        self._object_storage_address = config.worker_adapter_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_io_threads
        self._per_worker_task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_instances = config.worker_adapter_config.max_workers
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.event_loop
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file

        self._oci_auth_type = config.oci_auth_type
        self._oci_config_profile = config.oci_config_profile
        self._oci_region = config.oci_region
        self._compartment_id = config.compartment_id
        self._availability_domain = config.availability_domain
        self._subnet_id = config.subnet_id
        self._container_image = config.container_image
        self._oci_python_requirements = config.oci_python_requirements
        self._oci_python_version = config.oci_python_version
        self._instance_shape = config.instance_shape
        self._instance_ocpus = config.instance_ocpus
        self._instance_memory_gb = config.instance_memory_gb

        import oci

        if self._oci_auth_type == "instance_principal":
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            self._container_instances_client = oci.container_instances.ContainerInstanceClient(config={}, signer=signer)
        else:
            oci_config = oci.config.from_file(profile_name=self._oci_config_profile)
            self._container_instances_client = oci.container_instances.ContainerInstanceClient(oci_config)

        self._worker_groups: Dict[WorkerGroupID, WorkerGroupInfo] = {}

        self._context = create_async_simple_context()
        self._name = "worker_adapter_oci_container_instance"
        self._ident = f"{self._name}|{uuid.uuid4().bytes.hex()}".encode()

        self._connector_external = create_async_connector(
            self._context,
            name=self._name,
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
            identity=self._ident,
        )

    async def __on_receive_external(self, message: Message):
        if isinstance(message, WorkerAdapterCommand):
            await self._handle_command(message)
        elif isinstance(message, WorkerAdapterHeartbeatEcho):
            pass
        else:
            logging.warning(f"Received unknown message type: {type(message)}")

    async def _handle_command(self, command: WorkerAdapterCommand):
        cmd_type = command.command
        worker_group_id = command.worker_group_id
        response_status = Status.Success
        worker_ids: List[bytes] = []
        capabilities: Dict[str, int] = {}

        cmd_res = WorkerAdapterCommandType.StartWorkerGroup
        if cmd_type == WorkerAdapterCommandType.StartWorkerGroup:
            cmd_res = WorkerAdapterCommandType.StartWorkerGroup
            worker_group_id, response_status = await self.start_worker_group()
            if response_status == Status.Success:
                worker_ids = [bytes(wid) for wid in self._worker_groups[worker_group_id].worker_ids]
                capabilities = self._capabilities
        elif cmd_type == WorkerAdapterCommandType.ShutdownWorkerGroup:
            cmd_res = WorkerAdapterCommandType.ShutdownWorkerGroup
            response_status = await self.shutdown_worker_group(worker_group_id)
        else:
            raise ValueError("Unknown Command")

        await self._connector_external.send(
            WorkerAdapterCommandResponse.new_msg(
                worker_group_id=worker_group_id,
                command=cmd_res,
                status=response_status,
                worker_ids=worker_ids,
                capabilities=capabilities,
            )
        )

    async def __send_heartbeat(self):
        await self._connector_external.send(
            WorkerAdapterHeartbeat.new_msg(
                max_worker_groups=self._max_instances,
                workers_per_group=int(self._instance_ocpus),
                capabilities=self._capabilities,
            )
        )

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    def _cleanup(self):
        if self._connector_external is not None:
            self._connector_external.destroy()

    def __destroy(self):
        print(f"Worker adapter {self._ident!r} received signal, shutting down")
        self._task.cancel()

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
        self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)

    async def _run(self) -> None:
        register_event_loop(self._event_loop)
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        self._task = self._loop.create_task(self.__get_loops())
        self.__register_signal()
        await self._task

    async def __get_loops(self):
        loops = [
            create_async_loop_routine(self._connector_external.routine, 0),
            create_async_loop_routine(self.__send_heartbeat, self._heartbeat_interval_seconds),
        ]

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except (ymq.YMQException, uv_ymq.UVYMQException) as e:
            if e.code in {
                ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd,
                uv_ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd,
            }:
                pass
            else:
                logging.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")

    async def start_worker_group(self) -> Tuple[WorkerGroupID, Status]:
        if len(self._worker_groups) >= self._max_instances != -1:
            return b"", Status.WorkerGroupTooMuch

        num_workers = int(self._instance_ocpus)
        worker_names = [f"OCI_RAW|{uuid.uuid4().hex}" for _ in range(num_workers)]
        command = (
            f"scaler_cluster {self._address.to_address()} "
            f"--num-of-workers {num_workers} "
            f"--worker-names \"{','.join(worker_names)}\" "
            f"--per-worker-task-queue-size {self._per_worker_task_queue_size} "
            f"--heartbeat-interval-seconds {self._heartbeat_interval_seconds} "
            f"--task-timeout-seconds {self._task_timeout_seconds} "
            f"--garbage-collect-interval-seconds {self._garbage_collect_interval_seconds} "
            f"--death-timeout-seconds {self._death_timeout_seconds} "
            f"--trim-memory-threshold-bytes {self._trim_memory_threshold_bytes} "
            f"--event-loop {self._event_loop} "
            f"--worker-io-threads {self._io_threads}"
        )

        if self._hard_processor_suspend:
            command += " --hard-processor-suspend"

        if self._object_storage_address:
            command += f" --object-storage-address {self._object_storage_address.to_string()}"

        if format_capabilities(self._capabilities).strip():
            command += f" --per-worker-capabilities {format_capabilities(self._capabilities)}"

        import oci

        display_name = f"scaler-worker-{uuid.uuid4().hex[:8]}"
        create_details = oci.container_instances.models.CreateContainerInstanceDetails(
            compartment_id=self._compartment_id,
            availability_domain=self._availability_domain,
            shape=self._instance_shape,
            shape_config=oci.container_instances.models.CreateContainerInstanceShapeConfigDetails(
                ocpus=self._instance_ocpus, memory_in_gbs=self._instance_memory_gb
            ),
            containers=[
                oci.container_instances.models.CreateContainerDetails(
                    image_url=self._container_image,
                    display_name="scaler-container",
                  environment_variables={
                    "COMMAND": command,
                    "PYTHON_REQUIREMENTS": self._oci_python_requirements,
                    "PYTHON_VERSION": self._oci_python_version,
                    "SCALER_PACKAGE": "opengris-scaler==1.15.0",
},
                )
            ],
            vnics=[oci.container_instances.models.CreateContainerVnicDetails(subnet_id=self._subnet_id)],
            display_name=display_name,
        )

        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                functools.partial(
                    self._container_instances_client.create_container_instance,
                    create_container_instance_details=create_details,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            logging.error(f"OCI create_container_instance failed: {exc}")
            return b"", Status.UnknownAction

        instance_id = response.data.id
        worker_group_id = f"oci-raw-{uuid.uuid4().hex}".encode()
        self._worker_groups[worker_group_id] = WorkerGroupInfo(
            worker_ids={WorkerID.generate_worker_id(worker_name) for worker_name in worker_names},
            instance_id=instance_id,
        )
        return worker_group_id, Status.Success

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID) -> Status:
        if not worker_group_id:
            return Status.WorkerGroupIDNotSpecified

        if worker_group_id not in self._worker_groups:
            logging.warning(f"Worker group with ID {bytes(worker_group_id).decode()} does not exist.")
            return Status.WorkerGroupIDNotFound

        import oci

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._container_instances_client.delete_container_instance,
                    container_instance_id=self._worker_groups[worker_group_id].instance_id,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            if exc.status == 404:
                logging.warning(
                    f"OCI Container Instance not found (already deleted?): "
                    f"{self._worker_groups[worker_group_id].instance_id}"
                )
            else:
                logging.error(f"OCI delete_container_instance failed: {exc}")
                return Status.UnknownAction

        self._worker_groups.pop(worker_group_id)
        return Status.Success
