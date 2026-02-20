import asyncio
import json
import logging
import os
import signal
import urllib.request
import uuid
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

import boto3
import zmq

from scaler.config.section.orb_worker_adapter import ORBWorkerAdapterConfig
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
from scaler.worker_adapter.common import WorkerGroupID, format_capabilities
from scaler.worker_adapter.orb.helper import ORBHelper
from scaler.worker_adapter.orb.types import ORBTemplate

Status = WorkerAdapterCommandResponse.Status
logger = logging.getLogger(__name__)


# Polling configuration for ORB machine requests
ORB_POLLING_INTERVAL_SECONDS = 5
ORB_MAX_POLLING_ATTEMPTS = 60


def get_orb_worker_name(instance_id: str) -> str:
    """
    Returns the deterministic worker name for an ORB instance.
    If instance_id is the bash variable '${INSTANCE_ID}', it returns a bash-compatible string.
    """
    if instance_id == "${INSTANCE_ID}":
        return "Worker|ORB|${INSTANCE_ID}|${INSTANCE_ID//i-/}"
    tag = instance_id.replace("i-", "")
    return f"Worker|ORB|{instance_id}|{tag}"


class ORBWorkerAdapter:
    _config: ORBWorkerAdapterConfig
    _orb: ORBHelper
    _worker_groups: Dict[WorkerGroupID, WorkerID]
    _template_id: str
    _created_security_group_id: Optional[str]
    _created_key_name: Optional[str]
    _ec2: Optional[Any]

    def __init__(self, config: ORBWorkerAdapterConfig):
        self._config = config
        self._address = config.worker_adapter_config.scheduler_address
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._max_workers = config.worker_adapter_config.max_workers

        self._orb = None
        self._created_security_group_id = None
        self._created_key_name = None

        self._event_loop = config.event_loop
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file

        if self._config.subnet_id:
            logger.warning("subnet_id is specified in config but currently has no effect")

        if self._config.security_group_ids:
            logger.warning("security_group_ids are specified in config but currently have no effect")

        # Setup temporary execution environment for ORB via ORBHelper
        source_orb_root = os.path.abspath(config.orb_config_path)
        if not os.path.isdir(source_orb_root):
            raise NotADirectoryError(f"orb_config_path must be a directory: {source_orb_root}")

        self._orb = ORBHelper(config_root_path=source_orb_root)

        self._worker_groups = {}
        self._ec2 = boto3.client("ec2", region_name=self._config.aws_region)
        self._cleaned_up = False

        if not self._config.subnet_id:
            self._config.subnet_id = self._discover_default_subnet()

        self._template_id = os.urandom(8).hex()

        security_group_ids = self._config.security_group_ids
        if not security_group_ids:
            self._create_security_group()
            security_group_ids = [self._created_security_group_id]

        key_name = self._config.key_name
        if not key_name:
            self._create_key_pair()
            key_name = self._created_key_name

        user_data = self._create_user_data()
        user_data_file_path = os.path.join(self._orb.cwd, "config", "user_data.sh")
        with open(user_data_file_path, "w") as f:
            f.write(user_data)

        template = ORBTemplate(
            template_id=self._template_id,
            max_number=self._config.worker_adapter_config.max_workers,
            provider_api="RunInstances",
            provider_name="aws-default",
            image_id=self._config.image_id,
            vm_type=self._config.instance_type,
            subnet_id=self._config.subnet_id,
            security_group_ids=security_group_ids,
            key_name=key_name,
            user_data_script=user_data_file_path,
            metadata={
                "attributes": {
                    "type": ["String", "X86_64"],
                    "ncpus": ["Numeric", "1"],
                    "nram": ["Numeric", "1024"],
                    "ncores": ["Numeric", "1"],
                }
            },
        )

        # Create template in ORB
        # Use the cwd from ORBHelper to place the templates file
        templates_file_path = os.path.join(self._orb.cwd, "config", "templates.json")
        with open(templates_file_path, "w") as f:
            template_dict = asdict(template)
            # Remove empty list that might overwrite the subnet_id field
            if not template_dict.get("subnet_ids"):
                del template_dict["subnet_ids"]

            json.dump({"templates": [template_dict]}, f, indent=4)

        self._context = create_async_simple_context()
        self._name = "worker_adapter_orb"
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
                worker_ids = [bytes(self._worker_groups[worker_group_id])]
                capabilities = self._capabilities
        elif cmd_type == WorkerAdapterCommandType.ShutdownWorkerGroup:
            cmd_res = WorkerAdapterCommandType.ShutdownWorkerGroup
            response_status = await self.shutdown_worker_group(worker_group_id)
        else:
            raise ValueError("Unknown Command")

        await self._connector_external.send(
            WorkerAdapterCommandResponse.new_msg(
                worker_group_id=bytes(worker_group_id),
                command=cmd_res,
                status=response_status,
                worker_ids=worker_ids,
                capabilities=capabilities,
            )
        )

    async def __send_heartbeat(self):
        await self._connector_external.send(
            WorkerAdapterHeartbeat.new_msg(
                max_worker_groups=self._max_workers, workers_per_group=1, capabilities=self._capabilities
            )
        )

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

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
        except ymq.YMQException as e:
            if e.code == ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                pass
            else:
                logging.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")

    def _create_user_data(self) -> str:
        worker_config = self._config.worker_config
        adapter_config = self._config.worker_adapter_config

        # We assume 1 worker per machine for ORB
        # TODO: Add support for multiple workers per machine if needed
        num_workers = 1

        # Build the command
        # We construct the full WorkerID here so it's deterministic and matches what the adapter calculates
        # We fetch instance_id once and use it to construct the ID
        script = f"""#!/bin/bash
INSTANCE_ID=$(ec2-metadata --instance-id --quiet)
WORKER_NAME="{get_orb_worker_name('${INSTANCE_ID}')}"

nohup /usr/local/bin/scaler_cluster {adapter_config.scheduler_address.to_address()} \
    --num-of-workers {num_workers} \
    --worker-names "${{WORKER_NAME}}" \
    --per-worker-task-queue-size {worker_config.per_worker_task_queue_size} \
    --heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} \
    --task-timeout-seconds {worker_config.task_timeout_seconds} \
    --garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} \
    --death-timeout-seconds {worker_config.death_timeout_seconds} \
    --trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} \
    --event-loop {self._config.event_loop} \
    --worker-io-threads {self._config.worker_io_threads} \
    --deterministic-worker-ids"""

        if worker_config.hard_processor_suspend:
            script += " \
    --hard-processor-suspend"

        if adapter_config.object_storage_address:
            script += f" \
    --object-storage-address {adapter_config.object_storage_address.to_string()}"

        capabilities = worker_config.per_worker_capabilities.capabilities
        if capabilities:
            cap_str = format_capabilities(capabilities)
            if cap_str.strip():
                script += f" \
    --per-worker-capabilities {cap_str}"

        script += " > /var/log/opengris-scaler.log 2>&1 &\n"

        return script

    def _discover_default_subnet(self) -> str:
        vpcs = self._ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        if not vpcs["Vpcs"]:
            raise RuntimeError("No default VPC found, and no subnet_id provided.")
        default_vpc_id = vpcs["Vpcs"][0]["VpcId"]

        subnets = self._ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [default_vpc_id]}])
        if not subnets["Subnets"]:
            raise RuntimeError(f"No subnets found in default VPC {default_vpc_id}.")

        subnet_id = subnets["Subnets"][0]["SubnetId"]
        logger.info(f"Auto-discovered subnet_id: {subnet_id}")
        return subnet_id

    def _create_security_group(self):
        # Determine IP to allow
        if self._config.allowed_ip:
            ip_address = self._config.allowed_ip
        else:
            with urllib.request.urlopen("https://checkip.amazonaws.com") as response:
                ip_address = response.read().decode("utf-8").strip()

        # Get VPC ID from Subnet
        subnet_response = self._ec2.describe_subnets(SubnetIds=[self._config.subnet_id])
        vpc_id = subnet_response["Subnets"][0]["VpcId"]

        # Create Security Group
        group_name = f"opengris-orb-sg-{self._template_id}"
        sg_response = self._ec2.create_security_group(
            Description="Temporary security group created for OpenGRIS ORB worker adapter",
            GroupName=group_name,
            VpcId=vpc_id,
        )
        self._created_security_group_id = sg_response["GroupId"]
        logger.info(f"Created security group with ID: {self._created_security_group_id}")

        # Allow ingress
        # TODO: Do the worker processes need to accept incoming connections?
        # If not, we should not open any ports and just rely on outbound connectivity.
        self._ec2.authorize_security_group_ingress(
            GroupId=self._created_security_group_id,
            IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": 1024, "ToPort": 65535, "IpRanges": [{"CidrIp": f"{ip_address}/32"}]}
            ],
        )

    def _create_key_pair(self):
        key_name = f"opengris-orb-key-{self._template_id}"
        self._ec2.create_key_pair(KeyName=key_name)
        self._created_key_name = key_name
        logger.info(f"Created key pair: {key_name}")

    def _cleanup(self):
        if self._cleaned_up:
            return
        self._cleaned_up = True

        if self._connector_external is not None:
            self._connector_external.destroy()

        logger.info("Starting cleanup of ORB and AWS resources...")

        # 1. Shutdown all active worker groups (terminate instances)
        if self._worker_groups and self._orb is not None:
            logger.info(f"Terminating {len(self._worker_groups)} worker groups...")
            instance_ids = [wg_id.decode() for wg_id in self._worker_groups.keys()]
            try:
                # Use ORB to return (terminate) the machines
                self._orb.machines.return_machines(instance_ids)
                logger.info(f"Successfully requested termination of instances: {instance_ids}")
            except Exception as e:
                logger.warning(f"Failed to terminate instances during cleanup: {e}")
            self._worker_groups.clear()

        if self._created_security_group_id is not None:
            try:
                logger.info(f"Deleting AWS security group: {self._created_security_group_id}")
                self._ec2.delete_security_group(GroupId=self._created_security_group_id)
            except Exception as e:
                logger.warning(f"Failed to delete security group {self._created_security_group_id}: {e}")

        if self._created_key_name is not None:
            try:
                logger.info(f"Deleting AWS key pair: {self._created_key_name}")
                self._ec2.delete_key_pair(KeyName=self._created_key_name)
            except Exception as e:
                logger.warning(f"Failed to delete key pair {self._created_key_name}: {e}")

        logger.info("Cleanup completed.")

    def __del__(self):
        self._cleanup()

    async def start_worker_group(self) -> Tuple[WorkerGroupID, Status]:
        if len(self._worker_groups) >= self._max_workers:
            return b"", Status.WorkerGroupTooMuch

        # Request a machine. Note: wait and timeout flags in ORB CLI are currently ignored by the handler,
        # so we must handle the polling ourselves.
        response = self._orb.machines.request(template_id=self._template_id, count=1)

        if not response.request_id:
            logger.error(f"ORB machine request failed to return a request ID. Response: {response}")
            return b"", Status.UnknownAction

        logger.info(f"ORB machine request {response.request_id} submitted, polling for instance IDs...")

        instance_ids = []
        # Poll ORB for instance IDs as the request is processed asynchronously.
        # We manually poll here because the current ORB helper doesn't support blocking requests.
        for _ in range(ORB_MAX_POLLING_ATTEMPTS):
            status_response = self._orb.requests.show(response.request_id)
            logger.debug(f"ORB polling response for {response.request_id}: {status_response}")

            # Try to get instance IDs from multiple possible fields in the response using helper
            instance_ids = status_response.get_instance_ids()

            if instance_ids:
                logger.info(f"ORB request {response.request_id} fulfilled with instance IDs: {instance_ids}")
                break

            if status_response.status in ["failed", "cancelled", "timeout"]:
                error_msg = status_response.status_message or "Unknown failure"
                logger.error(
                    f"ORB machine request {response.request_id} failed"
                    f"with status '{status_response.status}': {error_msg}"
                )
                return b"", Status.UnknownAction

            await asyncio.sleep(ORB_POLLING_INTERVAL_SECONDS)

        if not instance_ids:
            timeout_seconds = ORB_MAX_POLLING_ATTEMPTS * ORB_POLLING_INTERVAL_SECONDS
            logger.error(
                f"ORB machine request {response.request_id} timed out"
                f"waiting for instance IDs after {timeout_seconds}s."
            )
            return b"", Status.UnknownAction

        instance_id = instance_ids[0]
        worker_group_id = instance_id.encode()

        # Deterministic WorkerID calculation to match the user_data script
        worker_id = WorkerID(get_orb_worker_name(instance_id).encode())

        self._worker_groups[worker_group_id] = worker_id
        return worker_group_id, Status.Success

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID) -> Status:
        if not worker_group_id:
            return Status.WorkerGroupIDNotSpecified

        if worker_group_id not in self._worker_groups:
            logger.warning(f"Worker group with ID {bytes(worker_group_id).decode()} does not exist.")
            return Status.WorkerGroupIDNotFound

        instance_id = worker_group_id.decode()
        self._orb.machines.return_machines([instance_id])

        del self._worker_groups[worker_group_id]
        return Status.Success
