import asyncio
import json
import logging
import os
import signal
import tempfile
import uuid
from typing import Any, Dict, List, Optional, Tuple

import boto3
import zmq
from orb import ORBClient

from scaler.config.section.orb_worker_adapter import ORBWorkerAdapterConfig
from scaler.io.mixins import AsyncConnector
from scaler.io.utility import create_async_connector, create_async_simple_context
from scaler.io.ymq import ymq
from scaler.protocol.python.message import (
    Message,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.common import WorkerGroupID, format_capabilities

Status = WorkerManagerCommandResponse.Status
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
    _sdk: Optional[ORBClient]
    _worker_groups: Dict[WorkerGroupID, WorkerID]
    _template_id: str
    _created_security_group_id: Optional[str]
    _created_key_name: Optional[str]
    _ec2: Optional[Any]

    def __init__(self, config: ORBWorkerAdapterConfig):
        self._config = config
        self._address = config.worker_manager_config.scheduler_address
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._max_workers = config.worker_manager_config.max_workers
        self._workers_per_group = 1

        self._event_loop = config.event_loop
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file

        self._sdk: Optional[ORBClient] = None
        self._config_temp_dir: Optional[tempfile.TemporaryDirectory] = None
        self._ec2: Optional[Any] = None
        self._context = None
        self._connector_external: Optional[AsyncConnector] = None
        self._created_security_group_id: Optional[str] = None
        self._created_key_name: Optional[str] = None
        self._cleaned_up = False
        self._worker_groups: Dict[WorkerGroupID, WorkerID] = {}
        self._ident: bytes = b"worker_manager_orb|uninitialized"
        self._subnet_id: Optional[str] = None

    def _build_app_config(self, data_dir: str) -> dict:
        region = self._config.aws_region or "us-east-1"
        return {
            "version": "2.0.0",
            "provider": {
                "selection_policy": "FIRST_AVAILABLE",
                "providers": [
                    {"name": "aws-default", "type": "aws", "enabled": True, "priority": 1, "config": {"region": region}}
                ],
            },
            "storage": {
                "strategy": "json",
                "json_strategy": {"storage_type": "single_file", "base_path": data_dir},
            },
        }

    @staticmethod
    def _patch_orb_template_repository() -> None:
        """Monkey-patch three API mismatches in ORB 1.2.2:
        - TemplateRepositoryImpl.get_by_id: handler passes command.template_id (str) but method calls .value on it
        - TemplateRepositoryImpl.add: handler calls uow.templates.add() but repository only has save()
        - Template.get_domain_events / clear_domain_events: save() calls these but the Pydantic model lacks them
        All are broken in the installed release without modifying ORB files.
        """
        from orb.domain.template.template_aggregate import Template
        from orb.infrastructure.storage.repositories.template_repository import TemplateRepositoryImpl

        original_get_by_id = TemplateRepositoryImpl.get_by_id

        def _get_by_id(self, template_id):
            if not hasattr(template_id, "value"):
                class _StrId:
                    value = str(template_id)
                template_id = _StrId()
            return original_get_by_id(self, template_id)

        TemplateRepositoryImpl.get_by_id = _get_by_id

        if not hasattr(TemplateRepositoryImpl, "add"):
            TemplateRepositoryImpl.add = TemplateRepositoryImpl.save

        if not hasattr(Template, "get_domain_events"):
            Template.get_domain_events = lambda self: []
        if not hasattr(Template, "clear_domain_events"):
            Template.clear_domain_events = lambda self: None

    async def __initialize(self):
        region = self._config.aws_region or "us-east-1"
        self._patch_orb_template_repository()
        self._config_temp_dir = tempfile.TemporaryDirectory()
        data_dir = os.path.join(self._config_temp_dir.name, "data")
        os.makedirs(data_dir, exist_ok=True)
        config_file = os.path.join(self._config_temp_dir.name, "config.json")
        with open(config_file, "w") as f:
            json.dump(self._build_app_config(data_dir), f)
        # ORB_CONFIG_DIR is read by get_config_location() when ConfigurationManager()
        # is instantiated with no args inside create_container(). Setting it here
        # ensures the singleton picks up our config before any file-path arg would
        # take effect (which is applied too late — after the singleton is already built).
        os.environ["ORB_CONFIG_DIR"] = self._config_temp_dir.name
        self._sdk = ORBClient()
        await self._sdk.initialize()

        self._ec2 = boto3.client("ec2", region_name=region)
        self._subnet_id = self._config.subnet_id or self._discover_default_subnet()
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

        await self._sdk.create_template(
            template_id=self._template_id,
            image_id=self._config.image_id,
            provider_api="RunInstances",
            instance_type=self._config.instance_type,
            configuration={
                "template_id": self._template_id,
                "image_id": self._config.image_id,
                "provider_api": "RunInstances",
                "instance_type": self._config.instance_type,
                "max_instances": self._config.worker_manager_config.max_workers,
                "provider_name": "aws-default",
                "machine_types": {self._config.instance_type: 1},
                "subnet_ids": [self._subnet_id],
                "security_group_ids": security_group_ids,
                "key_name": key_name,
                "user_data": user_data,
                "metadata": {
                    "attributes": {
                        "type": ["String", "X86_64"],
                        "ncpus": ["Numeric", "1"],
                        "nram": ["Numeric", "1024"],
                        "ncores": ["Numeric", "1"],
                    }
                },
            },
        )

        self._context = create_async_simple_context()
        self._name = "worker_manager_orb"
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
        if isinstance(message, WorkerManagerCommand):
            await self._handle_command(message)
        elif isinstance(message, WorkerManagerHeartbeatEcho):
            pass
        else:
            logging.warning(f"Received unknown message type: {type(message)}")

    async def _handle_command(self, command: WorkerManagerCommand):
        cmd_type = command.command
        worker_group_id = command.worker_group_id
        response_status = Status.Success
        worker_ids: List[bytes] = []
        capabilities: Dict[str, int] = {}

        cmd_res = WorkerManagerCommandType.StartWorkerGroup
        if cmd_type == WorkerManagerCommandType.StartWorkerGroup:
            cmd_res = WorkerManagerCommandType.StartWorkerGroup
            worker_group_id, response_status = await self.start_worker_group()
            if response_status == Status.Success:
                worker_ids = [bytes(self._worker_groups[worker_group_id])]
                capabilities = self._capabilities
        elif cmd_type == WorkerManagerCommandType.ShutdownWorkerGroup:
            cmd_res = WorkerManagerCommandType.ShutdownWorkerGroup
            response_status = await self.shutdown_worker_group(worker_group_id)
        else:
            raise ValueError("Unknown Command")

        assert self._connector_external is not None
        await self._connector_external.send(
            WorkerManagerCommandResponse.new_msg(
                worker_group_id=bytes(worker_group_id),
                command=cmd_res,
                status=response_status,
                worker_ids=worker_ids,
                capabilities=capabilities,
            )
        )

    async def __send_heartbeat(self):
        assert self._connector_external is not None
        await self._connector_external.send(
            WorkerManagerHeartbeat.new_msg(
                max_worker_groups=self._max_workers,
                workers_per_group=self._workers_per_group,
                capabilities=self._capabilities,
                worker_manager_id=self._ident,
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
        await self.__initialize()
        self._task = self._loop.create_task(self.__get_loops())
        self.__register_signal()
        await self._task

    async def __get_loops(self):
        assert self._connector_external is not None
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
        adapter_config = self._config.worker_manager_config

        # We assume 1 worker per machine for ORB
        # TODO: Add support for multiple workers per machine if needed
        num_workers = 1

        # Build the command.
        # NOTE: The worker IDs reported to the scheduler in StartWorkerGroup responses are computed
        # deterministically from the EC2 instance ID, but the workers launched here will self-report
        # different random IDs (Worker|ORB|<uuid>|<uuid>). Since workers connect to the scheduler
        # independently in fixed mode, the scheduler won't notice this mismatch and tasks will still
        # be processed correctly. Worker group membership tracking in the scheduler will be inaccurate.
        script = f"""#!/bin/bash
nohup /usr/local/bin/scaler_cluster {adapter_config.scheduler_address.to_address()} \
    --num-of-workers {num_workers} \
    --worker-type ORB \
    --per-worker-task-queue-size {worker_config.per_worker_task_queue_size} \
    --heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} \
    --task-timeout-seconds {worker_config.task_timeout_seconds} \
    --garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} \
    --death-timeout-seconds {worker_config.death_timeout_seconds} \
    --trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} \
    --event-loop {self._config.event_loop} \
    --worker-io-threads {self._config.worker_io_threads}"""

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
        # Get VPC ID from Subnet
        subnet_response = self._ec2.describe_subnets(SubnetIds=[self._subnet_id])
        vpc_id = subnet_response["Subnets"][0]["VpcId"]

        # Create Security Group (outbound-only — workers connect out to scheduler via ZMQ)
        group_name = f"opengris-orb-sg-{self._template_id}"
        sg_response = self._ec2.create_security_group(
            Description="Temporary security group created for OpenGRIS ORB worker adapter",
            GroupName=group_name,
            VpcId=vpc_id,
        )
        self._created_security_group_id = sg_response["GroupId"]
        logger.info(f"Created security group with ID: {self._created_security_group_id}")

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
        if self._worker_groups and self._sdk is not None:
            logger.info(f"Terminating {len(self._worker_groups)} worker groups...")
            instance_ids = [wg_id.decode() for wg_id in self._worker_groups.keys()]
            try:
                self._loop.run_until_complete(self._sdk.create_return_request(machine_ids=instance_ids))
                logger.info(f"Successfully requested termination of instances: {instance_ids}")
            except Exception as e:
                logger.warning(f"Failed to terminate instances during cleanup: {e}")
            self._worker_groups.clear()

        if self._sdk is not None:
            try:
                self._loop.run_until_complete(self._sdk.cleanup())
            except Exception as e:
                logger.warning(f"SDK cleanup failed: {e}")
            self._sdk = None

        if self._config_temp_dir is not None:
            self._config_temp_dir.cleanup()
            self._config_temp_dir = None
            os.environ.pop("ORB_CONFIG_DIR", None)

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

    async def _poll_for_instance_id(self, request_id: str) -> Optional[str]:
        for _ in range(ORB_MAX_POLLING_ATTEMPTS):
            response = await self._sdk.get_request(request_id=request_id)
            logger.debug(f"ORB polling response for {request_id}: {response}")

            instance_ids = response.get("machine_ids") or []
            if instance_ids:
                logger.info(f"ORB request {request_id} fulfilled with instance IDs: {instance_ids}")
                return instance_ids[0]

            status = response.get("status", "")
            if status in ("failed", "cancelled", "timeout"):
                error_msg = response.get("status_message") or response.get("message") or "Unknown failure"
                logger.error(f"ORB machine request {request_id} failed with status '{status}': {error_msg}")
                return None

            await asyncio.sleep(ORB_POLLING_INTERVAL_SECONDS)

        timeout_seconds = ORB_MAX_POLLING_ATTEMPTS * ORB_POLLING_INTERVAL_SECONDS
        logger.error(f"ORB machine request {request_id} timed out waiting for instance IDs after {timeout_seconds}s.")
        return None

    async def start_worker_group(self) -> Tuple[WorkerGroupID, Status]:
        if len(self._worker_groups) >= self._max_workers != -1:
            return b"", Status.WorkerGroupTooMuch

        response = await self._sdk.create_request(template_id=self._template_id, count=1)
        request_id = (response or {}).get("created_request_id")

        if not request_id:
            logger.error(f"ORB machine request failed to return a request ID. Response: {response}")
            return b"", Status.UnknownAction

        logger.info(f"ORB machine request {request_id} submitted, polling for instance IDs...")

        instance_id = await self._poll_for_instance_id(request_id)
        if not instance_id:
            return b"", Status.UnknownAction

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
        await self._sdk.create_return_request(machine_ids=[instance_id])

        del self._worker_groups[worker_group_id]
        return Status.Success
