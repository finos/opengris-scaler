import asyncio
import json
import logging
import os
import urllib.request
from dataclasses import asdict
from typing import Any, Dict, Optional

import boto3
from aiohttp import web
from aiohttp.web_request import Request

from scaler.config.section.orb_worker_adapter import ORBWorkerAdapterConfig
from scaler.utility.formatter import camelcase_dict
from scaler.utility.identifiers import WorkerID
from scaler.worker_adapter.common import (
    CapacityExceededError,
    WorkerGroupID,
    WorkerGroupNotFoundError,
    format_capabilities,
)
from scaler.worker_adapter.orb.helper import ORBHelper
from scaler.worker_adapter.orb.types import ORBTemplate

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


class ORBAdapter:
    _config: ORBWorkerAdapterConfig
    _orb: ORBHelper
    _worker_groups: Dict[WorkerGroupID, WorkerID]
    _template_id: str
    _created_security_group_id: Optional[str]
    _created_key_name: Optional[str]
    _ec2: Optional[Any]

    def __init__(self, config: ORBWorkerAdapterConfig):
        self._config = config
        self._orb = None
        self._created_security_group_id = None
        self._created_key_name = None

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
        templates_file_path = os.path.join(self._orb.cwd, "config", "awsprov_templates.json")
        with open(templates_file_path, "w") as f:
            template_dict = asdict(template)
            # Remove empty list that might overwrite the subnet_id field
            if not template_dict.get("subnet_ids"):
                del template_dict["subnet_ids"]

            json.dump({"templates": [camelcase_dict(template_dict)]}, f, indent=4)

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

    async def cleanup(self, app):
        self._cleanup()

    def __del__(self):
        self._cleanup()

    async def start_worker_group(self) -> WorkerGroupID:
        if len(self._worker_groups) >= self._config.worker_adapter_config.max_workers:
            raise CapacityExceededError(
                f"Maximum number of instances ({self._config.worker_adapter_config.max_workers}) reached."
            )

        # Request a machine. Note: wait and timeout flags in ORB CLI are currently ignored by the handler,
        # so we must handle the polling ourselves.
        response = self._orb.machines.request(template_id=self._template_id, count=1)

        if not response.request_id:
            raise RuntimeError(f"ORB machine request failed to return a request ID. Response: {response}")

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
                raise RuntimeError(
                    f"ORB machine request {response.request_id} failed"
                    f"with status '{status_response.status}': {error_msg}"
                )

            await asyncio.sleep(ORB_POLLING_INTERVAL_SECONDS)

        if not instance_ids:
            timeout_seconds = ORB_MAX_POLLING_ATTEMPTS * ORB_POLLING_INTERVAL_SECONDS
            raise RuntimeError(
                f"ORB machine request {response.request_id} timed out"
                f"waiting for instance IDs after {timeout_seconds}s."
            )

        instance_id = instance_ids[0]
        worker_group_id = instance_id.encode()

        # Deterministic WorkerID calculation to match the user_data script
        worker_id = WorkerID(get_orb_worker_name(instance_id).encode())

        self._worker_groups[worker_group_id] = worker_id
        return worker_group_id

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID):
        if worker_group_id not in self._worker_groups:
            raise WorkerGroupNotFoundError(f"Worker group with ID {worker_group_id.decode()} does not exist.")

        instance_id = worker_group_id.decode()
        self._orb.machines.return_machines([instance_id])

        del self._worker_groups[worker_group_id]

    async def webhook_handler(self, request: Request):
        request_json = await request.json()

        if "action" not in request_json:
            return web.json_response({"error": "No action specified"}, status=web.HTTPBadRequest.status_code)

        action = request_json["action"]

        if action == "get_worker_adapter_info":
            # Assuming 1 worker per machine for now, similar to Native adapter
            return web.json_response(
                {
                    "max_worker_groups": self._config.worker_adapter_config.max_workers,
                    "workers_per_group": 1,
                    "base_capabilities": {},  # TODO: Fill with capabilities if available/relevant
                },
                status=web.HTTPOk.status_code,
            )

        elif action == "start_worker_group":
            try:
                worker_group_id = await self.start_worker_group()
            except CapacityExceededError as e:
                logger.warning(f"Capacity exceeded when starting worker group: {e}")
                return web.json_response({"error": str(e)}, status=web.HTTPTooManyRequests.status_code)
            except Exception as e:
                logger.exception(f"Unexpected error starting worker group: {e}")
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response(
                {
                    "status": "Worker group started",
                    "worker_group_id": worker_group_id.decode(),
                    "worker_ids": [self._worker_groups[worker_group_id].decode()],
                },
                status=web.HTTPOk.status_code,
            )

        elif action == "shutdown_worker_group":
            if "worker_group_id" not in request_json:
                return web.json_response(
                    {"error": "No worker_group_id specified"}, status=web.HTTPBadRequest.status_code
                )

            worker_group_id = request_json["worker_group_id"].encode()
            try:
                await self.shutdown_worker_group(worker_group_id)
            except WorkerGroupNotFoundError as e:
                logger.warning(f"Worker group not found for shutdown: {e}")
                return web.json_response({"error": str(e)}, status=web.HTTPNotFound.status_code)
            except Exception as e:
                logger.exception(f"Unexpected error shutting down worker group {worker_group_id.decode()}: {e}")
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response({"status": "Worker group shutdown"}, status=web.HTTPOk.status_code)

        else:
            return web.json_response({"error": "Unknown action"}, status=web.HTTPBadRequest.status_code)

    def create_app(self):
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        app.on_cleanup.append(self.cleanup)
        return app
