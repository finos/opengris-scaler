import base64
import json
import logging
import os
import tempfile
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
from scaler.worker_adapter.orb.exception import ORBException
from scaler.worker_adapter.orb.helper import ORBHelper
from scaler.worker_adapter.orb.types import ORBTemplate

logger = logging.getLogger(__name__)


class ORBAdapter:
    _config: ORBWorkerAdapterConfig
    _orb: ORBHelper
    _worker_groups: Dict[WorkerGroupID, WorkerID]
    _template_id: str
    _created_security_group_id: Optional[str]
    _ec2: Optional[Any]

    def __init__(self, config: ORBWorkerAdapterConfig):
        self._config = config
        self._orb = ORBHelper(config.orb_config_path)
        self._worker_groups = {}
        self._created_security_group_id = None
        self._ec2 = boto3.client("ec2", region_name=self._config.aws_region)

        self._template_id = os.urandom(8).hex()

        if not self._config.subnet_id:
            self._config.subnet_id = self._discover_default_subnet()

        security_group_ids = self._config.security_group_ids
        if not security_group_ids:
            self._create_security_group()
            security_group_ids = [self._created_security_group_id]

        user_data = self._create_user_data()

        template = ORBTemplate(
            template_id=self._template_id,
            max_instances=self._config.worker_adapter_config.max_workers,
            provider_api="RunInstances",
            image_id=self._config.image_id,
            vm_type=self._config.instance_type,
            subnet_id=self._config.subnet_id,
            security_group_ids=security_group_ids,
            key_name="",
            user_data=user_data,
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
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as f:
            json.dump({"templates": [camelcase_dict(asdict(template))]}, f)
            self._orb.templates.create(f.name)

    def _create_user_data(self) -> str:
        worker_config = self._config.worker_config
        adapter_config = self._config.worker_adapter_config

        # We assume 1 worker per machine for ORB
        num_workers = 1

        # Build the command
        # We construct the full WorkerID here so it's deterministic and matches what the adapter calculates
        # We fetch instance_id once and use it to construct the ID

        script = f"""#!/bin/bash
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
TAG=${{INSTANCE_ID//i-/}}
WORKER_NAME="Worker|ORB|${{INSTANCE_ID}}|${{TAG}}"

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
    --no-random-worker-ids"""

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

        return base64.b64encode(script.encode()).decode()

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
            Description="Security group for OpenGRIS ORB worker adapter", GroupName=group_name, VpcId=vpc_id
        )
        self._created_security_group_id = sg_response["GroupId"]

        # Allow ingress
        self._ec2.authorize_security_group_ingress(
            GroupId=self._created_security_group_id,
            IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535, "IpRanges": [{"CidrIp": f"{ip_address}/32"}]}
            ],
        )

    def __del__(self):
        try:
            self._orb.templates.delete(self._template_id)
        except ORBException as e:
            logger.exception(f"Failed to delete ORB template {self._template_id}: {e}")

        if self._created_security_group_id:
            try:
                self._ec2.delete_security_group(GroupId=self._created_security_group_id)
            except Exception:
                logger.exception(f"Failed to delete security group {self._created_security_group_id}")

    async def start_worker_group(self) -> WorkerGroupID:
        if len(self._worker_groups) >= self._config.worker_adapter_config.max_workers:
            raise CapacityExceededError(
                f"Maximum number of instances ({self._config.worker_adapter_config.max_workers}) reached."
            )

        # Request a machine
        response = self._orb.machines.request(template_id=self._template_id, count=1, wait=True)

        instance_id = response.instance_ids[0]
        worker_group_id = instance_id.encode()

        # Deterministic WorkerID calculation to match the user_data script
        # Format: Worker|ORB|{instance_id}|{instance_id_without_prefix}
        tag = instance_id.replace("i-", "")
        worker_id = WorkerID(f"Worker|ORB|{instance_id}|{tag}".encode())

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
                return web.json_response({"error": str(e)}, status=web.HTTPTooManyRequests.status_code)
            except Exception as e:
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
                return web.json_response({"error": str(e)}, status=web.HTTPNotFound.status_code)
            except Exception as e:
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response({"status": "Worker group shutdown"}, status=web.HTTPOk.status_code)

        else:
            return web.json_response({"error": "Unknown action"}, status=web.HTTPBadRequest.status_code)

    def create_app(self):
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        return app
