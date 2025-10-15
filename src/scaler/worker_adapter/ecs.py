import logging
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Set

import boto3
from aiohttp import web
from aiohttp.web_request import Request

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.identifiers import WorkerID
from scaler.worker_adapter.common import (
    CapacityExceededError,
    WorkerGroupID,
    WorkerGroupNotFoundError,
    format_capabilities,
)


@dataclass
class WorkerGroupInfo:
    worker_ids: Set[WorkerID]
    task_arn: str


class ECSWorkerAdapter:
    def __init__(
        self,
        address: ZMQConfig,
        object_storage_address: Optional[ObjectStorageConfig],
        capabilities: Dict[str, int],
        io_threads: int,
        per_worker_task_queue_size: int,
        max_instances: int,
        heartbeat_interval_seconds: int,
        task_timeout_seconds: int,
        death_timeout_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        hard_processor_suspend: bool,
        event_loop: str,
        # AWS ECS specific configuration
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_region: str,
        ecs_subnets: list,
        ecs_cluster: str,
        ecs_task_image: str,
        ecs_python_requirements: str,
        ecs_python_version: str,
        ecs_task_definition: str,
        ecs_task_cpu: int,  # 4 vCPU
        ecs_task_memory: int,  # 30 GB, Fargate has weird supported sizes
    ):
        self._address = address
        self._object_storage_address = object_storage_address
        self._capabilities = capabilities
        self._io_threads = io_threads
        self._per_worker_task_queue_size = per_worker_task_queue_size
        self._max_instances = max_instances
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._task_timeout_seconds = task_timeout_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._hard_processor_suspend = hard_processor_suspend
        self._event_loop = event_loop

        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_region = aws_region

        self._ecs_cluster = ecs_cluster
        self._ecs_task_image = ecs_task_image
        self._ecs_python_requirements = ecs_python_requirements
        self._ecs_python_version = ecs_python_version
        self._ecs_task_definition = ecs_task_definition
        self._ecs_task_cpu = ecs_task_cpu
        self._ecs_task_memory = ecs_task_memory
        self._ecs_subnets = ecs_subnets

        aws_session = boto3.Session(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            region_name=self._aws_region,
        )
        self._ecs_client = aws_session.client("ecs")

        resp = self._ecs_client.describe_clusters(clusters=[self._ecs_cluster])
        clusters = resp.get("clusters") or []
        if not clusters or clusters[0]["status"] != "ACTIVE":
            logging.info(f"ECS cluster '{self._ecs_cluster}' missing, creating it.")
            self._ecs_client.create_cluster(clusterName=self._ecs_cluster)

        self._worker_groups: Dict[WorkerGroupID, WorkerGroupInfo] = {}

        try:
            resp = self._ecs_client.describe_task_definition(taskDefinition=self._ecs_task_definition)
        except self._ecs_client.exceptions.ClientException:
            logging.info(f"ECS task definition '{self._ecs_task_definition}' missing, creating it.")
            iam_client = aws_session.client("iam")
            try:
                resp = iam_client.get_role(RoleName="ecsTaskExecutionRole")
                execution_role_arn = resp["Role"]["Arn"]
            except iam_client.exceptions.NoSuchEntityException:
                resp = iam_client.create_role(
                    RoleName="ecsTaskExecutionRole",
                    AssumeRolePolicyDocument=(
                        '{"Version": "2012-10-17", '
                        '"Statement": [{"Effect": "Allow", '
                        '"Principal": {"Service": "ecs-tasks.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'
                    ),
                )
                execution_role_arn = resp["Role"]["Arn"]
                iam_client.attach_role_policy(
                    RoleName="ecsTaskExecutionRole",
                    PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
                )

            resp = self._ecs_client.register_task_definition(
                family=self._ecs_task_definition,
                cpu=str(self._ecs_task_cpu * 1024),
                memory=str(self._ecs_task_memory * 1024),
                runtimePlatform={"cpuArchitecture": "X86_64", "operatingSystemFamily": "LINUX"},
                networkMode="awsvpc",
                containerDefinitions=[{"name": "scaler-container", "image": self._ecs_task_image, "essential": True}],
                requiresCompatibilities=["FARGATE"],
                executionRoleArn=execution_role_arn,
            )
        self._ecs_task_definition = resp["taskDefinition"]["taskDefinitionArn"]

    async def start_worker_group(self) -> WorkerGroupID:
        if len(self._worker_groups) >= self._max_instances != -1:
            raise CapacityExceededError(f"Maximum number of instances ({self._max_instances}) reached.")

        worker_names = [f"ECS|{uuid.uuid4().hex}" for _ in range(self._ecs_task_cpu)]
        command = (
            f"scaler_cluster {self._address.to_address()} "
            f"--num-of-workers {self._ecs_task_cpu} "
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

        resp = self._ecs_client.run_task(
            cluster=self._ecs_cluster,
            taskDefinition=self._ecs_task_definition,
            launchType="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "scaler-container",
                        "environment": [
                            {"name": "COMMAND", "value": command},
                            {"name": "PYTHON_REQUIREMENTS", "value": self._ecs_python_requirements},
                            {"name": "PYTHON_VERSION", "value": self._ecs_python_version},
                        ],
                    }
                ]
            },
            networkConfiguration={"awsvpcConfiguration": {"subnets": self._ecs_subnets, "assignPublicIp": "ENABLED"}},
        )

        failures = resp.get("failures") or []
        if failures:
            raise RuntimeError(f"ECS run task failed: {failures}")

        tasks = resp.get("tasks") or []
        if not tasks:
            raise RuntimeError("ECS run task returned no tasks")
        if len(tasks) > 1:
            raise RuntimeError("ECS run task returned multiple tasks, expected only one")

        task_arn = tasks[0]["taskArn"]
        worker_group_id = f"ecs-{uuid.uuid4().hex}".encode()
        self._worker_groups[worker_group_id] = WorkerGroupInfo(
            worker_ids={WorkerID.generate_worker_id(worker_name) for worker_name in worker_names}, task_arn=task_arn
        )
        return worker_group_id

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID):
        if worker_group_id not in self._worker_groups:
            raise WorkerGroupNotFoundError(f"Worker group with ID {worker_group_id.decode()} does not exist.")

        resp = self._ecs_client.stop_task(
            cluster=self._ecs_cluster,
            task=self._worker_groups[worker_group_id].task_arn,
            reason="Shutdown requested by ecs adapter",
        )
        failures = resp.get("failures") or []
        if failures:
            raise RuntimeError(f"ECS stop task failed: {failures}")

        self._worker_groups.pop(worker_group_id)

    async def webhook_handler(self, request: Request):
        request_json = await request.json()

        if "action" not in request_json:
            return web.json_response({"error": "No action specified"}, status=web.HTTPBadRequest.status_code)

        action = request_json["action"]

        if action == "get_worker_adapter_info":
            return web.json_response(
                {
                    "max_worker_groups": self._max_instances,
                    "workers_per_group": self._ecs_task_cpu,
                    "base_capabilities": self._capabilities,
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
                    "worker_ids": [worker_id.decode() for worker_id in self._worker_groups[worker_group_id].worker_ids],
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
