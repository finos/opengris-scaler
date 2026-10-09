from __future__ import annotations

import asyncio
import logging
import math
import shlex

import boto3

from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.worker_manager.cloud.child_command import (
    CLOUD_UNIT_STARTUP_TIMEOUT_SECONDS,
    child_link_arguments,
    cloud_children_address,
    format_capabilities,
)
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.runner import WorkerManagerRunner

logger = logging.getLogger(__name__)


class ECSWorkerProvisioner(UnitProvisioner):
    def __init__(self, config: ECSWorkerManagerConfig) -> None:
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_config.io_threads
        self._per_worker_task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._max_instances = (
            math.ceil(self._max_task_concurrency / config.ecs_task_cpu) if self._max_task_concurrency != -1 else -1
        )
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._preload = config.worker_config.preload
        self._event_loop = config.worker_config.event_loop

        self._ecs_cluster = config.ecs_cluster
        self._ecs_task_image = config.ecs_task_image
        self._ecs_python_requirements = config.ecs_python_requirements
        self._ecs_python_version = config.ecs_python_version
        self._ecs_task_definition = config.ecs_task_definition
        self._ecs_task_cpu = config.ecs_task_cpu
        self._ecs_task_memory = config.ecs_task_memory
        self._ecs_subnets = config.ecs_subnets
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()
        self._worker_manager_config = config.worker_manager_config

        aws_session = boto3.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )
        self._ecs_client = aws_session.client("ecs")

        resp = self._ecs_client.describe_clusters(clusters=[self._ecs_cluster])
        clusters = resp.get("clusters") or []
        if not clusters or clusters[0]["status"] != "ACTIVE":
            logger.info(f"ECS cluster '{self._ecs_cluster}' missing, creating it.")
            self._ecs_client.create_cluster(clusterName=self._ecs_cluster)

        try:
            resp = self._ecs_client.describe_task_definition(taskDefinition=self._ecs_task_definition)
        except self._ecs_client.exceptions.ClientException:
            logger.info(f"ECS task definition '{self._ecs_task_definition}' missing, creating it.")
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

    def _build_task_command(self, unit_id: str) -> str:
        command = (
            f"scaler_worker_manager baremetal_native {self._worker_scheduler_address!r} "
            f"{child_link_arguments(self._worker_manager_config, unit_id, self._ecs_task_cpu)} "
            f"--worker-type ECS "
            f"--per-worker-task-queue-size {self._per_worker_task_queue_size} "
            f"--heartbeat-interval-seconds {self._heartbeat_interval_seconds} "
            f"--task-timeout-seconds {self._task_timeout_seconds} "
            f"--garbage-collect-interval-seconds {self._garbage_collect_interval_seconds} "
            f"--death-timeout-seconds {self._death_timeout_seconds} "
            f"--trim-memory-threshold-bytes {self._trim_memory_threshold_bytes} "
            f"--event-loop {self._event_loop} "
            f"--io-threads {self._io_threads}"
        )

        if self._hard_processor_suspend:
            command += " --hard-processor-suspend"

        if self._object_storage_address:
            command += f" --object-storage-address {self._object_storage_address!r}"

        if format_capabilities(self._capabilities).strip():
            command += f" --per-worker-capabilities {format_capabilities(self._capabilities)}"

        command += f" --worker-manager-id {self._worker_manager_id.decode()}"

        if self._preload is not None:
            command += f" --preload {shlex.quote(self._preload)}"

        return command

    async def create_unit(self, unit_id: str) -> UnitHandle:
        resp = await asyncio.to_thread(
            self._ecs_client.run_task,
            cluster=self._ecs_cluster,
            taskDefinition=self._ecs_task_definition,
            launchType="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "scaler-container",
                        "environment": [
                            {"name": "COMMAND", "value": self._build_task_command(unit_id)},
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
        if len(tasks) != 1:
            raise RuntimeError(f"ECS run task returned {len(tasks)} tasks, expected one")

        task_arn = tasks[0]["taskArn"]
        logger.info(f"started ECS task {task_arn!r}")
        return task_arn

    async def destroy_unit(self, handle: UnitHandle) -> None:
        resp = await asyncio.to_thread(
            self._ecs_client.stop_task,
            cluster=self._ecs_cluster,
            task=handle,
            reason="Shutdown requested by ECS worker manager",
        )
        failures = resp.get("failures") or []
        if failures:
            raise RuntimeError(f"ECS stop task {handle!r} failed: {failures}")
        logger.info(f"stopped ECS task {handle!r}")

    def max_units(self) -> int:
        return self._max_instances

    def task_concurrency_per_unit(self) -> int:
        return self._ecs_task_cpu

    def startup_timeout_seconds(self) -> int:
        return CLOUD_UNIT_STARTUP_TIMEOUT_SECONDS


class ECSWorkerManager:
    def __init__(self, config: ECSWorkerManagerConfig) -> None:
        self._runner = WorkerManagerRunner(
            name="worker_manager_ecs",
            worker_manager_config=config.worker_manager_config,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            provisioner=ECSWorkerProvisioner(config),
            children_address=cloud_children_address(config.worker_manager_config),
            io_threads=config.worker_config.io_threads,
        )

    def run(self) -> None:
        self._runner.run()
