import asyncio
import logging
import signal
import uuid
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

import boto3
import zmq

from scaler.config.section.ecs_worker_adapter import ECSWorkerAdapterConfig
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

Status = WorkerAdapterCommandResponse.Status


@dataclass
class WorkerGroupInfo:
    worker_ids: Set[WorkerID]
    task_arn: str


class ECSWorkerAdapter:
    def __init__(self, config: ECSWorkerAdapterConfig):
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

        self._aws_access_key_id = config.aws_access_key_id
        self._aws_secret_access_key = config.aws_secret_access_key
        self._aws_region = config.aws_region

        self._ecs_cluster = config.ecs_cluster
        self._ecs_task_image = config.ecs_task_image
        self._ecs_python_requirements = config.ecs_python_requirements
        self._ecs_python_version = config.ecs_python_version
        self._ecs_task_definition = config.ecs_task_definition
        self._ecs_task_cpu = config.ecs_task_cpu
        self._ecs_task_memory = config.ecs_task_memory
        self._ecs_subnets = config.ecs_subnets

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

        self._context = create_async_simple_context()
        self._name = "worker_adapter_ecs"
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
                workers_per_group=self._ecs_task_cpu,
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
        except ymq.YMQException as e:
            if e.code == ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                pass
            else:
                logging.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")

    async def start_worker_group(self) -> Tuple[WorkerGroupID, Status]:
        if len(self._worker_groups) >= self._max_instances != -1:
            return b"", Status.WorkerGroupTooMuch

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
        return worker_group_id, Status.Success

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID) -> Status:
        if not worker_group_id:
            return Status.WorkerGroupIDNotSpecified

        if worker_group_id not in self._worker_groups:
            logging.warning(f"Worker group with ID {bytes(worker_group_id).decode()} does not exist.")
            return Status.WorkerGroupIDNotFound

        resp = self._ecs_client.stop_task(
            cluster=self._ecs_cluster,
            task=self._worker_groups[worker_group_id].task_arn,
            reason="Shutdown requested by ecs adapter",
        )
        failures = resp.get("failures") or []
        if failures:
            logging.error(f"ECS stop task failed: {failures}")
            return Status.UnknownAction

        self._worker_groups.pop(worker_group_id)
        return Status.Success
