from __future__ import annotations

import logging
from typing import Set

from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig, AWSHPCBackend
from scaler.config.types.address import AddressConfig
from scaler.worker_manager.local_process import (
    LOCAL_PROCESS_POLL_INTERVAL_SECONDS,
    local_children_address,
    poll_local_processes,
    stop_local_process,
)
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.proxy.aws_batch.worker import create_aws_batch_worker
from scaler.worker_manager.proxy.worker_process import WorkerProcess
from scaler.worker_manager.runner import WorkerManagerRunner

logger = logging.getLogger(__name__)


class AWSBatchWorkerProvisioner(UnitProvisioner):
    def __init__(self, config: AWSBatchWorkerManagerConfig, children_address: AddressConfig) -> None:
        self._children_address = children_address
        self._config = config
        self._base_concurrency = config.max_concurrent_jobs
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities

    async def create_unit(self, unit_id: str) -> UnitHandle:
        config = self._config
        worker = create_aws_batch_worker(
            name=config.name,
            address=config.worker_manager_config.effective_worker_scheduler_address,
            object_storage_address=config.worker_manager_config.object_storage_address,
            job_queue=config.job_queue,
            job_definition=config.job_definition,
            aws_region=config.aws_region,
            s3_bucket=config.s3_bucket,
            s3_prefix=config.s3_prefix,
            capabilities=self._capabilities,
            base_concurrency=self._base_concurrency,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            death_timeout_seconds=config.worker_config.death_timeout_seconds,
            task_queue_size=config.worker_config.per_worker_task_queue_size,
            io_threads=config.worker_config.io_threads,
            event_loop=config.worker_config.event_loop,
            job_timeout_seconds=config.job_timeout_minutes * 60,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_manager_address=self._children_address,
            unit_id=unit_id,
        )
        worker.start()
        logger.info(f"started Batch worker process {worker.name!r}")
        return worker

    async def destroy_unit(self, handle: UnitHandle) -> None:
        assert isinstance(handle, WorkerProcess)
        await stop_local_process(handle)
        logger.info(f"stopped Batch worker process {handle.name!r}")

    async def poll_units(self, handles: Set[UnitHandle]) -> Set[UnitHandle]:
        return set(poll_local_processes({handle for handle in handles if isinstance(handle, WorkerProcess)}))

    def max_units(self) -> int:
        return -1

    def task_concurrency_per_unit(self) -> int:
        return self._base_concurrency

    def poll_interval_seconds(self) -> int:
        return LOCAL_PROCESS_POLL_INTERVAL_SECONDS


class AWSBatchWorkerManager:
    def __init__(self, config: AWSBatchWorkerManagerConfig) -> None:
        self._config = config

    def run(self) -> None:
        config = self._config
        logger.info(f"Starting AWS Batch Worker Manager (backend: {config.backend.name})")
        if config.backend != AWSHPCBackend.batch:
            raise NotImplementedError(f"backend {config.backend.name!r} is not yet implemented")

        children_address = local_children_address(config.worker_manager_config)

        provisioner = AWSBatchWorkerProvisioner(config, children_address)
        runner = WorkerManagerRunner(
            name="worker_manager_aws_hpc",
            worker_manager_config=config.worker_manager_config,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            provisioner=provisioner,
            children_address=children_address,
            io_threads=config.worker_config.io_threads,
        )
        runner.run()
