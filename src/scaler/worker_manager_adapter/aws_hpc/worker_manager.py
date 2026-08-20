from __future__ import annotations

import logging
from typing import Set

from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig, AWSHPCBackend
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.worker_manager_adapter.aws_hpc.worker import create_aws_batch_worker
from scaler.worker_manager_adapter.proxy_worker_pool import ProxyWorkerPool
from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS, UnitProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess

logger = logging.getLogger(__name__)

SENTINEL_POLL_INTERVAL_SECONDS = 1.0


class BatchWorkerProvisioner(UnitProvisioner):
    """A unit is one local proxy process that submits jobs to AWS Batch.

    There is no child manager below it, so this manager commands the process directly. The unit
    supplies the concurrent job limit as its task concurrency, which is fixed.
    """

    def __init__(self, config: AWSBatchWorkerManagerConfig) -> None:
        self._config = config
        self._base_concurrency = config.max_concurrent_jobs
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._pool = ProxyWorkerPool(self._build_worker, description="Batch worker process")

    def _build_worker(self) -> WorkerProcess:
        config = self._config
        return create_aws_batch_worker(
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
        )

    def register(self, binder: AsyncBinder, children_address: AddressConfig) -> None:
        self._pool.register(binder, children_address)

    async def create_unit(self) -> str:
        return await self._pool.create()

    async def destroy_unit(self, unit_id: str) -> None:
        await self._pool.destroy(unit_id)

    async def shutdown_unit(self, unit_id: str) -> None:
        await self._pool.shutdown(unit_id)

    async def set_unit_task_concurrency(self, unit_id: str, task_concurrency: int) -> None:
        """A proxy process supplies a fixed concurrent job limit, so there is nothing to adjust."""

    async def poll_units(self) -> Set[str]:
        return await self._pool.poll()

    def max_units(self) -> int:
        return UNLIMITED_UNITS

    def task_concurrency_per_unit(self) -> int:
        return self._base_concurrency

    def poll_interval_seconds(self) -> float:
        return SENTINEL_POLL_INTERVAL_SECONDS


class AWSHPCWorkerManager:
    def __init__(self, config: AWSBatchWorkerManagerConfig) -> None:
        self._config = config

    def run(self) -> None:
        config = self._config
        logger.info(f"Starting AWS HPC Worker Manager (backend: {config.backend.name})")
        if config.backend != AWSHPCBackend.batch:
            raise NotImplementedError(f"backend {config.backend.name!r} is not yet implemented")

        provisioner = BatchWorkerProvisioner(config)
        runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_aws_hpc",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=-1,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            children_bind_address=config.worker_manager_config.children_bind_address,
            io_threads=config.worker_config.io_threads,
            workers_per_provisioner_unit=config.max_concurrent_jobs,
        )
        runner.run()
