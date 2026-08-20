from __future__ import annotations

import itertools
import logging
from typing import Set

from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.worker_manager_adapter.oci_hpc.worker import create_oci_hpc_worker
from scaler.worker_manager_adapter.proxy_worker_pool import ProxyWorkerPool
from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS, UnitProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess

logger = logging.getLogger(__name__)

SENTINEL_POLL_INTERVAL_SECONDS = 1.0


class OCIHPCWorkerProvisioner(UnitProvisioner):
    """A unit is one local proxy process that runs jobs as OCI container instances."""

    def __init__(self, config: OCIHPCWorkerManagerConfig) -> None:
        self._config = config
        self._base_concurrency = config.base_concurrency
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._names = itertools.count()
        self._pool = ProxyWorkerPool(self._build_worker, description="OCI HPC worker process")

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

    def poll_interval_seconds(self) -> float:
        return SENTINEL_POLL_INTERVAL_SECONDS

    def max_units(self) -> int:
        return UNLIMITED_UNITS

    def task_concurrency_per_unit(self) -> int:
        return self._base_concurrency

    def _build_worker(self) -> WorkerProcess:
        config = self._config
        container_instance_config = config.container_instance_config
        return create_oci_hpc_worker(
            name=f"oci-hpc-{next(self._names)}",
            address=config.worker_manager_config.effective_worker_scheduler_address,
            object_storage_address=config.worker_manager_config.object_storage_address,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            compartment_id=container_instance_config.compartment_id,
            availability_domain=container_instance_config.availability_domain,
            subnet_id=container_instance_config.subnet_id,
            container_image=container_instance_config.container_image,
            oci_region=container_instance_config.oci_region,
            object_storage_namespace=config.object_storage_namespace,
            object_storage_bucket=config.object_storage_bucket,
            object_storage_prefix=config.object_storage_prefix,
            instance_shape=container_instance_config.instance_shape,
            instance_ocpus=config.instance_ocpus,
            instance_memory_gb=config.instance_memory_gb,
            capabilities=self._capabilities,
            base_concurrency=self._base_concurrency,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            death_timeout_seconds=config.worker_config.death_timeout_seconds,
            task_queue_size=config.worker_config.per_worker_task_queue_size,
            io_threads=config.worker_config.io_threads,
            event_loop=config.worker_config.event_loop,
            job_timeout_seconds=config.job_timeout_seconds,
            oci_profile=container_instance_config.oci_profile,
            auth_type=container_instance_config.auth_type,
        )


class OCIHPCWorkerManager:
    def __init__(self, config: OCIHPCWorkerManagerConfig) -> None:
        self._config = config

    def run(self) -> None:
        config = self._config
        logger.info(
            f"Starting OCI HPC Worker Manager\n"
            f"  Scheduler: {config.worker_manager_config.scheduler_address}\n"
            f"  Compartment: {config.container_instance_config.compartment_id}\n"
            f"  Region: {config.container_instance_config.oci_region}\n"
            f"  Object Storage: oci://{config.object_storage_bucket}/{config.object_storage_prefix}\n"
            f"  Container Image: {config.container_instance_config.container_image}\n"
            f"  Max Concurrent Jobs: {config.base_concurrency}\n"
            f"  Job Timeout: {config.job_timeout_seconds}s"
        )
        provisioner = OCIHPCWorkerProvisioner(config)
        runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_oci_hpc",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=-1,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            children_bind_address=config.worker_manager_config.children_bind_address,
            io_threads=config.worker_config.io_threads,
            workers_per_provisioner_unit=config.base_concurrency,
        )
        runner.run()
