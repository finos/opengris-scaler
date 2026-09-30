from __future__ import annotations

import logging
from typing import Set

from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.worker_manager.local_process import (
    LOCAL_PROCESS_POLL_INTERVAL_SECONDS,
    local_children_address,
    poll_local_processes,
    stop_local_process,
)
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.proxy.oci.worker import create_oci_worker
from scaler.worker_manager.proxy.worker_process import WorkerProcess
from scaler.worker_manager.runner import WorkerManagerRunner

logger = logging.getLogger(__name__)


class OCIJobsWorkerProvisioner(UnitProvisioner):
    def __init__(self, config: OCIHPCWorkerManagerConfig, children_address: AddressConfig) -> None:
        self._children_address = children_address
        self._config = config
        self._base_concurrency = config.base_concurrency
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities

    async def create_unit(self, unit_id: str) -> UnitHandle:
        config = self._config
        container_instance_config = config.container_instance_config
        worker = create_oci_worker(
            name=f"oci-hpc-{unit_id}",
            address=config.worker_manager_config.effective_worker_scheduler_address,
            object_storage_address=config.worker_manager_config.object_storage_address,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_manager_address=self._children_address,
            unit_id=unit_id,
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
        worker.start()
        logger.info(f"started OCI worker process {worker.name!r}")
        return worker

    async def destroy_unit(self, handle: UnitHandle) -> None:
        assert isinstance(handle, WorkerProcess)
        await stop_local_process(handle)
        logger.info(f"stopped OCI worker process {handle.name!r}")

    async def poll_units(self, handles: Set[UnitHandle]) -> Set[UnitHandle]:
        return set(poll_local_processes({handle for handle in handles if isinstance(handle, WorkerProcess)}))

    def max_units(self) -> int:
        return -1

    def task_concurrency_per_unit(self) -> int:
        return self._base_concurrency

    def poll_interval_seconds(self) -> int:
        return LOCAL_PROCESS_POLL_INTERVAL_SECONDS


class OCIJobsWorkerManager:
    def __init__(self, config: OCIHPCWorkerManagerConfig) -> None:
        self._config = config

    def run(self) -> None:
        config = self._config
        logger.info(
            f"Starting OCI Worker Manager\n"
            f"  Scheduler: {config.worker_manager_config.scheduler_address}\n"
            f"  Compartment: {config.container_instance_config.compartment_id}\n"
            f"  Region: {config.container_instance_config.oci_region}\n"
            f"  Object Storage: oci://{config.object_storage_bucket}/{config.object_storage_prefix}\n"
            f"  Container Image: {config.container_instance_config.container_image}\n"
            f"  Max Concurrent Jobs: {config.base_concurrency}\n"
            f"  Job Timeout: {config.job_timeout_seconds}s"
        )
        children_address = local_children_address(config.worker_manager_config)
        provisioner = OCIJobsWorkerProvisioner(config, children_address)
        runner = WorkerManagerRunner(
            name="worker_manager_oci_hpc",
            worker_manager_config=config.worker_manager_config,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            provisioner=provisioner,
            children_address=children_address,
            io_threads=config.worker_config.io_threads,
        )
        runner.run()
