from __future__ import annotations

import logging

from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.worker_manager.local_process import (
    LOCAL_PROCESS_STARTUP_TIMEOUT_SECONDS,
    local_children_address,
    stop_local_process,
)
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.proxy.symphony.worker import create_symphony_worker
from scaler.worker_manager.proxy.worker_process import WorkerProcess
from scaler.worker_manager.runner import WorkerManagerRunner

logger = logging.getLogger(__name__)


class SymphonyWorkerProvisioner(UnitProvisioner):
    def __init__(self, config: SymphonyWorkerManagerConfig, children_address: AddressConfig) -> None:
        self._children_address = children_address
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._service_name = config.service_name
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_config.io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._event_loop = config.worker_config.event_loop
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()

    async def create_unit(self, unit_id: str) -> UnitHandle:
        worker = create_symphony_worker(
            address=self._worker_scheduler_address,
            object_storage_address=self._object_storage_address,
            service_name=self._service_name,
            capabilities=self._capabilities,
            base_concurrency=self._max_task_concurrency,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            task_queue_size=self._task_queue_size,
            io_threads=self._io_threads,
            event_loop=self._event_loop,
            worker_manager_id=self._worker_manager_id,
            worker_manager_address=self._children_address,
            unit_id=unit_id,
        )
        worker.start()
        logger.info(f"started Symphony worker {worker.identity!r}")
        return worker

    async def destroy_unit(self, handle: UnitHandle) -> None:
        assert isinstance(handle, WorkerProcess)
        await stop_local_process(handle)
        logger.info(f"stopped Symphony worker process {handle.name!r}")

    def max_units(self) -> int:
        return self._max_task_concurrency

    def task_concurrency_per_unit(self) -> int:
        return 1

    def startup_timeout_seconds(self) -> int:
        return LOCAL_PROCESS_STARTUP_TIMEOUT_SECONDS


class SymphonyWorkerManager:
    def __init__(self, config: SymphonyWorkerManagerConfig) -> None:
        children_address = local_children_address(config.worker_manager_config)
        provisioner = SymphonyWorkerProvisioner(config, children_address)
        self._runner = WorkerManagerRunner(
            name="worker_manager_symphony",
            worker_manager_config=config.worker_manager_config,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            provisioner=provisioner,
            children_address=children_address,
            io_threads=config.worker_config.io_threads,
        )

    def run(self) -> None:
        self._runner.run()
