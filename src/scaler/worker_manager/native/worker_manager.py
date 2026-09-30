from __future__ import annotations

import logging
from typing import Set

from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.worker.worker import Worker
from scaler.worker_manager.local_process import (
    LOCAL_PROCESS_POLL_INTERVAL_SECONDS,
    local_children_address,
    poll_local_processes,
    stop_local_process,
)
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.runner import WorkerManagerRunner

logger = logging.getLogger(__name__)


class NativeWorkerProvisioner(UnitProvisioner):
    def __init__(self, config: NativeWorkerManagerConfig, children_address: AddressConfig) -> None:
        self._children_address = children_address
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()
        self._io_threads = config.worker_config.io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.worker_config.event_loop
        self._preload = config.worker_config.preload
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._security_config = config.security

        self._worker_prefix = config.worker_type

    def _create_worker(self, unit_id: str) -> Worker:
        return Worker(
            name=f"{self._worker_prefix}|{unit_id}",
            address=self._worker_scheduler_address,
            object_storage_address=self._object_storage_address,
            preload=self._preload,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            task_timeout_seconds=self._task_timeout_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            event_loop=self._event_loop,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
            worker_manager_id=self._worker_manager_id,
            worker_manager_address=self._children_address,
            unit_id=unit_id,
            security_config=self._security_config,
        )

    async def create_unit(self, unit_id: str) -> UnitHandle:
        worker = self._create_worker(unit_id)
        worker.start()
        logger.info(f"started native worker {worker.identity!r}")
        return worker

    async def destroy_unit(self, handle: UnitHandle) -> None:
        assert isinstance(handle, Worker)
        await stop_local_process(handle)
        logger.info(f"stopped native worker {handle.identity!r}")

    async def poll_units(self, handles: Set[UnitHandle]) -> Set[UnitHandle]:
        return set(poll_local_processes({handle for handle in handles if isinstance(handle, Worker)}))

    def max_units(self) -> int:
        return self._max_task_concurrency

    def task_concurrency_per_unit(self) -> int:
        return 1

    def poll_interval_seconds(self) -> int:
        return LOCAL_PROCESS_POLL_INTERVAL_SECONDS


class NativeWorkerManager:
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._config = config

    @property
    def config(self) -> NativeWorkerManagerConfig:
        return self._config

    def run(self) -> None:
        children_address = local_children_address(self._config.worker_manager_config)
        provisioner = NativeWorkerProvisioner(self._config, children_address)

        runner = WorkerManagerRunner(
            name="worker_manager_native",
            worker_manager_config=self._config.worker_manager_config,
            heartbeat_interval_seconds=self._config.worker_config.heartbeat_interval_seconds,
            capabilities=self._config.worker_config.per_worker_capabilities.capabilities,
            provisioner=provisioner,
            children_address=children_address,
            io_threads=self._config.worker_config.io_threads,
            security_config=self._config.security,
        )
        runner.run()
