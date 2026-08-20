from __future__ import annotations

import logging
from typing import Set

from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.worker_manager_adapter.proxy_worker_pool import ProxyWorkerPool
from scaler.worker_manager_adapter.symphony.worker import create_symphony_worker
from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS, UnitProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess

logger = logging.getLogger(__name__)

SENTINEL_POLL_INTERVAL_SECONDS = 1.0


class SymphonyWorkerProvisioner(UnitProvisioner):
    """A unit is one local proxy process that submits tasks to an IBM Symphony service."""

    def __init__(self, config: SymphonyWorkerManagerConfig) -> None:
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

        self._pool = ProxyWorkerPool(self._build_worker, description="Symphony worker")

    def _build_worker(self) -> WorkerProcess:
        return create_symphony_worker(
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
        return max(1, self._max_task_concurrency)

    def poll_interval_seconds(self) -> float:
        return SENTINEL_POLL_INTERVAL_SECONDS


class SymphonyWorkerManager:
    def __init__(self, config: SymphonyWorkerManagerConfig) -> None:
        provisioner = SymphonyWorkerProvisioner(config)
        self._runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_symphony",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=config.worker_manager_config.max_task_concurrency,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            children_bind_address=config.worker_manager_config.children_bind_address,
            io_threads=config.worker_config.io_threads,
        )

    def run(self) -> None:
        self._runner.run()
