import logging
import os
import signal
from typing import Dict, List, Tuple

from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.protocol.capnp import WorkerManagerCommandResponse
from scaler.utility.identifiers import WorkerID
from scaler.worker_manager_adapter.mixins import WorkerProvisioner
from scaler.worker_manager_adapter.symphony.worker import create_symphony_worker
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess

Status = WorkerManagerCommandResponse.Status


class SymphonyWorkerProvisioner(WorkerProvisioner):
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

        self._workers: Dict[WorkerID, WorkerProcess] = {}

    async def start_worker(self) -> Tuple[List[bytes], Status]:
        if self._max_task_concurrency != -1 and len(self._workers) >= self._max_task_concurrency:
            return [], Status.tooManyWorkers

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
        )

        worker.start()
        self._workers[worker.identity] = worker
        return [bytes(worker.identity)], Status.success

    async def shutdown_workers(self, worker_ids: List[bytes]) -> Tuple[List[bytes], Status]:
        if not worker_ids:
            return [], Status.workerNotFound

        for wid_bytes in worker_ids:
            wid = WorkerID(wid_bytes)
            if wid not in self._workers:
                logging.warning(f"Worker with ID {wid!r} does not exist.")
                return [], Status.workerNotFound

        for wid_bytes in worker_ids:
            wid = WorkerID(wid_bytes)
            worker = self._workers.pop(wid)
            os.kill(worker.pid, signal.SIGINT)
            worker.join()

        return list(worker_ids), Status.success


class SymphonyWorkerManager:
    def __init__(self, config: SymphonyWorkerManagerConfig) -> None:
        provisioner = SymphonyWorkerProvisioner(config)
        self._runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_symphony",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_task_concurrency=config.worker_manager_config.max_task_concurrency,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=config.worker_config.io_threads,
        )

    def run(self) -> None:
        self._runner.run()
