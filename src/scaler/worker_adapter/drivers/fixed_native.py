import uuid
from typing import Dict

from scaler.config.section.fixed_native_worker_adapter import FixedNativeWorkerAdapterConfig
from scaler.utility.identifiers import WorkerID
from scaler.worker.worker import Worker
from scaler.worker_adapter.drivers.common import WorkerGroupNotFoundError


class FixedNativeWorkerAdapter:
    def __init__(self, config: FixedNativeWorkerAdapterConfig):
        self._address = config.worker_adapter_config.scheduler_address
        self._object_storage_address = config.worker_adapter_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_workers = config.worker_adapter_config.max_workers
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
        self._preload = config.preload

        self._workers: Dict[WorkerID, Worker] = {}

    def _spawn_worker(self):
        worker = Worker(
            name=f"FIX|{uuid.uuid4().hex}",
            address=self._address,
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
        )
        worker.start()
        self._workers[worker.identity] = worker

    def _shutdown_worker(self, worker_id: WorkerID):
        if worker_id not in self._workers:
            raise WorkerGroupNotFoundError(f"Worker with ID {worker_id!r} not found.")

        worker = self._workers[worker_id]
        worker.terminate()
        worker.join()
        del self._workers[worker_id]

    def start(self):
        for _ in range(self._max_workers):
            self._spawn_worker()

    def shutdown(self):
        for worker_id in list(self._workers.keys()):
            self._shutdown_worker(worker_id)

    def join(self):
        """Wait for all workers to finish."""

        # this specific adapter cannot dynamically spawn workers
        # therefore we just wait for all existing workers to finish
        for worker in self._workers.values():
            worker.join()
