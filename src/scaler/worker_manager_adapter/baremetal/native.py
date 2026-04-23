import logging
import os
import signal
import uuid
from typing import Dict, List, Tuple

from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.protocol.capnp import WorkerManagerCommandResponse
from scaler.utility.identifiers import WorkerID
from scaler.worker.worker import Worker
from scaler.worker_manager_adapter.mixins import WorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

Status = WorkerManagerCommandResponse.Status


class NativeWorkerProvisioner(WorkerProvisioner):
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
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
        self._logging_config_file = config.logging_config.config_file

        if config.worker_type is not None:
            self._worker_prefix = config.worker_type
        elif config.mode == NativeWorkerManagerMode.FIXED:
            self._worker_prefix = "FIX"
        elif config.mode == NativeWorkerManagerMode.DYNAMIC:
            self._worker_prefix = "NAT"
        else:
            raise ValueError(f"worker_type is not set and mode is unrecognised: {config.mode!r}")

        self._workers: Dict[WorkerID, Worker] = {}

    def _create_worker(self) -> Worker:
        return Worker(
            name=f"{self._worker_prefix}|{uuid.uuid4().hex}",
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
        )

    def run_fixed(self) -> None:
        for _ in range(self._max_task_concurrency):
            worker = self._create_worker()
            worker.start()
            self._workers[worker.identity] = worker

        def _on_signal(sig: int, frame: object) -> None:
            logging.info("NativeWorkerProvisioner (FIXED): received signal %d, terminating workers", sig)
            for worker in self._workers.values():
                if worker.is_alive():
                    worker.terminate()

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

        for worker in self._workers.values():
            worker.join()

    async def start_worker(self) -> Tuple[List[bytes], Status]:
        if self._max_task_concurrency != -1 and len(self._workers) >= self._max_task_concurrency:
            return [], Status.tooManyWorkers

        worker = self._create_worker()
        worker.start()
        self._workers[worker.identity] = worker
        logging.info(f"Started native worker {worker.identity!r}")
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


class NativeWorkerManager:
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._config = config

    @property
    def config(self) -> NativeWorkerManagerConfig:
        return self._config

    def run(self) -> None:
        provisioner = NativeWorkerProvisioner(self._config)

        if self._config.mode == NativeWorkerManagerMode.FIXED:
            provisioner.run_fixed()
            return

        runner = WorkerManagerRunner(
            address=self._config.worker_manager_config.scheduler_address,
            name="worker_manager_native",
            heartbeat_interval_seconds=self._config.worker_config.heartbeat_interval_seconds,
            capabilities=self._config.worker_config.per_worker_capabilities.capabilities,
            max_task_concurrency=self._config.worker_manager_config.max_task_concurrency,
            worker_manager_id=self._config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=self._config.worker_config.io_threads,
        )
        runner.run()
