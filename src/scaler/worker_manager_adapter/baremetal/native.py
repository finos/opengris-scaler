import logging
import multiprocessing
import signal
import uuid
from multiprocessing.synchronize import Event as MultiprocessingEvent
from typing import Dict, List, Tuple

from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.protocol.capnp import WorkerManagerCommandResponse
from scaler.utility.identifiers import WorkerID
from scaler.worker.worker import Worker
from scaler.worker_manager_adapter.mixins import ImperativeWorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

Status = WorkerManagerCommandResponse.Status


class NativeWorkerProvisioner(ImperativeWorkerProvisioner):
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
        # Per-worker graceful-shutdown channel. Replaces POSIX-only os.kill(pid, SIGINT)
        # so Windows works the same way and POSIX has one shutdown path.
        self._worker_shutdown_events: Dict[WorkerID, MultiprocessingEvent] = {}

    def _create_worker(self) -> Tuple[Worker, MultiprocessingEvent]:
        shutdown_event = multiprocessing.get_context("spawn").Event()
        worker = Worker(
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
            shutdown_event=shutdown_event,
        )
        return worker, shutdown_event

    def run_fixed(self) -> None:
        for _ in range(self._max_task_concurrency):
            worker, shutdown_event = self._create_worker()
            worker.start()
            self._workers[worker.identity] = worker
            self._worker_shutdown_events[worker.identity] = shutdown_event

        def _on_signal(sig: int, frame: object) -> None:
            logging.info("NativeWorkerProvisioner (FIXED): received signal %d, terminating workers", sig)
            for worker in self._workers.values():
                if worker.is_alive():
                    worker.terminate()

        # signal.signal works on Windows for SIGINT (Ctrl+C); SIGTERM is not delivered on
        # Windows, so combo.py shutdown there comes via Process.terminate() (TerminateProcess).
        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

        for worker in self._workers.values():
            worker.join()

    async def start_worker(self) -> Tuple[List[WorkerID], Status]:
        if self._max_task_concurrency != -1 and len(self._workers) >= self._max_task_concurrency:
            return [], Status.tooManyWorkers

        worker, shutdown_event = self._create_worker()
        worker.start()
        self._workers[worker.identity] = worker
        self._worker_shutdown_events[worker.identity] = shutdown_event
        logging.info(f"Started native worker {worker.identity!r}")
        return [worker.identity], Status.success

    async def shutdown_workers(self, worker_ids: List[WorkerID]) -> Tuple[List[WorkerID], Status]:
        if not worker_ids:
            return [], Status.workerNotFound

        for wid in worker_ids:
            if wid not in self._workers:
                logging.warning(f"Worker with ID {wid!r} does not exist.")
                return [], Status.workerNotFound

        for wid in worker_ids:
            worker = self._workers.pop(wid)
            self._worker_shutdown_events.pop(wid).set()
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
            max_provisioner_units=self._config.worker_manager_config.max_task_concurrency,
            worker_manager_id=self._config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=self._config.worker_config.io_threads,
        )
        runner.run()
