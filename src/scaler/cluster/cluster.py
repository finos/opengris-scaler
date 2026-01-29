import logging
import multiprocessing
import os
import signal
from typing import List

from scaler.config.section.cluster import ClusterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker.worker import Worker


class Cluster(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(self, config: ClusterConfig):
        multiprocessing.Process.__init__(self, name="WorkerMaster")

        self._address = config.scheduler_address
        self._object_storage_address = config.object_storage_address
        self._preload = config.preload
        self._worker_io_threads = config.worker_io_threads
        self._worker_names = config.worker_names.names
        self._per_worker_capabilities = config.worker_config.per_worker_capabilities.capabilities

        self._per_worker_task_queue_size = config.worker_config.per_worker_task_queue_size
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.event_loop

        self._logging_paths = config.logging_config.paths
        self._logging_config_file = config.logging_config.config_file
        self._logging_level = config.logging_config.level
        self._no_random_worker_ids = config.no_random_worker_ids

        self._workers: List[Worker] = []

    def run(self):
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        self.__register_signal()
        self.__start_workers_and_run_forever()

    def __destroy(self, *args):
        assert args is not None
        logging.info(f"{self.__get_prefix()} received signal, shutting down")
        for worker in self._workers:
            logging.info(f"{self.__get_prefix()} shutting down {worker.identity!r}")
            os.kill(worker.pid, signal.SIGINT)

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__destroy)
        signal.signal(signal.SIGTERM, self.__destroy)

    def __start_workers_and_run_forever(self):
        logging.info(
            f"{self.__get_prefix()} starting {len(self._worker_names)} workers, heartbeat_interval_seconds="
            f"{self._heartbeat_interval_seconds}, task_timeout_seconds={self._task_timeout_seconds}"
        )

        self._workers = [
            Worker(
                event_loop=self._event_loop,
                name=name,
                address=self._address,
                object_storage_address=self._object_storage_address,
                capabilities=self._per_worker_capabilities,
                preload=self._preload,
                io_threads=self._worker_io_threads,
                task_queue_size=self._per_worker_task_queue_size,
                heartbeat_interval_seconds=self._heartbeat_interval_seconds,
                garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
                trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
                task_timeout_seconds=self._task_timeout_seconds,
                death_timeout_seconds=self._death_timeout_seconds,
                hard_processor_suspend=self._hard_processor_suspend,
                logging_paths=self._logging_paths,
                logging_level=self._logging_level,
                no_random_worker_ids=self._no_random_worker_ids,
            )
            for name in self._worker_names
        ]

        for worker in self._workers:
            worker.start()

        for worker in self._workers:
            logging.info(f"{worker.identity!r} started")

        for worker in self._workers:
            worker.join()

        logging.info(f"{self.__get_prefix()} shutdown")

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"
