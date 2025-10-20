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
        self._cluster_config = config
        self._workers: List[Worker] = []

    def run(self):
        setup_logger(
            self._cluster_config.logging_paths,
            self._cluster_config.logging_config_file,
            self._cluster_config.logging_level,
        )
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
            f"{self.__get_prefix()} starting {self._cluster_config.num_of_workers} workers, heartbeat_interval_seconds="
            f"{self._cluster_config.heartbeat_interval_seconds},\
                task_timeout_seconds={self._cluster_config.task_timeout_seconds}"
        )

        self._workers = [
            Worker(
                event_loop=self._cluster_config.event_loop,
                name=name,
                address=self._cluster_config.scheduler_address,
                object_storage_address=self._cluster_config.object_storage_address,
                capabilities=self._cluster_config.per_worker_capabilities.capabilities,
                preload=self._cluster_config.preload,
                io_threads=self._cluster_config.worker_io_threads,
                task_queue_size=self._cluster_config.per_worker_task_queue_size,
                heartbeat_interval_seconds=self._cluster_config.heartbeat_interval_seconds,
                garbage_collect_interval_seconds=self._cluster_config.garbage_collect_interval_seconds,
                trim_memory_threshold_bytes=self._cluster_config.trim_memory_threshold_bytes,
                task_timeout_seconds=self._cluster_config.task_timeout_seconds,
                death_timeout_seconds=self._cluster_config.death_timeout_seconds,
                hard_processor_suspend=self._cluster_config.hard_processor_suspend,
                logging_paths=self._cluster_config.logging_paths,
                logging_level=self._cluster_config.logging_level,
            )
            for name in self._cluster_config.worker_names.names
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
