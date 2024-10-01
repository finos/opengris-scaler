import logging
import multiprocessing
import os
import signal
from typing import List, Optional, Tuple

from scaler.utility.logging.utility import setup_logger
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.worker import Worker


class Cluster(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(
        self,
        address: ZMQConfig,
        worker_io_threads: int,
        worker_names: List[str],
        heartbeat_interval_seconds: int,
        task_timeout_seconds: int,
        death_timeout_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        hard_processor_suspend: bool,
        event_loop: str,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
    ):
        multiprocessing.Process.__init__(self, name="WorkerMaster")

        self._address = address
        self._worker_io_threads = worker_io_threads
        self._worker_names = worker_names
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._task_timeout_seconds = task_timeout_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._hard_processor_suspend = hard_processor_suspend
        self._event_loop = event_loop

        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        self._workers: List[Worker] = []

    def run(self):
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        self.__register_signal()
        self.__start_workers_and_run_forever()

    def __destroy(self, *args):
        assert args is not None
        logging.info(f"{self.__get_prefix()} received signal, shutting down")
        for worker in self._workers:
            logging.info(f"{self.__get_prefix()} shutting down worker[{worker.pid}]")
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
                io_threads=self._worker_io_threads,
                heartbeat_interval_seconds=self._heartbeat_interval_seconds,
                garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
                trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
                task_timeout_seconds=self._task_timeout_seconds,
                death_timeout_seconds=self._death_timeout_seconds,
                hard_processor_suspend=self._hard_processor_suspend,
                logging_paths=self._logging_paths,
                logging_level=self._logging_level,
            )
            for name in self._worker_names
        ]

        for worker in self._workers:
            worker.start()

        for i, worker in enumerate(self._workers):
            logging.info(f"Worker[{worker.ident}] started")

        for worker in self._workers:
            worker.join()

        logging.info(f"{self.__get_prefix()} shutdown")

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"
