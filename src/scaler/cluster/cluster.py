import logging
import multiprocessing
import signal

from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.section.cluster import ClusterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.native import NativeWorkerAdapter, NativeWorkerAdapterConfig


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

        self._worker_adapter = NativeWorkerAdapter(
            NativeWorkerAdapterConfig(
                web_config=None,
                preload=config.preload,
                worker_adapter_config=WorkerAdapterConfig(
                    scheduler_address=config.scheduler_address,
                    object_storage_address=config.object_storage_address,
                    max_workers=len(config.worker_names),
                ),
                worker_config=config.worker_config,
                logging_config=config.logging_config,
                event_loop=config.event_loop,
                worker_io_threads=config.worker_io_threads,
            )
        )

    def run(self):
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        self.__register_signal()
        self.__start_workers_and_run_forever()

    def __destroy(self, *args):
        assert args is not None
        logging.info(f"{self.__get_prefix()} received signal, shutting down")
        self._worker_adapter.shutdown()

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__destroy)
        signal.signal(signal.SIGTERM, self.__destroy)

    def __start_workers_and_run_forever(self):
        logging.info(
            f"{self.__get_prefix()} starting {len(self._worker_names)} workers, heartbeat_interval_seconds="
            f"{self._heartbeat_interval_seconds}, task_timeout_seconds={self._task_timeout_seconds}"
        )

        for _ in self._worker_names:
            group_id = self._worker_adapter.start_worker_group()
            logging.info(f"{self.__get_prefix()} started worker group {group_id!r}")

        self._worker_adapter.join()
        logging.info(f"{self.__get_prefix()} shutdown")

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"
