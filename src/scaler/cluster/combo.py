import logging
import socket
from typing import Dict, Optional, Tuple

from scaler.cluster.cluster import Cluster
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_LOGGING_PATHS,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.cluster import ClusterConfig
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.network_util import get_available_tcp_port


class SchedulerClusterCombo:
    def __init__(
        self,
        n_workers: int,
        address: Optional[str] = None,
        object_storage_address: Optional[str] = None,
        monitor_address: Optional[str] = None,
        per_worker_capabilities: Optional[Dict[str, int]] = None,
        worker_io_threads: int = DEFAULT_IO_THREADS,
        scheduler_io_threads: int = DEFAULT_IO_THREADS,
        max_number_of_tasks_waiting: int = DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        client_timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
        worker_timeout_seconds: int = DEFAULT_WORKER_TIMEOUT_SECONDS,
        object_retention_seconds: int = DEFAULT_OBJECT_RETENTION_SECONDS,
        task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS,
        death_timeout_seconds: int = DEFAULT_WORKER_DEATH_TIMEOUT,
        load_balance_seconds: int = DEFAULT_LOAD_BALANCE_SECONDS,
        load_balance_trigger_times: int = DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        garbage_collect_interval_seconds: int = DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        trim_memory_threshold_bytes: int = DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        per_worker_task_queue_size: int = DEFAULT_PER_WORKER_QUEUE_SIZE,
        hard_processor_suspend: bool = DEFAULT_HARD_PROCESSOR_SUSPEND,
        protected: bool = True,
        scaler_policy: PolicyConfig = PolicyConfig(),
        event_loop: str = "builtin",
        logging_paths: Tuple[str, ...] = DEFAULT_LOGGING_PATHS,
        logging_level: str = DEFAULT_LOGGING_LEVEL,
        logging_config_file: Optional[str] = None,
    ):
        self._shutdown_called = False

        if address is None:
            self._address = ZMQConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}")
        else:
            self._address = ZMQConfig.from_string(address)

        if object_storage_address is None:
            self._object_storage_address = ObjectStorageAddressConfig(self._address.host, get_available_tcp_port())
        else:
            self._object_storage_address = ObjectStorageAddressConfig.from_string(object_storage_address)

        if monitor_address is None:
            self._monitor_address = None
        else:
            self._monitor_address = ZMQConfig.from_string(monitor_address)

        self._object_storage = ObjectStorageServerProcess(
            object_storage_address=self._object_storage_address,
            logging_paths=logging_paths,
            logging_level=logging_level,
            logging_config_file=logging_config_file,
        )
        self._object_storage.start()
        self._object_storage.wait_until_ready()  # object storage should be ready before starting the cluster

        self._cluster = Cluster(
            config=ClusterConfig(
                scheduler_address=self._address,
                object_storage_address=self._object_storage_address,
                preload=None,
                worker_names=WorkerNames([f"{socket.gethostname().split('.')[0]}" for _ in range(n_workers)]),
                num_of_workers=n_workers,
                event_loop=event_loop,
                worker_io_threads=worker_io_threads,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities(per_worker_capabilities or {}),
                    per_worker_task_queue_size=per_worker_task_queue_size,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                    task_timeout_seconds=task_timeout_seconds,
                    death_timeout_seconds=death_timeout_seconds,
                    garbage_collect_interval_seconds=garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=trim_memory_threshold_bytes,
                    hard_processor_suspend=hard_processor_suspend,
                ),
                logging_config=LoggingConfig(paths=logging_paths, config_file=logging_config_file, level=logging_level),
            )
        )

        self._scheduler = SchedulerProcess(
            address=self._address,
            object_storage_address=self._object_storage_address,
            monitor_address=self._monitor_address,
            io_threads=scheduler_io_threads,
            max_number_of_tasks_waiting=max_number_of_tasks_waiting,
            client_timeout_seconds=client_timeout_seconds,
            worker_timeout_seconds=worker_timeout_seconds,
            object_retention_seconds=object_retention_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
            protected=protected,
            event_loop=event_loop,
            logging_paths=logging_paths,
            logging_config_file=logging_config_file,
            logging_level=logging_level,
            policy=scaler_policy,
        )

        self._cluster.start()
        self._scheduler.start()
        logging.info(f"{self.__get_prefix()} started")

    def __del__(self):
        if not self._shutdown_called:
            self.shutdown()

    def shutdown(self):
        self._shutdown_called = True

        logging.info(f"{self.__get_prefix()} shutdown")
        self._cluster.terminate()
        self._scheduler.terminate()
        self._cluster.join()
        self._scheduler.join()

        # object storage should terminate after the cluster and scheduler.
        self._object_storage.terminate()
        self._object_storage.join()

    def get_address(self) -> str:
        return self._address.to_address()

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"
