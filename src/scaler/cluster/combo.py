import logging
import multiprocessing
import sys
from typing import Dict, List, Optional, Tuple

import psutil

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.security import SecurityConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.constants import CLUSTER_SHUTDOWN_TIMEOUT_SECONDS
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
    DEFAULT_WORKER_MANAGER_DRAIN_TIMEOUT_SECONDS,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
    DRAIN_TIMEOUT_NONE,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.config.types.worker import WorkerCapabilities
from scaler.io.mixins import ConnectorRemoteType, NetworkBackend, SyncConnector
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.io.ymq import YMQException
from scaler.protocol.capnp import UnitShutdown
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.network_util import OS_PICKED_PORT
from scaler.utility.process_bootstrap import bootstrap_process
from scaler.worker_manager.native.worker_manager import NativeWorkerManager

logger = logging.getLogger(__name__)


def _run_native_worker_manager(worker_manager: NativeWorkerManager) -> None:
    # This process is spawned directly, so it must bootstrap its own logging/faulthandler.
    logging_config = worker_manager.config.logging_config
    bootstrap_process(
        logging_config.paths, logging_config.config_file, logging_config.level, process_name="worker_manager_native"
    )
    worker_manager.run()


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
        drain_timeout_seconds: int = DEFAULT_WORKER_MANAGER_DRAIN_TIMEOUT_SECONDS,
        protected: bool = True,
        scaler_policy: PolicyConfig = PolicyConfig(policy_content="allocate=even_load; scaling=static"),
        event_loop: str = "builtin",
        logging_paths: Tuple[str, ...] = DEFAULT_LOGGING_PATHS,
        logging_level: str = DEFAULT_LOGGING_LEVEL,
        logging_config_file: Optional[str] = None,
        worker_manager_id: str = "combo",
        security_config: Optional[SecurityConfig] = None,
    ):
        self._shutdown_called = False
        self._drain_timeout_seconds = drain_timeout_seconds
        self._security_config = security_config if security_config is not None else SecurityConfig()

        # port 0 lets the OS pick each free port at bind time; the processes report back the ports they got
        if address is None:
            self._address = AddressConfig(SocketType.tcp, "127.0.0.1", OS_PICKED_PORT)
        else:
            self._address = AddressConfig.from_string(address)

        if object_storage_address is None:
            self._object_storage_address = AddressConfig(self._address.type, self._address.host, OS_PICKED_PORT)
        else:
            self._object_storage_address = AddressConfig.from_string(object_storage_address)

        if monitor_address is None:
            self._monitor_address = None
        else:
            self._monitor_address = AddressConfig.from_string(monitor_address)

        self._object_storage = ObjectStorageServerProcess(
            bind_address=self._object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=logging_paths,
            logging_level=logging_level,
            logging_config_file=logging_config_file,
            security_config=self._security_config,
        )
        self._object_storage.start()

        # shutdown() skips what is still None, so each process is assigned only once start() returns
        self._scheduler: Optional[SchedulerProcess] = None
        self._worker_manager_process: Optional[multiprocessing.process.BaseProcess] = None
        self._network_backend: Optional[NetworkBackend] = None
        self._connector_scheduler: Optional[SyncConnector] = None
        try:
            # object storage should be ready before starting the cluster
            self._object_storage_address = self._object_storage.wait_until_ready()

            scheduler = SchedulerProcess(
                bind_address=self._address,
                object_storage_address=self._object_storage_address,
                advertised_object_storage_address=None,
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
                security_config=self._security_config,
            )

            scheduler.start()
            self._scheduler = scheduler
            # the workers and a client connecting next need the address the scheduler bound
            self._address = self._scheduler.wait_until_ready().bind_address

            # shutdown() asks the scheduler to drain the cluster over this connection
            self._network_backend = get_network_backend_from_env()
            self._connector_scheduler = self._network_backend.create_sync_connector(
                identity=generate_identity_from_name(self.__class__.__name__),
                connector_remote_type=ConnectorRemoteType.Binder,
                address=self._address,
                security_config=self._security_config,
            )

            self._worker_manager = NativeWorkerManager(
                NativeWorkerManagerConfig(
                    worker_manager_config=WorkerManagerConfig(
                        scheduler_address=self._address,
                        worker_manager_id=WorkerManagerID.from_string(worker_manager_id),
                        object_storage_address=self._object_storage_address,
                        max_task_concurrency=n_workers,
                        drain_timeout_seconds=drain_timeout_seconds,
                    ),
                    worker_config=WorkerConfig(
                        per_worker_capabilities=WorkerCapabilities(per_worker_capabilities or {}),
                        per_worker_task_queue_size=per_worker_task_queue_size,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                        task_timeout_seconds=task_timeout_seconds,
                        death_timeout_seconds=death_timeout_seconds,
                        garbage_collect_interval_seconds=garbage_collect_interval_seconds,
                        trim_memory_threshold_bytes=trim_memory_threshold_bytes,
                        hard_processor_suspend=hard_processor_suspend,
                        io_threads=worker_io_threads,
                        event_loop=event_loop,
                    ),
                    logging_config=LoggingConfig(
                        paths=logging_paths, config_file=logging_config_file, level=logging_level
                    ),
                    security=self._security_config,
                )
            )

            worker_manager_process = multiprocessing.get_context("spawn").Process(
                target=_run_native_worker_manager, args=(self._worker_manager,)
            )
            worker_manager_process.start()
            self._worker_manager_process = worker_manager_process
        except BaseException:
            self.shutdown()
            raise

        logger.info(f"{self.__get_prefix()} started")

    def __del__(self):
        if not self._shutdown_called and not sys.is_finalizing():
            self.shutdown()

    def shutdown(self):
        self._shutdown_called = True

        logger.info(f"{self.__get_prefix()} shutdown")

        # The scheduler drains the worker manager, which drains its workers, and quits last.
        if self._scheduler is not None and self._scheduler.is_alive() and self.__request_shutdown():
            shutdown_timeout_seconds = self.__shutdown_timeout_seconds()
            self._scheduler.join(timeout=shutdown_timeout_seconds)
            if self._worker_manager_process is not None:
                self._worker_manager_process.join(timeout=shutdown_timeout_seconds)

        self._stop_worker_manager()
        self._stop_scheduler()
        self.__close_scheduler_connection()

        # object storage should terminate after the cluster and scheduler.
        self._object_storage.terminate()
        self._object_storage.join()

    def __shutdown_timeout_seconds(self) -> Optional[float]:
        """How long each process may take to exit once asked to shut down: None waits for a drain with no deadline."""
        if self._drain_timeout_seconds == DRAIN_TIMEOUT_NONE:
            return None
        return self._drain_timeout_seconds + CLUSTER_SHUTDOWN_TIMEOUT_SECONDS

    def __request_shutdown(self) -> bool:
        """Ask the scheduler to drain the cluster. False if it cannot be reached, so its processes are terminated."""
        if self._connector_scheduler is None:
            return False

        try:
            self._connector_scheduler.send(UnitShutdown(force=False))
        except YMQException as e:
            logger.warning(f"{self.__get_prefix()} cannot reach the scheduler, terminating the cluster: {e!r}")
            return False
        return True

    def __close_scheduler_connection(self) -> None:
        if self._connector_scheduler is not None:
            self._connector_scheduler.destroy()
            self._connector_scheduler = None
        if self._network_backend is not None:
            self._network_backend.destroy()
            self._network_backend = None

    def _stop_worker_manager(self) -> None:
        if self._worker_manager_process is None:
            return

        if self._worker_manager_process.is_alive():
            logger.warning(f"{self.__get_prefix()} worker manager outlived the shutdown, terminating it")

            # On Windows terminate() is TerminateProcess: the manager's children are orphaned, so terminate them too.
            descendants: List[psutil.Process] = []
            if sys.platform == "win32":
                try:
                    descendants = psutil.Process(self._worker_manager_process.pid).children(recursive=True)
                except psutil.NoSuchProcess:
                    descendants = []

            self._worker_manager_process.terminate()

            for proc in descendants:
                try:
                    proc.terminate()
                except psutil.NoSuchProcess:
                    pass
        self._worker_manager_process.join()

    def _stop_scheduler(self) -> None:
        if self._scheduler is None:
            return

        if self._scheduler.is_alive():
            logger.warning(f"{self.__get_prefix()} scheduler outlived the shutdown, terminating it")
            self._scheduler.terminate()
        self._scheduler.join()

    def get_address(self) -> str:
        return repr(self._address)

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"
