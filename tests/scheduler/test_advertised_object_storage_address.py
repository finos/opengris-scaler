import multiprocessing
import sys
import unittest

from scaler import Client
from scaler.cluster.combo import _run_native_worker_manager
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_LOGGING_PATHS,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.utility import logging_test_name

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

# a client or worker dialing an unreachable object storage address never gets the result
TASK_TIMEOUT_SECONDS = 30


class TestAdvertisedObjectStorageAddress(unittest.TestCase):
    @override
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def _start(self, process: multiprocessing.process.BaseProcess) -> None:
        self.addCleanup(process.join)
        self.addCleanup(process.terminate)
        process.start()

    def test_an_advertised_port_zero_takes_the_object_storage_port(self) -> None:
        """Clients and workers dial the object storage port when the advertised address names port 0."""
        object_storage = ObjectStorageServerProcess(
            bind_address=AddressConfig.from_string("tcp://127.0.0.1:0"),
            identity="ObjectStorageServer",
            logging_paths=DEFAULT_LOGGING_PATHS,
            logging_config_file=None,
            logging_level="INFO",
        )
        self._start(object_storage)
        object_storage_address = object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string("tcp://127.0.0.1:0"),
            object_storage_address=object_storage_address,
            advertised_object_storage_address=AddressConfig.from_string("tcp://127.0.0.1:0"),
            monitor_address=None,
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            policy=PolicyConfig(policy_content="allocate=even_load; scaling=static"),
            event_loop="builtin",
            logging_paths=DEFAULT_LOGGING_PATHS,
            logging_config_file=None,
            logging_level="INFO",
        )
        self._start(scheduler)
        scheduler_address = scheduler.wait_until_ready().bind_address

        worker_manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=scheduler_address,
                    worker_manager_id=WorkerManagerID.from_string("advertised"),
                    max_task_concurrency=1,
                ),
                worker_config=WorkerConfig(),
                logging_config=LoggingConfig(),
            )
        )
        self._start(
            multiprocessing.get_context("spawn").Process(target=_run_native_worker_manager, args=(worker_manager,))
        )

        with Client(address=repr(scheduler_address)) as client:
            self.assertEqual(client.submit(abs, -3).result(timeout=TASK_TIMEOUT_SECONDS), 3)
