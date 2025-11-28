import os
import signal
import time
import unittest
from multiprocessing import Process

from aiohttp import web

from scaler import Client
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.config.types.zmq import ZMQConfig
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.scheduler.controllers.scaling_policies.types import ScalingControllerStrategy
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_adapter.native import NativeWorkerAdapter
from tests.utility.utility import logging_test_name


def _run_native_worker_adapter(scheduler_address: str, webhook_port: int) -> None:
    """Construct a NativeWorkerAdapter and run its aiohttp app. Runs in a separate process."""
    adapter = NativeWorkerAdapter(
        NativeWorkerAdapterConfig(
            web_config=WebConfig(),
            worker_adapter_config=WorkerAdapterConfig(
                scheduler_address=ZMQConfig.from_string(scheduler_address), object_storage_address=None, max_workers=4
            ),
            event_loop="builtin",
            worker_io_threads=DEFAULT_IO_THREADS,
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({}),
                per_worker_task_queue_size=10,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
                garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
            ),
            logging_config=LoggingConfig(paths=("/dev/stdout",), level="INFO", config_file=None),
        )
    )

    app = adapter.create_app()
    web.run_app(app, host="127.0.0.1", port=webhook_port)


class TestScaling(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.object_storage_config = ObjectStorageAddressConfig("127.0.0.1", get_available_tcp_port())
        self.webhook_port = get_available_tcp_port()

    def test_scaling_basic(self):
        object_storage = ObjectStorageServerProcess(
            object_storage_address=self.object_storage_config,
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            address=ZMQConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_config,
            monitor_address=None,
            scaling_controller_strategy=ScalingControllerStrategy.VANILLA,
            adapter_webhook_urls=(f"http://127.0.0.1:{self.webhook_port}",),
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            allocate_policy=AllocatePolicy.even,
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()

        webhook_server = Process(target=_run_native_worker_adapter, args=(self.scheduler_address, self.webhook_port))
        webhook_server.start()

        with Client(self.scheduler_address) as client:
            client.map(time.sleep, [(0.1,) for _ in range(100)])

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        os.kill(webhook_server.pid, signal.SIGINT)
        webhook_server.join()
