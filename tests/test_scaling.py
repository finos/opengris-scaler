import os
import signal
import time
import unittest
from multiprocessing import Process

from aiohttp import web

from scaler import Client
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.scheduler.controllers.scaling_policies.types import ScalingControllerStrategy
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_adapter.native import NativeWorkerAdapter
from tests.utility import logging_test_name


def _run_native_worker_adapter(address: str, webhook_port: int) -> None:
    """Construct a NativeWorkerAdapter and run its aiohttp app. Runs in a separate process."""
    native_config = NativeWorkerAdapterConfig(
        scheduler_address=ZMQConfig.from_string(address), worker_task_queue_size=10, max_workers=4
    )
    adapter = NativeWorkerAdapter(config=native_config)

    app = adapter.create_app()
    web.run_app(app, host="127.0.0.1", port=webhook_port)


class TestScaling(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.object_storage_config = ObjectStorageConfig("127.0.0.1", get_available_tcp_port())
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

        scheduler_config = SchedulerConfig(
            scheduler_address=ZMQConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_config,
            monitor_address=None,
            scaling_controller_strategy=ScalingControllerStrategy.VANILLA,
            adapter_webhook_urls=(f"http://127.0.0.1:{self.webhook_port}",),
            protected=False,
        )
        scheduler = SchedulerProcess(config=scheduler_config)
        scheduler.start()

        webhook_server = Process(target=_run_native_worker_adapter, args=(self.scheduler_address, self.webhook_port))
        webhook_server.start()

        with Client(self.scheduler_address) as client:
            client.map(time.sleep, [(0.1,) for _ in range(100)])

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        os.kill(object_storage.pid, signal.SIGKILL)
        object_storage.join()

        os.kill(webhook_server.pid, signal.SIGINT)
        webhook_server.join()
