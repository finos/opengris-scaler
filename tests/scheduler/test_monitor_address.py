import socket
import unittest
from typing import Tuple

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig
from scaler.scheduler.scheduler import DEFAULT_MONITOR_PORT_OFFSET
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name

HOST = "127.0.0.1"


def _hold_default_monitor_port() -> Tuple[int, socket.socket]:
    """Returns a scheduler port whose default monitor port this process listens on."""
    while True:
        scheduler_port = get_available_tcp_port(HOST)
        holder = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        try:
            holder.bind((HOST, scheduler_port + DEFAULT_MONITOR_PORT_OFFSET))
        except OSError:
            holder.close()
            continue

        holder.listen()
        return scheduler_port, holder


class TestDefaultMonitorAddress(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_scheduler_starts_when_the_default_monitor_port_is_taken(self) -> None:
        scheduler_port, holder = _hold_default_monitor_port()
        self.addCleanup(holder.close)

        object_storage_address = AddressConfig.from_string(f"tcp://{HOST}:{get_available_tcp_port(HOST)}")
        object_storage = ObjectStorageServerProcess(
            bind_address=object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        self.addCleanup(object_storage.join)
        self.addCleanup(object_storage.terminate)
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string(f"tcp://{HOST}:{scheduler_port}"),
            object_storage_address=object_storage_address,
            advertised_object_storage_address=None,
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
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()
        self.addCleanup(scheduler.join)
        self.addCleanup(scheduler.terminate)

        scheduler.wait_until_ready()
