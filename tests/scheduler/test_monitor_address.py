import asyncio
import socket
import sys
import unittest
from typing import Optional
from unittest import mock

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.common.security import SecurityConfig
from scaler.config.constants import DEFAULT_MONITOR_PORT_OFFSET
from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.scheduler.scheduler import Scheduler, default_monitor_address
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import OS_PICKED_PORT
from tests.utility.utility import logging_test_name

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

HOST = "127.0.0.1"

# a scheduler that falls back to another monitor port starts serving, so the test stops waiting after this long
STARTUP_TIMEOUT_SECONDS = 10


class TestDefaultMonitorAddress(unittest.TestCase):
    @override
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_a_port_zero_scheduler_gets_a_port_zero_monitor(self) -> None:
        """A scheduler binding port 0 lets the OS pick its monitor port too."""
        bind_address = AddressConfig(SocketType.tcp, HOST, OS_PICKED_PORT)
        self.assertEqual(default_monitor_address(bind_address), bind_address)

    def test_a_taken_default_monitor_port_fails_an_explicit_scheduler_port(self) -> None:
        """With an explicit scheduler port, a taken port + 2 fails startup, since monitors connect to port + 2."""
        object_storage = ObjectStorageServerProcess(
            bind_address=AddressConfig(SocketType.tcp, HOST, OS_PICKED_PORT),
            identity="ObjectStorageServer",
            logging_paths=DEFAULT_LOGGING_PATHS,
            logging_config_file=None,
            logging_level="INFO",
        )
        self.addCleanup(object_storage.join)
        self.addCleanup(object_storage.terminate)
        object_storage.start()

        holder = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        self.addCleanup(holder.close)
        holder.bind((HOST, OS_PICKED_PORT))
        holder.listen()
        scheduler_port = holder.getsockname()[1] - DEFAULT_MONITOR_PORT_OFFSET

        scheduler = Scheduler(
            SchedulerConfig(
                bind_address=AddressConfig(SocketType.tcp, HOST, scheduler_port),
                object_storage_address=object_storage.wait_until_ready(),
            )
        )
        self.addCleanup(scheduler._connector_storage.destroy)
        self.addCleanup(scheduler._binder_monitor.destroy)
        self.addCleanup(scheduler._binder.destroy)

        # the scheduler binds port 0, which nothing else can hold, so only the monitor bind can fail
        bind_scheduler = scheduler._binder.bind

        async def bind_port_zero(address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
            await bind_scheduler(AddressConfig(SocketType.tcp, HOST, OS_PICKED_PORT), security_config=security_config)

        async def start_scheduler() -> None:
            await asyncio.wait_for(scheduler.get_loops(), timeout=STARTUP_TIMEOUT_SECONDS)

        with mock.patch.object(scheduler._binder, "bind", bind_port_zero):
            with self.assertRaisesRegex(Exception, "EADDRINUSE|in use"):
                asyncio.run(start_scheduler())
