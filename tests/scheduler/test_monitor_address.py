import socket
import unittest

from scaler import SchedulerClusterCombo
from scaler.config.common.security import SecurityConfig
from scaler.config.constants import DEFAULT_MONITOR_PORT_OFFSET
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.network_backends import YMQNetworkBackend
from scaler.scheduler.scheduler import bind_default_monitor_address
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import OS_PICKED_PORT
from tests.utility.utility import logging_test_name

HOST = "127.0.0.1"


class TestDefaultMonitorAddress(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_a_taken_default_monitor_port_falls_back_to_an_os_picked_port(self) -> None:
        """When scheduler port + 2 is taken, the monitor binds a port the OS picks instead of failing."""
        holder = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        self.addCleanup(holder.close)
        holder.bind((HOST, OS_PICKED_PORT))
        holder.listen()
        taken_port = holder.getsockname()[1]

        backend = YMQNetworkBackend(num_threads=1)
        self.addCleanup(backend.destroy)
        publisher = backend.create_async_publisher(identity=b"monitor")
        self.addCleanup(publisher.destroy)

        # only the monitor port is bound here, so the scheduler port itself need not be free
        scheduler_address = AddressConfig(SocketType.tcp, HOST, taken_port - DEFAULT_MONITOR_PORT_OFFSET)
        await bind_default_monitor_address(publisher, scheduler_address, SecurityConfig())

        assert publisher.address is not None
        self.assertNotIn(publisher.address.port, {taken_port, OS_PICKED_PORT})

    def test_a_taken_default_monitor_port_fails_an_explicit_scheduler_port(self) -> None:
        """With an explicit scheduler port, a taken port + 2 fails startup, since monitors connect to port + 2."""
        holder = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        self.addCleanup(holder.close)
        holder.bind((HOST, OS_PICKED_PORT))
        holder.listen()
        scheduler_port = holder.getsockname()[1] - DEFAULT_MONITOR_PORT_OFFSET

        with self.assertRaises(RuntimeError):
            combo = SchedulerClusterCombo(address=f"tcp://{HOST}:{scheduler_port}", n_workers=1)
            combo.shutdown()
