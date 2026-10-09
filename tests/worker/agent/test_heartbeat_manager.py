import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.heartbeat_manager import VanillaHeartbeatManager
from scaler.worker.agent.mixins import ProcessorManager, TaskManager, TimeoutManager
from tests.utility.utility import logging_test_name


class TestVanillaHeartbeatManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.heartbeat_manager = VanillaHeartbeatManager(
            object_storage_address=AddressConfig.from_string("tcp://127.0.0.1:2346"),
            capabilities={},
            task_queue_size=10,
            worker_manager_id=b"test_manager",
        )
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_manager = AsyncMock(spec=AsyncConnector)
        task_manager = MagicMock(spec=TaskManager)
        task_manager.get_queued_size.return_value = 0
        task_manager.is_draining.return_value = False
        processor_manager = MagicMock(spec=ProcessorManager)
        processor_manager.processors.return_value = []
        processor_manager.num_suspended_processors.return_value = 0
        processor_manager.can_accept_task.return_value = True
        self.heartbeat_manager.register(
            self.connector_external,
            self.connector_manager,
            AsyncMock(spec=AsyncObjectStorageConnector),
            task_manager,
            MagicMock(spec=TimeoutManager),
            processor_manager,
        )

    async def test_the_manager_gets_a_heartbeat_while_the_scheduler_has_not_echoed(self) -> None:
        """The manager judges liveness by these heartbeats, so a silent scheduler must not silence them."""
        await self.heartbeat_manager.routine()
        await self.heartbeat_manager.routine()

        self.assertEqual(self.connector_manager.send.await_count, 2)
        self.assertIsInstance(self.connector_manager.send.call_args[0][0], WorkerHeartbeat)
        self.assertEqual(self.connector_external.send.await_count, 1, "the scheduler still waits for its echo")

    async def test_the_scheduler_gets_the_next_heartbeat_after_its_echo(self) -> None:
        await self.heartbeat_manager.routine()
        await self.heartbeat_manager.on_heartbeat_echo(MagicMock(spec=WorkerHeartbeatEcho))
        await self.heartbeat_manager.routine()

        self.assertEqual(self.connector_external.send.await_count, 2)


if __name__ == "__main__":
    unittest.main()
