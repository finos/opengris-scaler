import time
import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import Resource, WorkerDisconnectNotification, WorkerHeartbeat
from scaler.scheduler.controllers.mixins import ConfigController, PolicyController, TaskController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

_WORKER_ID = WorkerID(b"worker_aaa")
_MANAGER_ID = b"manager_bbb"
_TASK_ID = TaskID(b"0" * 16)


class TestVanillaWorkerControllerOnDisconnectNotification(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        config_controller = MagicMock(spec=ConfigController)
        self.policy_controller = MagicMock(spec=PolicyController)
        self.policy_controller.remove_worker.return_value = []

        self.controller = VanillaWorkerController(config_controller, self.policy_controller)

        self.binder = AsyncMock()
        self.binder_monitor = AsyncMock()
        self.task_controller = MagicMock(spec=TaskController)
        self.controller.register(self.binder, self.binder_monitor, self.task_controller)

        self.controller._worker_alive_since[_WORKER_ID] = (time.time(), MagicMock())
        self.controller._worker_to_manager[_WORKER_ID] = _MANAGER_ID
        self.controller._manager_to_workers[_MANAGER_ID] = {_WORKER_ID}

    async def test_on_disconnect_notification_removes_worker(self) -> None:
        await self.controller.on_disconnect_notification(_WORKER_ID, WorkerDisconnectNotification())
        self.assertNotIn(_WORKER_ID, self.controller._worker_alive_since)

    async def test_on_disconnect_notification_sends_no_reply(self) -> None:
        await self.controller.on_disconnect_notification(_WORKER_ID, WorkerDisconnectNotification())
        self.binder.send.assert_not_called()

    async def test_on_disconnect_notification_unknown_worker_is_safe(self) -> None:
        # WDN from a worker not in the registry (e.g. already timed out) must not crash, and must
        # leave the registered workers alone.
        unknown_id = WorkerID(b"unknown_worker")
        await self.controller.on_disconnect_notification(unknown_id, WorkerDisconnectNotification())
        self.assertIn(_WORKER_ID, self.controller._worker_alive_since)

    async def test_on_disconnect_notification_only_disconnects_the_sender(self) -> None:
        # The notification carries no worker name, so a peer cannot disconnect anyone but itself.
        other_id = WorkerID(b"worker_ccc")
        self.controller._worker_alive_since[other_id] = (time.time(), MagicMock())

        await self.controller.on_disconnect_notification(_WORKER_ID, WorkerDisconnectNotification())

        self.assertNotIn(_WORKER_ID, self.controller._worker_alive_since)
        self.assertIn(other_id, self.controller._worker_alive_since)

    async def test_on_disconnect_notification_re_dispatches_in_flight_tasks(self) -> None:
        self.policy_controller.remove_worker.return_value = [_TASK_ID]

        await self.controller.on_disconnect_notification(_WORKER_ID, WorkerDisconnectNotification())

        self.task_controller.on_worker_disconnect.assert_awaited_once_with(_TASK_ID, _WORKER_ID)


class TestVanillaWorkerControllerOnDrainingHeartbeat(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        config_controller = MagicMock(spec=ConfigController)
        config_controller.get_config.return_value = AddressConfig.from_string("tcp://127.0.0.1:1234")

        self.policy_controller = MagicMock(spec=PolicyController)
        self.policy_controller.add_worker.return_value = False
        self.policy_controller.mark_worker_draining.return_value = True
        self.policy_controller.evacuate_worker.return_value = [_TASK_ID]

        self.controller = VanillaWorkerController(config_controller, self.policy_controller)

        self.binder = AsyncMock()
        self.binder_monitor = AsyncMock()
        self.task_controller = AsyncMock(spec=TaskController)
        self.controller.register(self.binder, self.binder_monitor, self.task_controller)

    @staticmethod
    def _heartbeat(draining: bool) -> WorkerHeartbeat:
        return WorkerHeartbeat(
            agent=Resource(cpu=0, rss=0),
            rssFree=0,
            memLimit=0,
            queueSize=10,
            queuedTasks=0,
            latencyUS=0,
            taskLock=True,
            draining=draining,
            processors=[],
            capabilities={},
            workerManagerID=_MANAGER_ID,
        )

    async def test_a_draining_heartbeat_takes_the_worker_out_of_service(self) -> None:
        await self.controller.on_heartbeat(_WORKER_ID, self._heartbeat(draining=True))
        self.policy_controller.mark_worker_draining.assert_called_once_with(_WORKER_ID)

    async def test_a_draining_heartbeat_reclaims_the_queued_tasks(self) -> None:
        await self.controller.on_heartbeat(_WORKER_ID, self._heartbeat(draining=True))
        self.task_controller.on_task_balance_cancel.assert_awaited_once_with(_TASK_ID)

    async def test_an_ordinary_heartbeat_drains_nothing(self) -> None:
        await self.controller.on_heartbeat(_WORKER_ID, self._heartbeat(draining=False))
        self.policy_controller.mark_worker_draining.assert_not_called()
        self.task_controller.on_task_balance_cancel.assert_not_awaited()

    async def test_the_queue_is_reclaimed_only_once(self) -> None:
        # A draining worker keeps reporting the flag in every later heartbeat. Only the first one
        # does the work; mark_worker_draining returning False is what stops the repeat.
        self.policy_controller.mark_worker_draining.side_effect = [True, False, False]

        for _ in range(3):
            await self.controller.on_heartbeat(_WORKER_ID, self._heartbeat(draining=True))

        self.assertEqual(self.task_controller.on_task_balance_cancel.await_count, 1)

    async def test_a_draining_worker_with_an_empty_queue_cancels_nothing(self) -> None:
        self.policy_controller.evacuate_worker.return_value = []
        await self.controller.on_heartbeat(_WORKER_ID, self._heartbeat(draining=True))
        self.task_controller.on_task_balance_cancel.assert_not_awaited()
