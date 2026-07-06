import time
import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import WorkerDisconnectNotification
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
        notification = WorkerDisconnectNotification(worker=_WORKER_ID)
        await self.controller.on_disconnect_notification(_WORKER_ID, notification)
        self.assertNotIn(_WORKER_ID, self.controller._worker_alive_since)

    async def test_on_disconnect_notification_sends_no_reply(self) -> None:
        notification = WorkerDisconnectNotification(worker=_WORKER_ID)
        await self.controller.on_disconnect_notification(_WORKER_ID, notification)
        self.binder.send.assert_not_called()

    async def test_on_disconnect_notification_unknown_worker_is_safe(self) -> None:
        # WDN for a worker not in the registry (e.g. already timed out) must not crash.
        unknown_id = WorkerID(b"unknown_worker")
        notification = WorkerDisconnectNotification(worker=unknown_id)
        await self.controller.on_disconnect_notification(_WORKER_ID, notification)
        self.assertIn(_WORKER_ID, self.controller._worker_alive_since)

    async def test_on_disconnect_notification_re_dispatches_in_flight_tasks(self) -> None:
        self.policy_controller.remove_worker.return_value = [_TASK_ID]

        notification = WorkerDisconnectNotification(worker=_WORKER_ID)
        await self.controller.on_disconnect_notification(_WORKER_ID, notification)

        self.task_controller.on_worker_disconnect.assert_awaited_once_with(_TASK_ID, _WORKER_ID)
