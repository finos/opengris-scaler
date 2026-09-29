import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, ErrorCode
from scaler.protocol.capnp import Task, WorkerDisconnectNotification
from scaler.scheduler.controllers.mixins import ConfigController, PolicyController, TaskController
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.scheduler.controllers.vanilla_policy_controller import VanillaPolicyController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
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


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _DeadableBinder:
    """An async binder whose send() fails as a departed peer for any worker marked dead."""

    def __init__(self):
        self.dead = set()

    async def send(self, to, message, *, detached: bool = True):
        if WorkerID(bytes(to)) in self.dead:
            raise ConnectorSocketClosedByRemoteEndError(
                ErrorCode.ConnectorSocketClosedByRemoteEnd, "worker socket closed by remote end"
            )


class _NullMonitor:
    async def send(self, message):
        return None


class TestWorkerControllerMassEviction(unittest.TestCase):
    """A whole batch of workers dropping at once must be cleaned up without crashing the scheduler.

    With one shared capability every task fits every worker, so a task shed from a dead worker is
    reassigned to another dead-but-still-registered worker whose send also fails. Nothing on that path may
    re-enter the disconnect path: the heartbeat sweep disconnects the whole batch iteratively, without a
    per-worker reroute cascade or unbounded coroutine recursion. The fake binder below raises on a send to
    a dead worker, which real detached sends no longer do -- keeping it holds the stricter guarantee.
    """

    N_WORKERS = 300  # a large simultaneous batch, well past any reasonable recursion limit

    @staticmethod
    def _make_task(index: int) -> Task:
        return Task(
            taskId=TaskID(f"task-{index}".encode()),
            source=ClientID(b"client"),
            metadata=b"",
            funcObjectId=b"",
            functionArgs=[],
            capabilities={},
        )

    def test_mass_eviction_is_handled_without_crashing(self):
        config = MagicMock()
        config.get_config.side_effect = lambda key: 0 if key == "worker_timeout_seconds" else MagicMock()
        policy = VanillaPolicyController("simple", "allocate=capability; scaling=vanilla")
        worker_controller = VanillaWorkerController(config, policy)
        task_controller = VanillaTaskController(config)

        binder = _DeadableBinder()
        monitor = _NullMonitor()
        connector_storage = MagicMock()
        client_controller = MagicMock()
        client_controller.on_task_finish.return_value = None
        object_controller = MagicMock()
        object_controller.get_object_name.return_value = b""
        graph_controller = MagicMock()
        graph_controller.is_graph_subtask.return_value = False

        worker_controller.register(binder, monitor, task_controller)  # type: ignore[arg-type]
        task_controller.register(
            binder,  # type: ignore[arg-type]
            monitor,  # type: ignore[arg-type]
            connector_storage,
            client_controller,
            object_controller,
            worker_controller,
            graph_controller,
        )

        # Register N workers with a stale last-heartbeat so the sweep times all of them out at once
        # (bypassing on_heartbeat; replicate the state it maintains).
        manager_id = b"worker-manager"
        for i in range(self.N_WORKERS):
            worker_id = WorkerID(f"worker-{i}".encode())
            policy.add_worker(worker_id, {"capA": -1}, 10)
            worker_controller._worker_alive_since[worker_id] = (time.time() - 3600, None)
            worker_controller._worker_to_manager[worker_id] = manager_id
            worker_controller._manager_to_workers.setdefault(manager_id, set()).add(worker_id)

        async def scenario():
            for i in range(self.N_WORKERS):  # one running task per worker, all sent while live
                await task_controller.on_task_new(self._make_task(i))
            for i in range(self.N_WORKERS):  # a whole batch of workers dies at once
                binder.dead.add(WorkerID(f"worker-{i}".encode()))
            # Must not raise RecursionError: the sweep disconnects the batch iteratively rather than
            # letting a failed reroute send re-enter the disconnect path.
            await worker_controller.routine()

        _run(scenario())

        # Every dead worker was disconnected; none is left registered.
        self.assertEqual(len(policy.get_worker_ids()), 0)


if __name__ == "__main__":
    unittest.main()
