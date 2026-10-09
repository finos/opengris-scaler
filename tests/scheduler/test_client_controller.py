import asyncio
import unittest
from typing import List, Tuple
from unittest.mock import AsyncMock, MagicMock

from scaler.io.utility import deserialize, serialize
from scaler.protocol.capnp import ClientDisconnect, TaskCancel, WorkerManagerHeartbeat, WorkerManagerShutdown
from scaler.scheduler.controllers.client_controller import VanillaClientController
from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ClientID, TaskID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class TestClientControllerDisconnect(unittest.TestCase):
    """A client that is gone must not keep its workers: a worker refuses an unforced cancel of a running task."""

    def test_disconnect_force_cancels_the_clients_tasks(self):
        controller = VanillaClientController(config_controller=MagicMock())

        cancelled: List[Tuple[ClientID, TaskCancel]] = []

        task_controller = MagicMock()

        async def on_task_cancel(client_id: ClientID, task_cancel: TaskCancel) -> None:
            cancelled.append((client_id, task_cancel))

        task_controller.on_task_cancel = on_task_cancel

        controller.register(
            binder=MagicMock(),
            binder_monitor=MagicMock(),
            object_controller=MagicMock(),
            task_controller=task_controller,
            worker_manager_controller=MagicMock(),
        )

        client_id = ClientID.generate_client_id()
        task_ids = [TaskID(b"running-task-0"), TaskID(b"running-task-1")]
        for task_id in task_ids:
            controller.on_task_begin(client_id, task_id)

        disconnect = controller._VanillaClientController__on_client_disconnect  # type: ignore[attr-defined]
        _run(disconnect(client_id))

        self.assertEqual(len(cancelled), len(task_ids))
        self.assertEqual({task_cancel.taskId for _client, task_cancel in cancelled}, set(task_ids))
        for _client, task_cancel in cancelled:
            # read it back the way the worker will: an unset flags field only becomes force=False on the wire
            on_the_wire = TaskCancel.from_bytes(task_cancel.to_bytes())
            self.assertTrue(
                on_the_wire.flags.force,
                "a dead client's tasks must be force-cancelled, or a running one keeps its worker",
            )


if __name__ == "__main__":
    unittest.main()


class TestClientControllerShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_asks_every_worker_manager_to_drain(self) -> None:
        """A cluster shutdown goes through the worker managers: the scheduler stops no worker itself."""
        config_controller = MagicMock()
        config_controller.get_config.side_effect = lambda path: False if path == "protected" else MagicMock()
        worker_manager_controller = WorkerManagerController(config_controller, MagicMock())
        binder = AsyncMock()
        worker_manager_controller.register(binder, MagicMock(), MagicMock())
        worker_manager_controller._manager_alive_since = {
            b"manager-a": (0.0, MagicMock()),
            b"manager-b": (0.0, MagicMock()),
        }

        controller = VanillaClientController(config_controller=config_controller)
        controller.register(
            binder=binder,
            binder_monitor=AsyncMock(),
            object_controller=MagicMock(),
            task_controller=MagicMock(),
            worker_manager_controller=worker_manager_controller,
        )

        with self.assertRaises(ClientShutdownException):
            await controller.on_client_disconnect(
                ClientID.generate_client_id(), ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.shutdown)
            )

        shutdowns = [
            call.args[0] for call in binder.send.call_args_list if isinstance(call.args[1], WorkerManagerShutdown)
        ]
        self.assertEqual(shutdowns, [b"manager-a", b"manager-b"])


class TestWorkerManagerControllerStatus(unittest.IsolatedAsyncioTestCase):
    async def test_status_reports_the_units_each_manager_reports(self) -> None:
        policy_controller = MagicMock()
        policy_controller.get_scaling_commands.return_value = []
        worker_manager_controller = WorkerManagerController(MagicMock(), policy_controller)
        worker_controller = MagicMock()
        worker_controller.get_workers_by_manager_id.return_value = []
        worker_controller._worker_alive_since = {}
        task_controller = MagicMock()
        task_controller._task_id_to_task = {}
        worker_manager_controller.register(AsyncMock(), task_controller, worker_controller)

        heartbeat = deserialize(
            serialize(
                WorkerManagerHeartbeat(
                    maxTaskConcurrency=4, workerManagerID=b"manager", activeUnits=2, pendingUnits=1, drainingUnits=3
                )
            )
        )
        assert isinstance(heartbeat, WorkerManagerHeartbeat)
        await worker_manager_controller.on_heartbeat(b"source", heartbeat)

        (detail,) = worker_manager_controller.get_status().workerManagerDetails
        self.assertEqual((detail.activeUnits, detail.pendingUnits, detail.drainingUnits), (2, 1, 3))
