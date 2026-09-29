import asyncio
import unittest
from typing import List, Tuple
from unittest.mock import MagicMock

from scaler.protocol.capnp import TaskCancel
from scaler.scheduler.controllers.client_controller import VanillaClientController
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
            worker_controller=MagicMock(),
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
