import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, ErrorCode
from scaler.protocol.capnp import (
    Task,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
    TaskState,
    TaskTransition,
)
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.utility.identifiers import ClientID, TaskID, WorkerID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class TestTaskControllerRoutingResilience(unittest.TestCase):
    """No exception from a state function may propagate out of __routing and crash the scheduler.

    __routing runs from message handlers and from timer loops (the balancer, the worker-cleanup loop);
    an escape would propagate through asyncio.gather and terminate the whole scheduler. A departed-peer
    error is expected and dropped quietly; any other bug is logged with its transition/state path and
    dropped so the scheduler stays alive.
    """

    @staticmethod
    def _controller() -> VanillaTaskController:
        controller = VanillaTaskController(config_controller=MagicMock())
        return controller

    def _drive_running_handler_raising(self, controller: VanillaTaskController, task_id: TaskID, error: Exception):
        controller._task_state_manager.add_state_machine(task_id)  # starts inactive

        async def handler(*_args, **_kwargs):
            raise error

        controller._state_functions[TaskState.running] = handler
        # inactive --hasCapacity--> running, then the (patched) running handler raises.
        routing = controller._VanillaTaskController__routing  # type: ignore[attr-defined]
        _run(routing(task_id, TaskTransition.hasCapacity, worker_id=None))

    def test_departed_peer_socket_closed_is_swallowed(self):
        controller = self._controller()
        # Must not raise: a departed peer is routine, not scheduler-fatal.
        self._drive_running_handler_raising(
            controller,
            TaskID(b"departed-peer-task"),
            ConnectorSocketClosedByRemoteEndError(ErrorCode.ConnectorSocketClosedByRemoteEnd, "client gone"),
        )

    def test_other_errors_are_logged_not_propagated(self):
        controller = self._controller()
        # A genuine bug in a state function must NOT propagate out of __routing and crash the scheduler:
        # it is logged and the transition is dropped so the scheduler stays alive (create_async_loop_routine
        # is the wider backstop for the rest).
        with patch("scaler.scheduler.controllers.task_controller.logger.exception") as mock_exception:
            self._drive_running_handler_raising(controller, TaskID(b"real-bug-task"), ValueError("real bug"))
        self.assertTrue(mock_exception.called, "the bug must be logged")

    def test_worker_disconnect_while_canceling_supplies_cancel_confirm(self):
        """A canceling task whose worker disconnects must reach __state_canceled with a TaskCancelConfirm,
        not a worker_id, or the canceling -> canceled transition raises TypeError."""
        controller = self._controller()
        task_id = TaskID(b"canceling-task")

        state_machine = controller._task_state_manager.add_state_machine(task_id)
        state_machine.on_transition(TaskTransition.hasCapacity)  # inactive -> running
        state_machine.on_transition(TaskTransition.taskCancel)  # running -> canceling
        self.assertEqual(state_machine.current_state(), TaskState.canceling)

        captured = {}

        async def fake_canceled(_task_id, _state_machine, task_cancel_confirm):  # __state_canceled's signature
            captured["confirm"] = task_cancel_confirm

        controller._state_functions[TaskState.canceled] = fake_canceled

        # Must not raise: pre-fix this handed __state_canceled worker_id and raised TypeError.
        _run(controller.on_worker_disconnect(task_id, WorkerID(b"dead-worker")))

        self.assertEqual(state_machine.current_state(), TaskState.canceled)
        self.assertIsInstance(captured.get("confirm"), TaskCancelConfirm)
        self.assertEqual(captured["confirm"].taskId, task_id)
        self.assertEqual(captured["confirm"].cancelConfirmType, TaskCancelConfirmType.canceled)


class TestTaskControllerDepartedClientCleanup(unittest.TestCase):
    """Delivering a task result to a client that has departed must still clean the task up.

    If the send raised out of __send_task_result_to_client, remove_state_machine / pop / retry were all
    skipped: the failed task's state machine leaked ("stuck in limbo") and freed workers were never handed
    the queued tasks. Under a batch of failing tasks whose client was killed, that wedges the scheduler.
    """

    @staticmethod
    def _make_task(task_id: TaskID) -> Task:
        return Task(
            taskId=task_id,
            source=ClientID(b"departed-client"),
            metadata=b"",
            funcObjectId=b"",
            functionArgs=[],
            capabilities={},
        )

    def test_failed_result_to_departed_client_still_cleans_up(self):
        controller = VanillaTaskController(config_controller=MagicMock())

        binder = MagicMock()
        binder.send = AsyncMock(
            side_effect=ConnectorSocketClosedByRemoteEndError(ErrorCode.ConnectorSocketClosedByRemoteEnd, "client gone")
        )
        binder_monitor = MagicMock()
        binder_monitor.send = AsyncMock()

        client_controller = MagicMock()
        client_controller.on_task_finish.return_value = ClientID(b"departed-client")  # still tracked, but dead

        object_controller = MagicMock()
        object_controller.get_object_name.return_value = b""

        worker_controller = MagicMock()
        worker_controller.on_task_done = AsyncMock()
        worker_controller.acquire_worker.return_value = WorkerID.invalid_worker_id()

        graph_controller = MagicMock()
        graph_controller.is_graph_subtask.return_value = False

        controller.register(
            binder, binder_monitor, client_controller, object_controller, worker_controller, graph_controller
        )

        task_id = TaskID(b"failing-task")
        state_machine = controller._task_state_manager.add_state_machine(task_id)
        state_machine.on_transition(TaskTransition.hasCapacity)  # inactive -> running
        controller._task_id_to_task[task_id] = self._make_task(task_id)

        # The worker reports the task failed; delivering that failure to the departed client raises inside
        # the send. It must not abort the cleanup.
        _run(
            controller.on_task_result(
                TaskResult(taskId=task_id, resultType=TaskResultType.failed, metadata=b"", results=[])
            )
        )

        self.assertIsNone(
            controller._task_state_manager.get_state_machine(task_id), "state machine leaked (task in limbo)"
        )
        self.assertNotIn(task_id, controller._task_id_to_task)
        worker_controller.on_task_done.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
