import asyncio
import unittest
from unittest.mock import MagicMock, patch

from scaler.protocol.capnp import TaskCancelConfirm, TaskCancelConfirmType, TaskState, TaskTransition
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.utility.identifiers import TaskID, WorkerID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class TestTaskControllerRoutingResilience(unittest.TestCase):
    """No exception from a state function may propagate out of __routing and crash the scheduler.

    __routing runs from message handlers and from timer loops (the balancer, the worker-cleanup loop);
    an escape would propagate through asyncio.gather and terminate the whole scheduler. A bug is logged
    with its transition/state path and dropped so the scheduler stays alive.
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


class TestTaskControllerBalanceCancelGuard(unittest.TestCase):
    """on_task_balance_cancel must be a no-op for a task that is not running.

    Under load a saturated worker is slow to confirm a balance-cancel, so the task lingers in
    balanceCanceling and the balancer re-advises the same move every cycle. Re-issuing balanceTaskCancel to a
    balanceCanceling task is an invalid transition ("cannot apply 8 to current state 3"); the guard skips it
    instead of logging that error every cycle.
    """

    def test_balance_cancel_on_balance_canceling_task_is_skipped(self):
        controller = VanillaTaskController(config_controller=MagicMock())
        task_id = TaskID(b"balancing-task")
        state_machine = controller._task_state_manager.add_state_machine(task_id)
        state_machine.on_transition(TaskTransition.hasCapacity)  # inactive -> running
        state_machine.on_transition(TaskTransition.balanceTaskCancel)  # running -> balanceCanceling
        self.assertEqual(state_machine.current_state(), TaskState.balanceCanceling)

        with patch("scaler.scheduler.task.task_state_manager.logger") as mock_state_logger:
            _run(controller.on_task_balance_cancel(task_id))

        mock_state_logger.error.assert_not_called()  # no invalid-transition error
        self.assertEqual(state_machine.current_state(), TaskState.balanceCanceling)


if __name__ == "__main__":
    unittest.main()
