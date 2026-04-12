from typing import Dict, Optional

from scaler.protocol.capnp import TaskState, TaskTransition


class TaskStateMachine:
    # see https://github.com/finos/opengris-scaler/issues/56
    TRANSITION_MAP: Dict[TaskState, Dict[TaskTransition, TaskState]] = {
        TaskState.inactive: {
            TaskTransition.hasCapacity: TaskState.running,
            TaskTransition.taskCancel: TaskState.canceled,
        },
        TaskState.canceling: {
            TaskTransition.taskCancelConfirmCanceled: TaskState.canceled,
            TaskTransition.workerDisconnect: TaskState.canceled,
            TaskTransition.taskCancelConfirmFailed: TaskState.running,
            TaskTransition.taskCancelConfirmNotFound: TaskState.canceledNotFound,
        },
        TaskState.running: {
            TaskTransition.taskResultSuccess: TaskState.success,
            TaskTransition.taskResultFailed: TaskState.failed,
            TaskTransition.taskResultWorkerDied: TaskState.failedWorkerDied,
            TaskTransition.taskCancel: TaskState.canceling,
            TaskTransition.balanceTaskCancel: TaskState.balanceCanceling,
            TaskTransition.workerDisconnect: TaskState.workerDisconnecting,
        },
        TaskState.balanceCanceling: {
            TaskTransition.taskResultSuccess: TaskState.success,
            TaskTransition.taskResultFailed: TaskState.failed,
            TaskTransition.taskResultWorkerDied: TaskState.failedWorkerDied,
            TaskTransition.taskCancel: TaskState.canceling,
            TaskTransition.taskCancelConfirmCanceled: TaskState.inactive,
            TaskTransition.taskCancelConfirmFailed: TaskState.running,
            TaskTransition.workerDisconnect: TaskState.workerDisconnecting,
        },
        TaskState.workerDisconnecting: {
            TaskTransition.schedulerHasTask: TaskState.inactive,
            TaskTransition.schedulerHasNoTask: TaskState.failedWorkerDied,
        },
    }

    def __init__(self, debug):
        self._debug = debug
        self._paths = list()

        self._previous_state: Optional[TaskState] = None
        self._state = TaskState.inactive

    def __repr__(self):
        return f"TaskStateMachine(previous_state={self._previous_state}, state={self._state})"

    def get_path(self):
        return (
            " ".join(f"[{state._as_str()}] -{transition._as_str()}->" for state, transition in self._paths)
            + f" [{self._state._as_str()}]"
        )

    def previous_state(self) -> Optional[TaskState]:
        return self._previous_state

    def current_state(self) -> TaskState:
        return self._state

    def is_running(self) -> bool:
        return self._state == TaskState.running

    def is_canceling(self) -> bool:
        return self._state == TaskState.canceling

    def is_finished(self) -> bool:
        return self._state in {TaskState.success, TaskState.failed, TaskState.failedWorkerDied}

    def is_canceled(self) -> bool:
        return self._state in {TaskState.canceled, TaskState.canceledNotFound}

    def is_done(self) -> bool:
        return self.is_finished() or self.is_canceled()

    def on_transition(self, transition: TaskTransition) -> bool:
        if self._state not in TaskStateMachine.TRANSITION_MAP:
            return False

        options = TaskStateMachine.TRANSITION_MAP[self._state]
        if transition not in options:
            return False

        if self._debug:
            self._paths.append((self._state, transition))

        self._previous_state = self._state
        self._state = options[transition]
        return True
