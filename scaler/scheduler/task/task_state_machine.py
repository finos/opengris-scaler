from typing import Optional, Dict

from scaler.protocol.python.common import TaskState, TaskTransition


class TaskStateMachine:
    # see https://github.com/finos/opengris-scaler/issues/56
    TRANSITION_MAP: Dict[TaskState, Dict[TaskTransition, TaskState]] = {
        TaskState.Inactive: {
            TaskTransition.HasCapacity: TaskState.Running,
            TaskTransition.TaskCancel: TaskState.Canceled,
        },
        TaskState.Canceling: {
            TaskTransition.TaskCancelConfirmCanceled: TaskState.Canceled,
            TaskTransition.WorkerDisconnect: TaskState.Canceled,
            TaskTransition.TaskCancelConfirmFailed: TaskState.Running,
            TaskTransition.TaskCancelConfirmNotFound: TaskState.CanceledNotFound,
        },
        TaskState.Running: {
            TaskTransition.TaskResultSuccess: TaskState.Success,
            TaskTransition.TaskResultFailed: TaskState.Failed,
            TaskTransition.TaskResultWorkerDied: TaskState.FailedWorkerDied,
            TaskTransition.TaskCancel: TaskState.Canceling,
            TaskTransition.BalanceTaskCancel: TaskState.BalanceCanceling,
            TaskTransition.WorkerDisconnect: TaskState.WorkerDisconnecting,
        },
        TaskState.BalanceCanceling: {
            TaskTransition.TaskResultSuccess: TaskState.Success,
            TaskTransition.TaskResultFailed: TaskState.Failed,
            TaskTransition.TaskResultWorkerDied: TaskState.FailedWorkerDied,
            TaskTransition.TaskCancel: TaskState.Canceling,
            TaskTransition.TaskCancelConfirmCanceled: TaskState.Inactive,
            TaskTransition.TaskCancelConfirmFailed: TaskState.Running,
            TaskTransition.WorkerDisconnect: TaskState.WorkerDisconnecting,
        },
        TaskState.WorkerDisconnecting: {
            TaskTransition.SchedulerHasTask: TaskState.Inactive,
            TaskTransition.SchedulerHasNoTask: TaskState.FailedWorkerDied,
        },
    }

    def __init__(self, debug):
        self._debug = debug
        self._paths = list()

        self._previous_state = None
        self._state = TaskState.Inactive

    def __repr__(self):
        return f"TaskStateMachine(previous_state={self._previous_state}, state={self._state})"

    def get_path(self):
        return (
            " ".join(f"[{state.name}] -{transition.name}->" for state, transition in self._paths)
            + f" [{self._state.name}]"
        )

    def previous_state(self) -> Optional[TaskState]:
        return self._previous_state

    def current_state(self) -> TaskState:
        return self._state

    def is_running(self) -> bool:
        return self._state == TaskState.Running

    def is_canceling(self) -> bool:
        return self._state == TaskState.Canceling

    def is_finished(self) -> bool:
        return self._state in {TaskState.Success, TaskState.Failed, TaskState.FailedWorkerDied}

    def is_canceled(self) -> bool:
        return self._state in {TaskState.Canceled, TaskState.CanceledNotFound}

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
