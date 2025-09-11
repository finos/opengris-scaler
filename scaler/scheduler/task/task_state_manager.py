import logging
from typing import Dict, Optional

from scaler.protocol.python.common import TaskState, TaskTransition
from scaler.scheduler.task.task_state_machine import TaskStateMachine
from scaler.utility.identifiers import TaskID


class TaskStateManager:
    def __init__(self, debug: bool):
        self._debug = debug
        self._task_id_to_state_machine: Dict[TaskID, TaskStateMachine] = dict()
        self._statistics: Dict[TaskState, int] = {state: 0 for state in TaskState}

    def add_state_machine(self, task_id: TaskID) -> TaskStateMachine:
        """Create new task state machine, return True if success, False otherwise"""
        assert task_id not in self._task_id_to_state_machine

        state_machine = TaskStateMachine(self._debug)
        self._task_id_to_state_machine[task_id] = state_machine
        self._statistics[state_machine.current_state()] += 1
        return state_machine

    def remove_state_machine(self, task_id: TaskID):
        self._task_id_to_state_machine.pop(task_id)

    def get_state_machine(self, task_id: TaskID) -> Optional[TaskStateMachine]:
        return self._task_id_to_state_machine.get(task_id, None)

    def on_transition(self, task_id: TaskID, transition: TaskTransition) -> Optional[TaskStateMachine]:
        """if adjust task state machine is successful, then return TaskStateFlags object associate with the task_id,
        return None otherwise

        This should be a central place to synchronize task state machine, if any unexpected event happened, it will not
        return the TaskStateFlags
        """

        task_state_machine = self._task_id_to_state_machine.get(task_id, None)
        if task_state_machine is None:
            logging.error(f"{task_id!r}: unknown {transition=} for non-existed state machine")
            return None

        transit_success = task_state_machine.on_transition(transition)
        if transit_success:
            self._statistics[task_state_machine.previous_state()] -= 1
            self._statistics[task_state_machine.current_state()] += 1
        else:
            logging.error(
                f"{task_id!r}: cannot apply {transition} to current state" f" {task_state_machine.current_state()}"
            )

        return task_state_machine if transit_success else None

    def get_statistics(self) -> Dict[TaskState, int]:
        return self._statistics

    def get_debug_paths(self):
        return "\n".join(
            f"{task_id!r}: {state_machine.get_path()}"
            for task_id, state_machine in self._task_id_to_state_machine.items()
        )
