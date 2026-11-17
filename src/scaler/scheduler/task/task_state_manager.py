import logging
from typing import Dict, Optional, Tuple, Type

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

from scaler.protocol.capnp import TaskState, TraceContext
from scaler.scheduler.task.task_event import TaskEvent
from scaler.scheduler.task.task_state_machine import TaskStateMachine
from scaler.utility.identifiers import TaskID
from scaler.utility.semantic_conventions import (
    FUNCTION_NAME_ATTRIBUTE,
    SCHEDULER_QUEUED_SPAN,
    SCHEDULER_TASK_SPAN,
    TASK_ID_ATTRIBUTE,
    TASK_STATE_ATTRIBUTE,
)
from scaler.utility.telemetry import empty_trace_context, extract_trace_context, inject_trace_context

logger = logging.getLogger(__name__)

tracer = trace.get_tracer(__name__)


# the span status a terminal state ends the task span with; a canceled task is not an error
TERMINAL_STATE_STATUS: Dict[TaskState, StatusCode] = {
    TaskState.success: StatusCode.OK,
    TaskState.failed: StatusCode.ERROR,
    TaskState.failedWorkerDied: StatusCode.ERROR,
    TaskState.failedSchedulerFault: StatusCode.ERROR,
    TaskState.canceled: StatusCode.UNSET,
    TaskState.canceledNotFound: StatusCode.UNSET,
}


class TaskStateManager:
    # every state carries a counter, so a new member of TaskState needs no edit here
    TASK_STATES: Tuple[TaskState, ...] = tuple(TaskState)

    def __init__(self, debug: bool):
        self._debug = debug
        self._task_id_to_state_machine: Dict[TaskID, TaskStateMachine] = dict()
        # the context holding each task's span, and the client's baggage the worker must receive with it
        self._task_id_to_context: Dict[TaskID, Context] = dict()
        # the open queued span of each task waiting in inactive for a worker
        self._task_id_to_queued_span: Dict[TaskID, Span] = dict()
        self._statistics: Dict[TaskState, int] = {state: 0 for state in self.TASK_STATES}

    def add_state_machine(self, task_id: TaskID, trace_context: TraceContext, function_name: str) -> TaskStateMachine:
        """Create new task state machine, the machine starts in the inactive state.

        The task span lives as long as the machine, as a child of the client's span in `trace_context`.
        """
        assert task_id not in self._task_id_to_state_machine

        state_machine = TaskStateMachine(self._debug)
        self._task_id_to_state_machine[task_id] = state_machine
        self._statistics[state_machine.current_state()] += 1

        parent_context = extract_trace_context(trace_context)
        span = tracer.start_span(
            SCHEDULER_TASK_SPAN,
            context=parent_context,
            kind=SpanKind.CONSUMER,
            attributes={TASK_ID_ATTRIBUTE: task_id.hex(), FUNCTION_NAME_ATTRIBUTE: function_name},
        )
        self._task_id_to_context[task_id] = trace.set_span_in_context(span, parent_context)
        self.__start_queued_span(task_id)
        return state_machine

    def remove_state_machine(self, task_id: TaskID) -> None:
        """Remove a machine. The caller must hold its lock, or the machine could vanish under a transition that
        already checked for it. This logs rather than asserts, since asserts are stripped under -O.
        """

        state_machine = self._task_id_to_state_machine.pop(task_id, None)
        if state_machine is not None and not state_machine.lock.locked():
            logger.error(f"{task_id!r}: state machine removed without its lock held")

        self.__end_queued_span(task_id)
        span_context = self._task_id_to_context.pop(task_id, None)
        if span_context is not None and state_machine is not None:
            span = trace.get_current_span(span_context)
            final_state = state_machine.current_state()
            span.set_attribute(TASK_STATE_ATTRIBUTE, final_state.name)
            span.set_status(Status(TERMINAL_STATE_STATUS.get(final_state, StatusCode.UNSET)))
            span.end()

    def get_state_machine(self, task_id: TaskID) -> Optional[TaskStateMachine]:
        return self._task_id_to_state_machine.get(task_id, None)

    def get_trace_context(self, task_id: TaskID) -> TraceContext:
        """The trace context a worker receives with the task, so its spans nest under the task span."""
        span_context = self._task_id_to_context.get(task_id)
        if span_context is None:
            return empty_trace_context()
        return inject_trace_context(span_context)

    def commit(self, task_id: TaskID, event_type: Type[TaskEvent], target: TaskState) -> None:
        """Write the state that the action of ``event_type`` returned, or the state a faulted task is torn down into.

        This is the only place that moves a task from one state to another, so it is also the only place the state
        counts stay balanced. The action already ran, so this cannot fail: an event that the source state does not
        accept never reaches this point.
        """

        state_machine = self._task_id_to_state_machine.get(task_id, None)
        if state_machine is None:
            logger.error(f"{task_id!r}: cannot commit {event_type.__name__} for non-existent state machine")
            return

        if not state_machine.lock.locked():
            logger.error(
                f"{task_id!r}: attempted to commit {event_type.__name__} for a state machine without holding its lock"
            )
            return

        source = state_machine.current_state()
        state_machine.commit(event_type.__name__, target)

        self._statistics[source] -= 1
        self._statistics[target] += 1

        span_context = self._task_id_to_context.get(task_id)
        if span_context is not None:
            trace.get_current_span(span_context).add_event(event_type.__name__, {TASK_STATE_ATTRIBUTE: target.name})

        if source == TaskState.inactive and target != TaskState.inactive:
            self.__end_queued_span(task_id)
        elif source != TaskState.inactive and target == TaskState.inactive:
            self.__start_queued_span(task_id)  # requeued, after its worker died or a balance cancel

    def __start_queued_span(self, task_id: TaskID) -> None:
        span_context = self._task_id_to_context.get(task_id)
        if span_context is None:
            return
        self._task_id_to_queued_span[task_id] = tracer.start_span(SCHEDULER_QUEUED_SPAN, context=span_context)

    def __end_queued_span(self, task_id: TaskID) -> None:
        queued_span = self._task_id_to_queued_span.pop(task_id, None)
        if queued_span is not None:
            queued_span.end()

    def get_statistics(self) -> Dict[TaskState, int]:
        return self._statistics

    def get_debug_paths(self) -> str:
        return "\n".join(
            f"{task_id!r}: {state_machine.get_path()}"
            for task_id, state_machine in self._task_id_to_state_machine.items()
        )
