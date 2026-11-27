import asyncio
import dataclasses
import enum
import functools
import logging
import math
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.trace import Span, Status, StatusCode

from scaler.config.constants import WORKER_MANAGER_MAX_RESTART_BACKOFF_DOUBLINGS, WORKER_MANAGER_UNIT_STABLE_SECONDS
from scaler.utility.cooldown import Cooldown
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.semantic_conventions import (
    UNIT_ID_ATTRIBUTE,
    WORKER_MANAGER_CREATE_UNIT_SPAN,
    WORKER_MANAGER_DESTROY_UNIT_SPAN,
)
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner

logger = logging.getLogger(__name__)

tracer = trace.get_tracer(__name__)


class UnitState(enum.Enum):
    pending = enum.auto()  # create dispatched, no heartbeat yet
    active = enum.auto()  # serving
    stopping = enum.auto()  # to destroy, no destroy call in flight
    destroying = enum.auto()  # destroy call in flight


@dataclasses.dataclass
class Unit:
    unit_id: str
    handle: Optional[UnitHandle]  # None until create_unit returns
    state: UnitState
    state_since: float  # time.monotonic()
    active_task_concurrency: int = 0  # task slots the unit reported
    assigned_tasks: int = 0  # queued and running tasks the unit reported
    last_heartbeat: Optional[float] = None  # time.monotonic(); None until the first heartbeat
    # the scaling decision a shed unit stops for, so its destroy nests under it; a lost unit's destroy starts a trace
    stop_context: Optional[Context] = None


@dataclasses.dataclass(frozen=True)
class UnitControllerStatus:
    active_task_concurrency: int
    assigned_tasks: int
    active_units: int
    pending_units: int


class UnitController(Looper, Reporter):
    """The only owner of the fleet of a worker manager: creates, supervises, and destroys its units.

    Every provisioner call runs in the background, so a slow cloud API never stops the routine. The controller sends
    no message: the runner answers each unit heartbeat from the state the controller keeps.

    A unit is supervised by its messages alone: its heartbeat says it works, and its disconnect notification says it is
    gone. A unit that sends no heartbeat, such as a crashed or deadlocked worker, is lost.
    """

    def __init__(
        self,
        provisioner: UnitProvisioner,
        scale_down_cooldown_seconds: float,
        unit_timeout_seconds: float,
        restart_backoff_seconds: float,
    ) -> None:
        self._provisioner = provisioner
        self._scale_down_cooldown = Cooldown(scale_down_cooldown_seconds)
        self._unit_timeout_seconds = unit_timeout_seconds
        self._restart_backoff_seconds = restart_backoff_seconds

        self._desired_task_concurrency = 0
        # the context of the call that last changed the desired concurrency, so the creates it causes nest under it
        self._desired_change_context: Context = context.get_current()
        self._units: Dict[str, Unit] = {}  # in creation order

        # The event loop keeps only a weak reference to a task: without this set, a provisioner call could be
        # collected mid-flight and leak the resource it was creating. A call leaves it only after its done
        # callback ran, so terminate() never sees it empty while a follow-up call is about to start.
        self._tasks: Set[asyncio.Task] = set()

        self._consecutive_unit_losses = 0
        self._create_not_before = 0.0  # time.monotonic()
        self._terminating = False  # set by terminate(): every unit is destroyed as soon as it has a handle

    def set_desired_task_concurrency(self, task_concurrency: int) -> None:
        if task_concurrency != self._desired_task_concurrency:
            logger.info(f"desired task concurrency changed: {self._desired_task_concurrency} -> {task_concurrency}")
            self._desired_change_context = context.get_current()
        self._desired_task_concurrency = task_concurrency

    def on_unit_heartbeat(self, unit_id: str, active_task_concurrency: int, assigned_tasks: int) -> None:
        unit = self._units.get(unit_id)
        if unit is None:
            return

        now = time.monotonic()
        unit.last_heartbeat = now
        unit.active_task_concurrency = active_task_concurrency
        unit.assigned_tasks = assigned_tasks

        if self._is_stable(unit, now):
            self._consecutive_unit_losses = 0

        # A heartbeat that beats create_unit back waits for the next one: an active unit always has a handle.
        if unit.state == UnitState.pending and unit.handle is not None:
            logger.info(f"unit {unit_id!r} is active")
            self._set_state(unit, UnitState.active)

    def on_unit_disconnect(self, unit_id: str) -> None:
        """The unit is about to exit on its own: destroy what it runs on now, and count it as lost."""
        unit = self._units.get(unit_id)
        if unit is None or unit.state in (UnitState.stopping, UnitState.destroying) or unit.handle is None:
            return

        logger.warning(f"unit {unit_id!r} exited on its own from state {unit.state.name}")
        self._on_unit_lost(unit)
        self._set_state(unit, UnitState.stopping)

    def get_status(self) -> UnitControllerStatus:
        active = self._units_in(UnitState.active)
        return UnitControllerStatus(
            active_task_concurrency=sum(unit.active_task_concurrency for unit in active),
            assigned_tasks=sum(unit.assigned_tasks for unit in self._units.values()),
            active_units=len(active),
            pending_units=len(self._units_in(UnitState.pending)),
        )

    async def routine(self) -> None:
        self._sweep_silent_units()
        self._reconcile()
        self._drive_destroys()

    async def terminate(self) -> None:
        """Destroy every unit: a created one now, one still being created as soon as its create returns.

        A create can take minutes, such as an ORB request polled for its instance, and the process may be killed.
        """
        self._terminating = True
        for unit in self._units_in(UnitState.pending, UnitState.active):
            if unit.handle is not None:
                self._set_state(unit, UnitState.stopping)
        self._drive_destroys()

        while self._tasks:
            logger.info(f"waiting for {len(self._tasks)} provisioner call(s) in flight")
            await asyncio.wait(set(self._tasks))

    def _on_unit_lost(self, unit: Unit) -> None:
        """Count a unit lost before it was stable toward the crash-loop backoff; a stable unit's loss resets it."""
        if self._is_stable(unit, time.monotonic()):
            self._consecutive_unit_losses = 0
            return

        self._consecutive_unit_losses += 1
        doublings = min(self._consecutive_unit_losses - 1, WORKER_MANAGER_MAX_RESTART_BACKOFF_DOUBLINGS)
        backoff_seconds = self._restart_backoff_seconds * 2**doublings
        self._create_not_before = time.monotonic() + backoff_seconds
        logger.warning(f"{self._consecutive_unit_losses} consecutive unit loss(es): no create for {backoff_seconds}s")

    def _sweep_silent_units(self) -> None:
        """Destroy each unit that exists but sent no heartbeat in time, and count it as lost."""
        now = time.monotonic()
        for unit in self._units_in(UnitState.pending, UnitState.active):
            if unit.handle is None or not self._is_silent(unit, now):
                continue

            logger.warning(
                f"unit {unit.unit_id!r} sent no heartbeat in time from state {unit.state.name}, destroying it"
            )
            self._on_unit_lost(unit)
            self._set_state(unit, UnitState.stopping)

    def _is_stable(self, unit: Unit, now: float) -> bool:
        return unit.state == UnitState.active and now - unit.state_since > WORKER_MANAGER_UNIT_STABLE_SECONDS

    def _is_silent(self, unit: Unit, now: float) -> bool:
        if unit.last_heartbeat is None:
            return now - unit.state_since > self._provisioner.startup_timeout_seconds()
        return now - unit.last_heartbeat > self._unit_timeout_seconds

    def _reconcile(self) -> None:
        """Create or destroy units until the supply matches the desired task concurrency.

        Stopping and destroying units are not supply: counting them would destroy one more unit at every routine
        while a slow destroy runs.
        """
        per_unit = self._provisioner.task_concurrency_per_unit()
        desired_units = math.ceil(self._desired_task_concurrency / per_unit)
        if self._provisioner.max_units() != -1:
            desired_units = min(desired_units, self._provisioner.max_units())

        supply = len(self._units_in(UnitState.pending, UnitState.active))
        if supply <= desired_units:
            self._scale_down_cooldown.reset()
            for _ in range(desired_units - supply):
                self._dispatch_create()
            return

        # Anchors to the first scale-down request in the streak, so a scale-down that keeps being asked for is
        # honored once the cooldown elapses, even if the requested count changes meanwhile.
        self._scale_down_cooldown.start_if_not_running()
        if self._scale_down_cooldown.remaining_seconds() is not None:
            return

        self._scale_down_cooldown.reset()
        for unit in self._select_units_to_shed(supply - desired_units):
            logger.info(f"shedding unit {unit.unit_id!r} (assigned_tasks={unit.assigned_tasks})")
            unit.stop_context = self._desired_change_context
            self._set_state(unit, UnitState.stopping)

    def _dispatch_create(self) -> None:
        """Add a pending unit and create it in the background, unless the crash-loop backoff is active."""
        if time.monotonic() < self._create_not_before:
            return

        unit = Unit(unit_id=uuid.uuid4().hex, handle=None, state=UnitState.pending, state_since=time.monotonic())
        self._units[unit.unit_id] = unit
        logger.info(f"creating unit {unit.unit_id!r}")
        span = tracer.start_span(
            WORKER_MANAGER_CREATE_UNIT_SPAN,
            context=self._desired_change_context,
            attributes={UNIT_ID_ATTRIBUTE: unit.unit_id},
        )
        # the task copies the current context, so create_unit runs under the span and passes it to the unit
        with trace.use_span(span, end_on_exit=False):
            self._start_task(
                self._provisioner.create_unit(unit.unit_id), functools.partial(self._on_create_done, unit, span)
            )

    def _on_create_done(self, unit: Unit, span: Span, task: asyncio.Task) -> None:
        error = _failure_of(task)
        _end_span(span, error)

        if error is not None:
            logger.error(f"failed to create unit {unit.unit_id!r}: {error!r}")
            self._units.pop(unit.unit_id, None)
            self._on_unit_lost(unit)
            return

        unit.handle = task.result()
        if self._terminating:
            self._set_state(unit, UnitState.stopping)
            self._drive_destroys()

    def _drive_destroys(self) -> None:
        """Destroy each stopping unit, and retry each destroy that failed."""
        for unit in self._units_in(UnitState.stopping):
            assert unit.handle is not None, "only a created unit can stop"
            self._set_state(unit, UnitState.destroying)
            span = tracer.start_span(
                WORKER_MANAGER_DESTROY_UNIT_SPAN,
                context=unit.stop_context or Context(),
                attributes={UNIT_ID_ATTRIBUTE: unit.unit_id},
            )
            with trace.use_span(span, end_on_exit=False):
                self._start_task(
                    self._provisioner.destroy_unit(unit.handle), functools.partial(self._on_destroy_done, unit, span)
                )

    def _on_destroy_done(self, unit: Unit, span: Span, task: asyncio.Task) -> None:
        error = _failure_of(task)
        _end_span(span, error)

        if error is not None and self._terminating:
            logger.error(f"failed to destroy unit {unit.unit_id!r}: {error!r}")
            self._units.pop(unit.unit_id, None)
            return
        if error is not None:
            logger.error(f"failed to destroy unit {unit.unit_id!r}, retrying: {error!r}")
            self._set_state(unit, UnitState.stopping)
            return

        self._units.pop(unit.unit_id, None)
        logger.info(f"destroyed unit {unit.unit_id!r}")

    def _start_task(self, call: Awaitable[Any], on_done: Callable[[asyncio.Task], None]) -> None:
        task = asyncio.ensure_future(call)
        self._tasks.add(task)
        task.add_done_callback(functools.partial(self._on_task_done, on_done))

    def _on_task_done(self, on_done: Callable[[asyncio.Task], None], task: asyncio.Task) -> None:
        """Run `on_done` before forgetting `task`, so a call it starts is in flight before this one leaves."""
        try:
            on_done(task)
        finally:
            self._tasks.discard(task)

    def _select_units_to_shed(self, count: int) -> List[Unit]:
        """The `count` active units with the fewest assigned tasks.

        Emptying whole units, rather than one worker from each, is what releases a resource that costs money.
        """
        least_loaded_first = sorted(
            self._units_in(UnitState.active), key=lambda unit: (unit.assigned_tasks, unit.state_since)
        )
        return least_loaded_first[:count]

    def _set_state(self, unit: Unit, state: UnitState) -> None:
        unit.state = state
        unit.state_since = time.monotonic()

    def _units_in(self, *states: UnitState) -> List[Unit]:
        """The units whose state is one of `states`, in creation order."""
        return [unit for unit in self._units.values() if unit.state in states]


def _failure_of(task: asyncio.Task) -> Optional[BaseException]:
    if task.cancelled():
        return asyncio.CancelledError()
    return task.exception()


def _end_span(span: Span, error: Optional[BaseException]) -> None:
    if error is not None:
        span.record_exception(error)
        span.set_status(Status(StatusCode.ERROR, type(error).__name__))
    span.end()
