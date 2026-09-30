import asyncio
import dataclasses
import enum
import functools
import logging
import math
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from scaler.utility.cooldown import Cooldown
from scaler.utility.mixins import Looper, Reporter
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner

logger = logging.getLogger(__name__)

# A unit that stays active this long proves the unit recipe works, so the crash-loop backoff starts over.
UNIT_STABLE_SECONDS = 60

# The crash-loop backoff doubles on each consecutive loss, up to this many times.
MAX_RESTART_BACKOFF_DOUBLINGS = 5


class UnitState(enum.Enum):
    pending = enum.auto()  # create dispatched, not yet confirmed to exist
    active = enum.auto()  # serving
    draining = enum.auto()  # told to drain, still finishing its tasks; never serves again
    stopping = enum.auto()  # teardown dispatched


@dataclasses.dataclass
class Unit:
    unit_id: str
    handle: Optional[UnitHandle]  # None until create_unit returns
    state: UnitState
    state_since: float  # time.monotonic()
    desired_task_concurrency: int = 0  # the share of the desired task concurrency this unit is asked for
    active_task_concurrency: int = 0  # task slots the unit reported
    occupancy: int = 0  # queued and running tasks the unit reported
    drain_deadline: Optional[float] = None  # time.monotonic()


@dataclasses.dataclass(frozen=True)
class UnitControllerStatus:
    active_task_concurrency: int
    occupancy: int
    active_units: int
    pending_units: int
    draining_units: int


class UnitController(Looper, Reporter):
    """The only owner of the fleet of a worker manager: creates, supervises, drains, and destroys its units.

    Every provisioner call runs in the background, so a slow cloud API never stops the routine. The controller sends
    no message: the runner answers each unit heartbeat from the state the controller keeps.
    """

    def __init__(
        self,
        provisioner: UnitProvisioner,
        scale_down_cooldown_seconds: float,
        drain_timeout_seconds: float,
        restart_backoff_seconds: float,
    ) -> None:
        self._provisioner = provisioner
        self._scale_down_cooldown = Cooldown(scale_down_cooldown_seconds)
        self._drain_timeout_seconds = drain_timeout_seconds
        self._restart_backoff_seconds = restart_backoff_seconds
        self._shutting_down = False

        self._desired_task_concurrency = 0
        self._units: Dict[str, Unit] = {}  # in creation order

        # The event loop keeps only a weak reference to a task: without this set, a provisioner call could be
        # collected mid-flight and leak the resource it was creating.
        self._tasks: Set[asyncio.Task] = set()
        self._destroying_unit_ids: Set[str] = set()

        self._consecutive_unit_losses = 0
        self._create_not_before = 0.0  # time.monotonic()

    def set_desired_task_concurrency(self, task_concurrency: int) -> None:
        if task_concurrency != self._desired_task_concurrency:
            logger.info(f"desired task concurrency changed: {self._desired_task_concurrency} -> {task_concurrency}")
        self._desired_task_concurrency = task_concurrency

    def on_unit_report(self, unit_id: str, active_task_concurrency: int, occupancy: int) -> None:
        unit = self._units.get(unit_id)
        if unit is None:
            return

        unit.active_task_concurrency = active_task_concurrency
        unit.occupancy = occupancy

    def on_unit_disconnect(self, unit_id: str) -> None:
        """The unit drained its fleet and is about to exit: destroy the resource it runs on now."""
        unit = self._units.get(unit_id)
        if unit is None or unit.state == UnitState.stopping or unit.handle is None:
            return

        logger.info(f"unit {unit_id!r} reported its fleet gone")
        self._set_state(unit, UnitState.stopping)

    def is_unit_serving(self, unit_id: str) -> bool:
        """False for a unit that must drain: one told to, or one this controller does not know."""
        unit = self._units.get(unit_id)
        return unit is not None and unit.state in (UnitState.pending, UnitState.active)

    def get_unit_desired_task_concurrency(self, unit_id: str) -> int:
        unit = self._units.get(unit_id)
        return unit.desired_task_concurrency if unit is not None else 0

    def begin_shutdown(self) -> None:
        """Drain every unit, create no more, and ignore new desired counts. Calling it again does nothing."""
        if self._shutting_down:
            return

        logger.info(f"shutting down: draining {len(self._units)} unit(s)")
        self._shutting_down = True

    def is_shut_down(self) -> bool:
        """True once shutdown has begun, no unit remains, and no provisioner call is in flight."""
        return self._shutting_down and not self._units and not self._tasks

    def get_status(self) -> UnitControllerStatus:
        active = self._units_in(UnitState.active)
        return UnitControllerStatus(
            active_task_concurrency=sum(unit.active_task_concurrency for unit in active),
            occupancy=sum(unit.occupancy for unit in self._units.values()),
            active_units=len(active),
            pending_units=len(self._units_in(UnitState.pending)),
            draining_units=len(self._units_in(UnitState.draining)),
        )

    async def routine(self) -> None:
        await self._reap()
        self._sweep_drains()
        self._reconcile()
        self._drive_destroys()

    async def terminate(self) -> None:
        """Destroy every unit at once, without a drain. Waits for the provisioner calls in flight first, so a
        create that finishes now is destroyed too."""
        if self._tasks:
            logger.info(f"waiting for {len(self._tasks)} provisioner call(s) in flight")
            await asyncio.gather(*self._tasks, return_exceptions=True)

        units = [unit for unit in self._units.values() if unit.handle is not None]
        results = await asyncio.gather(
            *(self._provisioner.destroy_unit(unit.handle) for unit in units), return_exceptions=True
        )
        for unit, result in zip(units, results):
            if isinstance(result, BaseException):
                logger.error(f"failed to destroy unit {unit.unit_id!r}: {result!r}")
            else:
                logger.info(f"destroyed unit {unit.unit_id!r}")
        self._units.clear()

    async def _reap(self) -> None:
        """Promote each created unit that exists, and remove each one that vanished."""
        created = [unit for unit in self._units.values() if unit.handle is not None]
        alive = await self._provisioner.poll_units({unit.handle for unit in created})

        now = time.monotonic()
        for unit in created:
            if unit.handle in alive:
                self._on_unit_alive(unit, now)
                continue

            # the exit of a unit that drains or stops is the report that it finished
            if self._units.pop(unit.unit_id, None) is None or unit.state in (UnitState.draining, UnitState.stopping):
                continue

            logger.warning(f"unit {unit.unit_id!r} vanished unexpectedly from state {unit.state.name}")
            self._on_unit_lost()

    def _on_unit_alive(self, unit: Unit, now: float) -> None:
        if unit.state == UnitState.pending:
            logger.info(f"unit {unit.unit_id!r} is active")
            self._set_state(unit, UnitState.active)
            return

        if unit.state == UnitState.active and now - unit.state_since > UNIT_STABLE_SECONDS:
            self._consecutive_unit_losses = 0

    def _on_unit_lost(self) -> None:
        self._consecutive_unit_losses += 1
        doublings = min(self._consecutive_unit_losses - 1, MAX_RESTART_BACKOFF_DOUBLINGS)
        backoff_seconds = self._restart_backoff_seconds * 2**doublings
        self._create_not_before = time.monotonic() + backoff_seconds
        logger.warning(f"{self._consecutive_unit_losses} consecutive unit loss(es): no create for {backoff_seconds}s")

    def _sweep_drains(self) -> None:
        """Destroy each unit whose drain passed its deadline."""
        now = time.monotonic()
        for unit in self._units_in(UnitState.draining):
            assert unit.drain_deadline is not None, "a draining unit always has a deadline"
            if now > unit.drain_deadline:
                logger.warning(f"unit {unit.unit_id!r} did not drain in {self._drain_timeout_seconds}s, destroying it")
                self._set_state(unit, UnitState.stopping)

    def _reconcile(self) -> None:
        """Create or drain units until the supply matches the desired task concurrency.

        Draining and stopping units are not supply: counting them would drain one more unit at every routine while
        a long drain runs.
        """
        per_unit = self._provisioner.task_concurrency_per_unit()
        desired_units = math.ceil(self._desired_task_concurrency / per_unit)
        if self._shutting_down:
            desired_units = 0
        elif self._provisioner.max_units() != -1:
            desired_units = min(desired_units, self._provisioner.max_units())

        self._assign_unit_targets()

        supply = len(self._units_in(UnitState.pending, UnitState.active))
        if supply <= desired_units:
            self._scale_down_cooldown.reset()
            for _ in range(desired_units - supply):
                self._dispatch_create()
            return

        # Anchors to the first scale-down request in the streak, so a scale-down that keeps being asked for is
        # honored once the cooldown elapses, even if the requested count changes meanwhile. A shutdown does not wait.
        self._scale_down_cooldown.start_if_not_running()
        if self._scale_down_cooldown.remaining_seconds() is not None and not self._shutting_down:
            return

        self._scale_down_cooldown.reset()
        for unit in self._select_units_to_shed(supply - desired_units):
            self._begin_drain(unit)

    def _assign_unit_targets(self) -> None:
        """Fill each active unit to capacity except the last, which takes the remainder.

        Only a unit that runs a child manager reads its target: the task concurrency of a local process is fixed.
        """
        per_unit = self._provisioner.task_concurrency_per_unit()
        remaining = 0 if self._shutting_down else self._desired_task_concurrency
        for unit in self._units_in(UnitState.active):
            unit.desired_task_concurrency = min(per_unit, remaining)
            remaining -= unit.desired_task_concurrency

    def _begin_drain(self, unit: Unit) -> None:
        logger.info(f"draining unit {unit.unit_id!r} (occupancy={unit.occupancy})")
        self._set_state(unit, UnitState.draining)
        unit.drain_deadline = unit.state_since + self._drain_timeout_seconds

    def _dispatch_create(self) -> None:
        """Add a pending unit and create it in the background, unless the crash-loop backoff is active."""
        if self._shutting_down or time.monotonic() < self._create_not_before:
            return

        unit = Unit(unit_id=uuid.uuid4().hex, handle=None, state=UnitState.pending, state_since=time.monotonic())
        self._units[unit.unit_id] = unit
        logger.info(f"creating unit {unit.unit_id!r}")
        self._start_task(self._provisioner.create_unit(unit.unit_id), functools.partial(self._on_create_done, unit))

    def _on_create_done(self, unit: Unit, task: asyncio.Task) -> None:
        error = _failure_of(task)
        if error is not None:
            logger.error(f"failed to create unit {unit.unit_id!r}: {error!r}")
            self._units.pop(unit.unit_id, None)
            self._on_unit_lost()
            return

        unit.handle = task.result()

    def _drive_destroys(self) -> None:
        """Destroy each stopping unit, and retry each destroy that failed."""
        for unit in self._units_in(UnitState.stopping):
            if unit.unit_id in self._destroying_unit_ids:
                continue

            assert unit.handle is not None, "only a created unit can stop"
            self._destroying_unit_ids.add(unit.unit_id)
            self._start_task(
                self._provisioner.destroy_unit(unit.handle), functools.partial(self._on_destroy_done, unit)
            )

    def _on_destroy_done(self, unit: Unit, task: asyncio.Task) -> None:
        self._destroying_unit_ids.discard(unit.unit_id)
        error = _failure_of(task)
        if error is not None:
            logger.error(f"failed to destroy unit {unit.unit_id!r}, retrying: {error!r}")
            return

        self._units.pop(unit.unit_id, None)
        logger.info(f"destroyed unit {unit.unit_id!r}")

    def _start_task(self, call: Awaitable[Any], on_done: Callable[[asyncio.Task], None]) -> None:
        task = asyncio.ensure_future(call)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(on_done)

    def _select_units_to_shed(self, count: int) -> List[Unit]:
        """The `count` active units with the lowest occupancy.

        Emptying whole units, rather than one worker from each, is what releases a resource that costs money.
        """
        return sorted(self._units_in(UnitState.active), key=lambda unit: (unit.occupancy, unit.state_since))[:count]

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
