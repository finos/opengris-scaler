from __future__ import annotations

import asyncio
import functools
import logging
import time
import uuid
from math import ceil
from typing import Dict, List

from scaler.utility.mixins import Looper, Reporter
from scaler.worker_manager_adapter.unit import Unit, UnitState
from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS, UnitProvisioner

logger = logging.getLogger(__name__)

MAX_RESTART_BACKOFF_SECONDS = 60.0
DRAIN_POLL_SECONDS = 0.05


class UnitController(Looper, Reporter):
    """Sole owner of the fleet state.

    Every change to a unit arrives here, and that single ownership is what serialises them, so the
    loops do not need merging.
    """

    def __init__(
        self, provisioner: UnitProvisioner, drain_timeout_seconds: float, restart_backoff_seconds: float
    ) -> None:
        self._provisioner = provisioner
        self._drain_timeout_seconds = drain_timeout_seconds
        self._restart_backoff_seconds = restart_backoff_seconds

        self._units: Dict[str, Unit] = {}
        self._desired_task_concurrency = 0
        self._consecutive_unexpected_losses = 0
        self._create_not_before = 0.0
        self._draining_all = False

    def set_desired_task_concurrency(self, count: int) -> None:
        if count == self._desired_task_concurrency:
            return

        logger.info(f"desired task concurrency changed: {self._desired_task_concurrency} -> {count}")
        self._desired_task_concurrency = count

    def on_unit_report(self, unit_id: str, active_task_concurrency: int, occupancy: int) -> None:
        unit = self._units.get(unit_id)
        if unit is None:
            return

        unit.active_task_concurrency = active_task_concurrency
        unit.occupancy = occupancy

        if unit.state is UnitState.pending:
            self._transition(unit, UnitState.active)
            self._consecutive_unexpected_losses = 0

    def on_unit_gone(self, unit_id: str) -> None:
        """A unit has reported that it has finished draining and is about to disappear."""
        unit = self._units.get(unit_id)
        if unit is None or unit.state in (UnitState.stopping, UnitState.gone):
            return

        self._dispatch_destroy(unit)

    async def routine(self) -> None:
        await self._reap()
        await self._sweep_drains()

        if not self._draining_all:
            await self._reconcile()

    async def drain_all(self) -> None:
        """Drain every unit and return once the last one is gone, or once the deadline passes.

        This needs the binder loop to still be running, because a drain is a message to each unit.
        Use it for a shutdown asked for by a parent, not for one triggered by a signal: by the time
        the loops have stopped, nothing can deliver the command and nothing will report back.
        """
        self._draining_all = True
        self._desired_task_concurrency = 0

        for unit in list(self._units.values()):
            if unit.is_supply():
                await self._begin_drain(unit)

        deadline = time.time() + self._drain_timeout_seconds
        while not self._all_gone():
            if time.time() > deadline:
                logger.warning("fleet drain timed out, forcing the rest down")
                await self.destroy_all()
                return

            await self._reap()
            await self._sweep_drains()
            if self._all_gone():
                return
            await asyncio.sleep(DRAIN_POLL_SECONDS)

    async def destroy_all(self) -> None:
        """Tear every unit down now, without waiting for it to finish what it holds.

        This is the signal path. The manager is going away immediately, so there is no chance to
        keep the work, and no working link over which to ask for one.
        """
        self._draining_all = True
        self._desired_task_concurrency = 0

        for unit in list(self._units.values()):
            if unit.state is UnitState.gone:
                continue
            self._transition(unit, UnitState.stopping)
            try:
                await self._provisioner.destroy_unit(unit.unit_id)
            except Exception:
                logger.exception(f"unit {unit.unit_id!r} could not be destroyed")
            finally:
                self._transition(unit, UnitState.gone)

    def _all_gone(self) -> bool:
        return all(unit.state is UnitState.gone for unit in self._units.values())

    def get_status(self) -> Dict[str, int]:
        counts = {state.name: 0 for state in UnitState}
        for unit in self._units.values():
            counts[unit.state.name] += 1

        counts["desired_task_concurrency"] = self._desired_task_concurrency
        counts["active_task_concurrency"] = sum(
            unit.active_task_concurrency for unit in self._units.values() if unit.is_supply()
        )
        counts["occupancy"] = sum(unit.occupancy for unit in self._units.values() if unit.is_supply())
        return counts

    async def _reap(self) -> None:
        """Units that vanished move to gone. An unexpected loss arms the crash-loop backoff."""
        alive = await self._provisioner.poll_units()

        for unit in list(self._units.values()):
            if unit.state is UnitState.gone:
                continue

            if unit.unit_id in alive:
                if unit.state is UnitState.pending:
                    # The unit exists, which is all "active" claims. Occupancy still comes from the
                    # unit's own reports; this only says the create succeeded.
                    self._transition(unit, UnitState.active)
                    self._consecutive_unexpected_losses = 0
                continue

            if unit.state is UnitState.pending:
                continue  # the create call is still open, so absence means nothing yet

            if unit.state is UnitState.stopping:
                self._transition(unit, UnitState.gone)
                continue

            logger.warning(f"unit {unit.unit_id!r} vanished unexpectedly from state {unit.state.name}")
            self._transition(unit, UnitState.gone)
            self._consecutive_unexpected_losses += 1
            self._create_not_before = time.time() + self._backoff_seconds()

    async def _sweep_drains(self) -> None:
        now = time.time()
        for unit in list(self._units.values()):
            if unit.state is not UnitState.draining:
                continue
            if unit.drain_deadline is None or now <= unit.drain_deadline:
                continue

            logger.warning(f"unit {unit.unit_id!r} drain timed out, forcing teardown")
            self._dispatch_destroy(unit)

    async def _reconcile(self) -> None:
        per_unit = self._provisioner.task_concurrency_per_unit()
        desired_units = ceil(self._desired_task_concurrency / per_unit) if per_unit > 0 else 0

        max_units = self._provisioner.max_units()
        if max_units != UNLIMITED_UNITS:
            desired_units = min(desired_units, max_units)

        supply = [unit for unit in self._units.values() if unit.is_supply()]
        delta = desired_units - len(supply)

        for _ in range(max(0, delta)):
            self._dispatch_create()

        for unit in self._select_units_to_shed(-min(0, delta)):
            await self._begin_drain(unit)

        await self._assign_unit_targets()

    def _select_units_to_shed(self, count: int) -> List[Unit]:
        """Empty the least occupied units completely, and keep the rest full.

        Concentrating the load is what makes a scale-down save money. A unit is a whole resource,
        and a half empty instance costs the same as a full one, so spreading the shrink evenly
        would release nothing. The order within that rule matters much less: this view is one
        report behind the scheduler's and misses assignments in flight, so the choice is sometimes
        wrong, and a wrong choice costs the runtime of one task.
        """
        if count <= 0:
            return []

        candidates = [unit for unit in self._units.values() if unit.state is UnitState.active]
        return sorted(candidates, key=lambda unit: (unit.occupancy, unit.state_since))[:count]

    async def _assign_unit_targets(self) -> None:
        """Fill each surviving unit to capacity except the last, which takes the remainder.

        Without a partial fill the manager over-supplies by as much as one unit. This does nothing
        where a unit's task concurrency is fixed.
        """
        per_unit = self._provisioner.task_concurrency_per_unit()
        remaining = self._desired_task_concurrency

        for unit in self._serving_units():
            target = min(per_unit, remaining)
            remaining -= target

            if target == unit.desired_task_concurrency:
                continue

            unit.desired_task_concurrency = target
            await self._provisioner.set_unit_task_concurrency(unit.unit_id, target)

    def _serving_units(self) -> List[Unit]:
        return sorted((unit for unit in self._units.values() if unit.is_supply()), key=lambda unit: unit.state_since)

    async def _begin_drain(self, unit: Unit) -> None:
        if unit.state not in (UnitState.pending, UnitState.active):
            return

        self._transition(unit, UnitState.draining)
        unit.drain_deadline = time.time() + self._drain_timeout_seconds

        try:
            await self._provisioner.shutdown_unit(unit.unit_id)
        except Exception:
            logger.exception(f"unit {unit.unit_id!r} could not be told to drain, tearing it down")
            self._dispatch_destroy(unit)

    def _dispatch_create(self) -> None:
        """Start a unit without blocking the routine.

        A RunInstances call takes seconds. Waiting for it here would stop the liveness checks, and
        the manager would then time out its own children.
        """
        if time.time() < self._create_not_before:
            return

        unit = Unit(unit_id=self._placeholder_id(), state=UnitState.pending, state_since=time.time())
        self._units[unit.unit_id] = unit

        task = asyncio.create_task(self._provisioner.create_unit())
        task.add_done_callback(functools.partial(self._on_create_done, unit.unit_id))

    def _on_create_done(self, placeholder_id: str, task: asyncio.Task) -> None:
        unit = self._units.pop(placeholder_id, None)
        if unit is None:
            return

        try:
            unit_id = task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("unit creation failed")
            self._transition(unit, UnitState.gone)
            self._units[placeholder_id] = unit
            self._consecutive_unexpected_losses += 1
            self._create_not_before = time.time() + self._backoff_seconds()
            return

        unit.unit_id = unit_id
        self._units[unit_id] = unit

    def _dispatch_destroy(self, unit: Unit) -> None:
        self._transition(unit, UnitState.stopping)
        asyncio.create_task(self._destroy(unit))

    async def _destroy(self, unit: Unit) -> None:
        try:
            await self._provisioner.destroy_unit(unit.unit_id)
        except Exception:
            logger.exception(f"unit {unit.unit_id!r} could not be destroyed")
        finally:
            self._transition(unit, UnitState.gone)

    def _transition(self, unit: Unit, state: UnitState) -> None:
        if unit.state is state:
            return

        unit.state = state
        unit.state_since = time.time()

    def _backoff_seconds(self) -> float:
        return min(
            self._restart_backoff_seconds * (2 ** (self._consecutive_unexpected_losses - 1)),
            MAX_RESTART_BACKOFF_SECONDS,
        )

    @staticmethod
    def _placeholder_id() -> str:
        """Stands in until the real id arrives, so a pending unit still counts as supply."""
        return f"pending-{uuid.uuid4().hex}"
