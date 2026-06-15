from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional


class CapacityCoordinator:
    """Manages async scale-up/down reconciliation for a pool of homogeneous units.

    Callers set a desired unit count via `set_desired_unit_count`. The loop
    compares that against the live count returned by `active_unit_count` and
    calls `start_units` or `stop_units` with the delta. A single long-lived task
    blocks on an asyncio.Event between reconciles so it only wakes when a new
    desired count has been signalled; rapid successive calls are coalesced because
    the task always reads the latest desired count when it wakes.

    Call `cancel()` to stop the reconcile task on shutdown.

    Args:
        start_units: Async callable that launches `n` new units.
        stop_units: Async callable that terminates `n` existing units.
        active_unit_count: Callable that returns the current live unit count.
        max_unit_count: Hard cap on the number of units. -1 means unlimited.
    """

    def __init__(
        self,
        start_units: Callable[[int], Awaitable[None]],
        stop_units: Callable[[int], Awaitable[None]],
        active_unit_count: Callable[[], int],
        max_unit_count: int,
    ) -> None:
        self._start_units = start_units
        self._stop_units = stop_units
        self._active_unit_count = active_unit_count
        self._max_unit_count = max_unit_count
        self._desired_unit_count: int = 0
        self._active_reconcile_task: Optional[asyncio.Task] = None
        self._reconcile_needed: asyncio.Event = asyncio.Event()
        self._stop: asyncio.Event = asyncio.Event()

    @property
    def desired_unit_count(self) -> int:
        return self._desired_unit_count

    async def set_desired_unit_count(self, count: int) -> None:
        """Set the desired number of units and signal the reconcile task."""
        if count == self._desired_unit_count:
            return
        logging.info(f"Desired unit count changed: {self._desired_unit_count} -> {count}")
        self._desired_unit_count = count
        self._reconcile_needed.set()
        if self._active_reconcile_task is None:
            self._active_reconcile_task = asyncio.create_task(self._reconcile())

    def cancel(self) -> None:
        """Stop the reconcile task. Safe to call multiple times."""
        self._stop.set()
        self._reconcile_needed.set()  # unblock any waiting
        if self._active_reconcile_task is not None:
            self._active_reconcile_task.cancel()

    def __del__(self) -> None:
        self.cancel()

    async def _reconcile(self) -> None:
        try:
            while not self._stop.is_set():
                await self._reconcile_needed.wait()
                self._reconcile_needed.clear()
                if self._stop.is_set():
                    break

                desired = self._desired_unit_count
                current = self._active_unit_count()
                delta = desired - current
                if self._max_unit_count != -1:
                    delta = min(delta, self._max_unit_count - current)
                capped = self._max_unit_count != -1 and delta != desired - current
                msg = f"Reconcile: desired={desired}, current={current}, delta={delta:+d}" + (
                    f" (capped by max_unit_count={self._max_unit_count})" if capped else ""
                )
                if delta != 0:
                    logging.info(msg)
                else:
                    logging.debug(msg)
                try:
                    if delta > 0:
                        await self._start_units(delta)
                    elif delta < 0:
                        await self._stop_units(abs(delta))
                except Exception:
                    logging.exception("Reconcile failed")
        finally:
            self._active_reconcile_task = None
