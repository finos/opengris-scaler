from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


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

    async def set_desired_unit_count(self, count: int) -> None:
        """Set the desired number of units and signal the reconcile task."""
        current_active = self._active_unit_count()
        print(
            f"[CAPACITY-COORD][set_desired_unit_count] ts={time.time():.3f} "
            f"new_desired={count} prev_desired={self._desired_unit_count} "
            f"active_units={current_active} max_units={self._max_unit_count} "
            f"reconcile_task_running={self._active_reconcile_task is not None and not self._active_reconcile_task.done()}",
            flush=True,
        )
        if count == self._desired_unit_count:
            print(
                f"[CAPACITY-COORD][set_desired_unit_count]   no change (desired already={count}), skipping",
                flush=True,
            )
            return
        print(
            f"[CAPACITY-COORD][set_desired_unit_count]   desired changed: {self._desired_unit_count} -> {count} "
            f"(delta from active={count - current_active:+d})",
            flush=True,
        )
        logger.info(f"Desired unit count changed: {self._desired_unit_count} -> {count}")
        self._desired_unit_count = count
        self._reconcile_needed.set()
        if self._active_reconcile_task is None or self._active_reconcile_task.done():
            print(
                f"[CAPACITY-COORD][set_desired_unit_count]   spawning new reconcile task",
                flush=True,
            )
            self._active_reconcile_task = asyncio.create_task(self._reconcile())
        else:
            print(
                f"[CAPACITY-COORD][set_desired_unit_count]   reconcile task already running, signalled via event",
                flush=True,
            )

    def cancel(self) -> None:
        """Stop the reconcile task. Safe to call multiple times."""
        self._stop.set()
        self._reconcile_needed.set()  # unblock any waiting
        if self._active_reconcile_task is not None:
            self._active_reconcile_task.cancel()

    def __del__(self) -> None:
        self.cancel()

    async def _reconcile(self) -> None:
        print(
            f"[CAPACITY-COORD][_reconcile] ts={time.time():.3f} reconcile task started",
            flush=True,
        )
        try:
            while not self._stop.is_set():
                print(
                    f"[CAPACITY-COORD][_reconcile]   waiting for reconcile_needed event ...",
                    flush=True,
                )
                await self._reconcile_needed.wait()
                self._reconcile_needed.clear()
                if self._stop.is_set():
                    print(
                        f"[CAPACITY-COORD][_reconcile]   stop event set, exiting reconcile loop",
                        flush=True,
                    )
                    break

                desired = self._desired_unit_count
                current = self._active_unit_count()
                raw_delta = desired - current
                delta = raw_delta
                if self._max_unit_count != -1:
                    delta = min(raw_delta, self._max_unit_count - current)
                capped = self._max_unit_count != -1 and delta != raw_delta

                print(
                    f"[CAPACITY-COORD][_reconcile]   ts={time.time():.3f} "
                    f"desired={desired} current={current} raw_delta={raw_delta:+d} "
                    f"effective_delta={delta:+d} max_unit_count={self._max_unit_count}"
                    + (f" CAPPED (would have been {raw_delta:+d})" if capped else ""),
                    flush=True,
                )

                msg = f"Reconcile: desired={desired}, current={current}, delta={delta:+d}" + (
                    f" (capped by max_unit_count={self._max_unit_count})" if capped else ""
                )
                if delta != 0:
                    logger.info(msg)
                else:
                    logger.debug(msg)

                try:
                    if delta > 0:
                        print(
                            f"[CAPACITY-COORD][_reconcile]   ACTION: start_units({delta}) ...",
                            flush=True,
                        )
                        await self._start_units(delta)
                        print(
                            f"[CAPACITY-COORD][_reconcile]   start_units({delta}) returned. "
                            f"new active_count={self._active_unit_count()}",
                            flush=True,
                        )
                    elif delta < 0:
                        print(
                            f"[CAPACITY-COORD][_reconcile]   ACTION: stop_units({abs(delta)}) ...",
                            flush=True,
                        )
                        await self._stop_units(abs(delta))
                        print(
                            f"[CAPACITY-COORD][_reconcile]   stop_units({abs(delta)}) returned. "
                            f"new active_count={self._active_unit_count()}",
                            flush=True,
                        )
                    else:
                        print(
                            f"[CAPACITY-COORD][_reconcile]   no action needed (delta=0)",
                            flush=True,
                        )
                except Exception:
                    print(
                        f"[CAPACITY-COORD][_reconcile]   EXCEPTION during reconcile action!",
                        flush=True,
                    )
                    logger.exception("Reconcile failed")
        finally:
            print(
                f"[CAPACITY-COORD][_reconcile] ts={time.time():.3f} reconcile task ending",
                flush=True,
            )
            self._active_reconcile_task = None
