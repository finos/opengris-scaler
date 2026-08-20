from __future__ import annotations

import dataclasses
import enum
from typing import Optional


class UnitState(enum.Enum):
    """Where a unit is in its life.

    The two shutdown states are what a two-state model cannot express. A unit that is on its way
    out counts neither as supply nor as a reason to shed another unit. Without them, a drain that
    outlasts one reconcile interval makes the controller shed a second unit, then a third, for as
    long as the drain takes.
    """

    pending = enum.auto()  # create dispatched, not yet confirmed
    active = enum.auto()  # serving
    draining = enum.auto()  # drain sent, still finishing tasks
    stopping = enum.auto()  # drained, teardown dispatched
    gone = enum.auto()  # reaped


SUPPLY_STATES = (UnitState.pending, UnitState.active)


@dataclasses.dataclass
class Unit:
    """One thing a provisioner creates and destroys as a single indivisible act.

    For a native manager a unit is one worker process. For a nested provisioner it is a resource
    running a child worker manager, and this row is a summary of that child's whole fleet.
    """

    unit_id: str
    state: UnitState
    state_since: float

    desired_task_concurrency: int = 0  # target this manager has asked the unit for
    active_task_concurrency: int = 0  # what the unit reports it is actually running
    occupancy: int = 0  # queued + processing, from the unit's own reports
    drain_deadline: Optional[float] = None

    def is_supply(self) -> bool:
        return self.state in SUPPLY_STATES
