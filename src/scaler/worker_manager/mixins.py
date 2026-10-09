from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Hashable

UnitHandle = Hashable  # opaque to the controller: each provisioner picks its own type


class UnitProvisioner(ABC):
    """The mechanics of one kind of unit: the resource a worker manager creates and destroys as one act.

    A provisioner is stateless. It keeps no record of its units: the unit controller stores each handle and passes it
    back.
    """

    @abstractmethod
    async def create_unit(self, unit_id: str) -> UnitHandle:
        """Allocate one unit that identifies itself as `unit_id`, and return what destroy needs."""
        ...

    @abstractmethod
    async def destroy_unit(self, handle: UnitHandle) -> None:
        """Release the unit, and return once it is gone."""
        ...

    @abstractmethod
    def max_units(self) -> int:
        """The most units this provisioner may run, -1 for no limit."""
        ...

    @abstractmethod
    def task_concurrency_per_unit(self) -> int: ...

    @abstractmethod
    def startup_timeout_seconds(self) -> int:
        """How long a new unit may take to send its first heartbeat before it counts as lost."""
        ...
