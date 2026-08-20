from abc import ABC, abstractmethod
from typing import Set

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder

UNLIMITED_UNITS = -1


class UnitProvisioner(ABC):
    """The whole of a worker manager backend: the mechanics of one resource, plus three constants.

    A provisioner holds no state about capacity. It never reads a capnp message, never decides how
    many units to run, never sends a drain command and never waits for one to finish. UnitController
    does all of that, identically for every backend.
    """

    @abstractmethod
    def register(self, binder: AsyncBinder, children_address: AddressConfig) -> None:
        """Receive the binder its units dial, and the address they should dial to reach it."""

    @abstractmethod
    async def create_unit(self) -> str:
        """Allocate one unit and return its id. May be slow; the controller does not wait on it."""

    @abstractmethod
    async def destroy_unit(self, unit_id: str) -> None:
        """Release the unit. Called only after the unit has drained, or after its deadline."""

    @abstractmethod
    async def poll_units(self) -> Set[str]:
        """Return the ids of the units that still exist, which is how losses are noticed."""

    @abstractmethod
    async def shutdown_unit(self, unit_id: str) -> None:
        """Ask the unit to drain. WorkerShutdown to a worker, WorkerManagerShutdown to a child."""

    @abstractmethod
    async def set_unit_task_concurrency(self, unit_id: str, task_concurrency: int) -> None:
        """Ask a unit to run this many tasks. A no-op where a unit's concurrency is fixed."""

    @abstractmethod
    def max_units(self) -> int:
        """Upper bound on units, or UNLIMITED_UNITS."""

    @abstractmethod
    def task_concurrency_per_unit(self) -> int:
        """How many task slots one unit supplies."""

    @abstractmethod
    def poll_interval_seconds(self) -> float:
        """A process sentinel check is free; describe-instances is not."""
