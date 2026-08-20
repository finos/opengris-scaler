import asyncio
import unittest
from typing import Dict, List, Optional, Set

from scaler.worker_manager_adapter.unit import UnitState
from scaler.worker_manager_adapter.unit_controller import UnitController
from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS, UnitProvisioner

DRAIN_TIMEOUT_SECONDS = 30.0
BACKOFF_SECONDS = 1.0


class _FakeProvisioner(UnitProvisioner):
    """A provisioner whose units exist only as ids, so the controller can be driven directly."""

    def __init__(self, per_unit: int = 1, max_units: int = UNLIMITED_UNITS) -> None:
        self._per_unit = per_unit
        self._max_units = max_units
        self._next = 0

        self.live: Set[str] = set()
        self.created: List[str] = []
        self.destroyed: List[str] = []
        self.shutdown_called: List[str] = []
        self.targets: Dict[str, int] = {}
        self.create_error: Optional[Exception] = None

    def register(self, binder, children_address) -> None:
        self.binder = binder
        self.children_address = children_address

    async def create_unit(self) -> str:
        if self.create_error is not None:
            raise self.create_error

        self._next += 1
        unit_id = f"unit-{self._next}"
        self.live.add(unit_id)
        self.created.append(unit_id)
        return unit_id

    async def destroy_unit(self, unit_id: str) -> None:
        self.live.discard(unit_id)
        self.destroyed.append(unit_id)

    async def poll_units(self) -> Set[str]:
        return set(self.live)

    async def shutdown_unit(self, unit_id: str) -> None:
        self.shutdown_called.append(unit_id)

    async def set_unit_task_concurrency(self, unit_id: str, task_concurrency: int) -> None:
        self.targets[unit_id] = task_concurrency

    def max_units(self) -> int:
        return self._max_units

    def task_concurrency_per_unit(self) -> int:
        return self._per_unit

    def poll_interval_seconds(self) -> float:
        return 0.0


async def _settle() -> None:
    """Let the create/destroy tasks the controller dispatched actually run."""
    for _ in range(6):
        await asyncio.sleep(0)


class TestUnitControllerScaleUp(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.provisioner = _FakeProvisioner()
        self.controller = UnitController(self.provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)

    async def test_it_creates_the_units_the_target_asks_for(self) -> None:
        self.controller.set_desired_task_concurrency(3)
        await self.controller.routine()
        await _settle()

        self.assertEqual(len(self.provisioner.created), 3)

    async def test_a_created_unit_is_supply_before_it_reports(self) -> None:
        self.controller.set_desired_task_concurrency(2)
        await self.controller.routine()
        await _settle()

        # A second reconcile must not create more: the pending units already count.
        await self.controller.routine()
        await _settle()

        self.assertEqual(len(self.provisioner.created), 2)

    async def test_a_report_promotes_a_pending_unit_to_active(self) -> None:
        self.controller.set_desired_task_concurrency(1)
        await self.controller.routine()
        await _settle()

        unit_id = self.provisioner.created[0]
        self.controller.on_unit_report(unit_id, active_task_concurrency=1, occupancy=0)

        self.assertEqual(self.controller.get_status()[UnitState.active.name], 1)

    async def test_max_units_caps_the_fleet(self) -> None:
        provisioner = _FakeProvisioner(max_units=2)
        controller = UnitController(provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)

        controller.set_desired_task_concurrency(10)
        await controller.routine()
        await _settle()

        self.assertEqual(len(provisioner.created), 2)

    async def test_a_multi_slot_unit_covers_several_task_slots(self) -> None:
        provisioner = _FakeProvisioner(per_unit=4)
        controller = UnitController(provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)

        controller.set_desired_task_concurrency(6)
        await controller.routine()
        await _settle()

        self.assertEqual(len(provisioner.created), 2)  # ceil(6 / 4)


class TestUnitControllerScaleDown(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.provisioner = _FakeProvisioner()
        self.controller = UnitController(self.provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)

        self.controller.set_desired_task_concurrency(3)
        await self.controller.routine()
        await _settle()
        for unit_id in self.provisioner.created:
            self.controller.on_unit_report(unit_id, active_task_concurrency=1, occupancy=0)

    async def test_shedding_drains_rather_than_destroys(self) -> None:
        self.controller.set_desired_task_concurrency(2)
        await self.controller.routine()
        await _settle()

        self.assertEqual(len(self.provisioner.shutdown_called), 1)
        self.assertEqual(self.provisioner.destroyed, [])

    async def test_a_draining_unit_is_not_a_reason_to_shed_another(self) -> None:
        """The reason the state machine has more than two states.

        A drain that outlasts one reconcile interval would otherwise look like missing supply,
        and the controller would shed a second unit, then a third, for the length of the drain.
        """
        self.controller.set_desired_task_concurrency(2)
        for _ in range(5):
            await self.controller.routine()
            await _settle()

        self.assertEqual(len(self.provisioner.shutdown_called), 1)

    async def test_it_sheds_the_least_occupied_unit(self) -> None:
        busy, middling, idle = self.provisioner.created
        self.controller.on_unit_report(busy, active_task_concurrency=1, occupancy=9)
        self.controller.on_unit_report(middling, active_task_concurrency=1, occupancy=4)
        self.controller.on_unit_report(idle, active_task_concurrency=1, occupancy=0)

        self.controller.set_desired_task_concurrency(2)
        await self.controller.routine()
        await _settle()

        self.assertEqual(self.provisioner.shutdown_called, [idle])

    async def test_a_drained_unit_is_destroyed_when_it_reports_gone(self) -> None:
        self.controller.set_desired_task_concurrency(2)
        await self.controller.routine()
        await _settle()

        drained = self.provisioner.shutdown_called[0]
        self.controller.on_unit_gone(drained)
        await _settle()

        self.assertEqual(self.provisioner.destroyed, [drained])

    async def test_a_drain_that_overruns_its_deadline_is_forced(self) -> None:
        controller = UnitController(self.provisioner, drain_timeout_seconds=-1.0, restart_backoff_seconds=1.0)
        controller.set_desired_task_concurrency(1)
        await controller.routine()
        await _settle()
        unit_id = self.provisioner.created[-1]
        controller.on_unit_report(unit_id, active_task_concurrency=1, occupancy=0)

        controller.set_desired_task_concurrency(0)
        await controller.routine()
        await _settle()
        await controller.routine()  # deadline already behind us
        await _settle()

        self.assertIn(unit_id, self.provisioner.destroyed)


class TestUnitControllerSupervision(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.provisioner = _FakeProvisioner()
        self.controller = UnitController(self.provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)

    async def test_a_unit_that_vanishes_is_replaced(self) -> None:
        self.controller.set_desired_task_concurrency(1)
        await self.controller.routine()
        await _settle()
        unit_id = self.provisioner.created[0]
        self.controller.on_unit_report(unit_id, active_task_concurrency=1, occupancy=0)

        self.provisioner.live.discard(unit_id)  # the unit dies on its own
        self.controller._create_not_before = 0.0  # skip the crash-loop backoff for the assertion
        await self.controller.routine()
        await _settle()
        self.controller._create_not_before = 0.0
        await self.controller.routine()
        await _settle()

        self.assertEqual(len(self.provisioner.created), 2)

    async def test_an_unexpected_loss_arms_the_backoff(self) -> None:
        self.controller.set_desired_task_concurrency(1)
        await self.controller.routine()
        await _settle()
        unit_id = self.provisioner.created[0]
        self.controller.on_unit_report(unit_id, active_task_concurrency=1, occupancy=0)

        self.provisioner.live.discard(unit_id)
        await self.controller.routine()
        await _settle()

        self.assertGreater(self.controller._create_not_before, 0.0)

    async def test_a_failed_creation_does_not_leave_phantom_supply(self) -> None:
        self.provisioner.create_error = RuntimeError("no capacity")

        self.controller.set_desired_task_concurrency(1)
        await self.controller.routine()
        await _settle()

        status = self.controller.get_status()
        self.assertEqual(status[UnitState.pending.name], 0)
        self.assertEqual(status[UnitState.active.name], 0)
        self.assertEqual(status[UnitState.gone.name], 1)

    async def test_drain_all_empties_the_fleet(self) -> None:
        self.controller.set_desired_task_concurrency(2)
        await self.controller.routine()
        await _settle()
        for unit_id in self.provisioner.created:
            self.controller.on_unit_report(unit_id, active_task_concurrency=1, occupancy=0)
            self.provisioner.live.discard(unit_id)  # they exit promptly once told

        await asyncio.wait_for(self.controller.drain_all(), timeout=5)

        self.assertEqual(len(self.provisioner.shutdown_called), 2)
        self.assertEqual(self.controller.get_status()[UnitState.gone.name], 2)


class TestUnitControllerTargets(unittest.IsolatedAsyncioTestCase):
    async def test_it_fills_units_and_leaves_the_remainder_to_the_last(self) -> None:
        provisioner = _FakeProvisioner(per_unit=4)
        controller = UnitController(provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)

        controller.set_desired_task_concurrency(6)
        await controller.routine()
        await _settle()
        for unit_id in provisioner.created:
            controller.on_unit_report(unit_id, active_task_concurrency=0, occupancy=0)
        await controller.routine()
        await _settle()

        self.assertEqual(sorted(provisioner.targets.values()), [2, 4])


if __name__ == "__main__":
    unittest.main()


class TestUnitControllerPromotion(unittest.IsolatedAsyncioTestCase):
    """For a provisioner whose units are processes, existing is the only report there is."""

    async def asyncSetUp(self) -> None:
        self.provisioner = _FakeProvisioner()
        self.controller = UnitController(self.provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)

    async def test_a_polled_unit_becomes_active_without_a_report(self) -> None:
        self.controller.set_desired_task_concurrency(1)
        await self.controller.routine()
        await _settle()
        await self.controller.routine()  # the poll now sees it

        self.assertEqual(self.controller.get_status()[UnitState.active.name], 1)

    async def test_promotion_makes_the_unit_shed_able(self) -> None:
        self.controller.set_desired_task_concurrency(2)
        await self.controller.routine()
        await _settle()
        await self.controller.routine()
        await _settle()

        self.controller.set_desired_task_concurrency(1)
        await self.controller.routine()
        await _settle()

        self.assertEqual(len(self.provisioner.shutdown_called), 1)

    async def test_a_unit_still_being_created_is_not_treated_as_lost(self) -> None:
        self.controller.set_desired_task_concurrency(1)
        await self.controller.routine()  # create dispatched, not yet awaited

        await self.controller.routine()  # poll returns nothing yet

        self.assertEqual(self.controller.get_status()[UnitState.gone.name], 0)


class TestUnitControllerShutdownPaths(unittest.IsolatedAsyncioTestCase):
    """The two ways a fleet ends, and why they are not the same call."""

    async def asyncSetUp(self) -> None:
        self.provisioner = _FakeProvisioner()
        self.controller = UnitController(self.provisioner, DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)
        self.controller.set_desired_task_concurrency(2)
        await self.controller.routine()
        await _settle()
        await self.controller.routine()  # promote to active
        await _settle()

    async def test_destroy_all_does_not_wait_for_units_to_finish(self) -> None:
        """The signal path. No loop is running to carry a drain command or bring a report back."""
        await asyncio.wait_for(self.controller.destroy_all(), timeout=2)

        self.assertEqual(len(self.provisioner.destroyed), 2)
        self.assertEqual(self.provisioner.shutdown_called, [])
        self.assertEqual(self.controller.get_status()[UnitState.gone.name], 2)

    async def test_destroy_all_on_an_empty_fleet_is_safe(self) -> None:
        controller = UnitController(_FakeProvisioner(), DRAIN_TIMEOUT_SECONDS, BACKOFF_SECONDS)
        await asyncio.wait_for(controller.destroy_all(), timeout=2)

    async def test_drain_all_asks_each_unit_to_finish_first(self) -> None:
        for unit_id in list(self.provisioner.live):
            self.provisioner.live.discard(unit_id)  # they exit promptly once told

        await asyncio.wait_for(self.controller.drain_all(), timeout=5)

        self.assertEqual(len(self.provisioner.shutdown_called), 2)

    async def test_drain_all_gives_up_at_its_deadline(self) -> None:
        """A unit that never leaves must not hold the manager open for ever."""
        provisioner = _FakeProvisioner()
        controller = UnitController(provisioner, drain_timeout_seconds=0.2, restart_backoff_seconds=1.0)
        controller.set_desired_task_concurrency(2)
        await controller.routine()
        await _settle()
        await controller.routine()
        await _settle()

        # provisioner.live keeps holding the units, so none of them ever reports gone.
        await asyncio.wait_for(controller.drain_all(), timeout=5)

        self.assertEqual(controller.get_status()[UnitState.gone.name], 2)
        self.assertEqual(len(provisioner.destroyed), 2)
