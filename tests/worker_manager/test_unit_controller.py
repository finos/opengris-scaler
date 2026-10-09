import asyncio
import unittest
from typing import Dict, List, Set

from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.unit_controller import UNIT_STABLE_SECONDS, UnitController, UnitState
from tests.utility.utility import logging_test_name

TASK_CONCURRENCY_PER_UNIT = 2
RESTART_BACKOFF_SECONDS = 60
DRAIN_TIMEOUT_SECONDS = 60
UNIT_TIMEOUT_SECONDS = 60
STARTUP_TIMEOUT_SECONDS = 60


class _FakeProvisioner(UnitProvisioner):
    """Units that exist until destroyed; each create waits for `release_creates` when `hold_creates` is set."""

    def __init__(self, max_units: int = -1) -> None:
        self._max_units = max_units
        self.existing: Set[str] = set()
        self.created: List[str] = []
        self.destroyed: List[str] = []
        self.fail_creates = False
        self.fail_destroys = False
        self.hold_creates = False
        self.release_creates = asyncio.Event()

    async def create_unit(self, unit_id: str) -> UnitHandle:
        if self.hold_creates:
            await self.release_creates.wait()
        if self.fail_creates:
            raise RuntimeError("no capacity")
        self.created.append(unit_id)
        self.existing.add(unit_id)
        return unit_id

    async def destroy_unit(self, handle: UnitHandle) -> None:
        if self.fail_destroys:
            raise RuntimeError("api error")
        assert isinstance(handle, str)
        self.destroyed.append(handle)
        self.existing.discard(handle)

    def max_units(self) -> int:
        return self._max_units

    def task_concurrency_per_unit(self) -> int:
        return TASK_CONCURRENCY_PER_UNIT

    def startup_timeout_seconds(self) -> int:
        return STARTUP_TIMEOUT_SECONDS


class TestUnitController(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.provisioner = _FakeProvisioner()
        self.controller = self.__make_controller(self.provisioner)

    @staticmethod
    def __make_controller(provisioner: UnitProvisioner, scale_down_cooldown_seconds: float = 0) -> UnitController:
        return UnitController(
            provisioner,
            scale_down_cooldown_seconds=scale_down_cooldown_seconds,
            unit_timeout_seconds=UNIT_TIMEOUT_SECONDS,
            drain_timeout_seconds=DRAIN_TIMEOUT_SECONDS,
            restart_backoff_seconds=RESTART_BACKOFF_SECONDS,
        )

    async def __tick(self, controller: UnitController) -> None:
        """Run one routine, then let the provisioner calls it started finish."""
        await controller.routine()
        for _ in range(3):
            await asyncio.sleep(0)

    def __heartbeat_all(self, controller: UnitController, provisioner: _FakeProvisioner) -> None:
        """Every unit that exists sends one heartbeat."""
        for unit_id in provisioner.existing:
            controller.on_unit_heartbeat(unit_id, TASK_CONCURRENCY_PER_UNIT, occupancy=0)

    def __states(self, controller: UnitController) -> Dict[UnitState, int]:
        states: Dict[UnitState, int] = {}
        for unit in controller._units.values():
            states[unit.state] = states.get(unit.state, 0) + 1
        return states

    async def test_creates_enough_units_for_the_desired_task_concurrency(self) -> None:
        self.controller.set_desired_task_concurrency(5)
        await self.__tick(self.controller)
        self.assertEqual(len(self.provisioner.created), 3)

    async def test_max_units_caps_the_fleet(self) -> None:
        provisioner = _FakeProvisioner(max_units=2)
        controller = self.__make_controller(provisioner)
        controller.set_desired_task_concurrency(100)
        await self.__tick(controller)
        self.assertEqual(len(provisioner.created), 2)

    async def test_a_create_in_flight_counts_as_supply(self) -> None:
        """A slow create does not make the next routine create another unit."""
        self.provisioner.hold_creates = True
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.pending: 1})

        self.provisioner.release_creates.set()
        await self.__tick(self.controller)
        self.__heartbeat_all(self.controller, self.provisioner)
        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.created, list(self.controller._units))
        self.assertEqual(self.__states(self.controller), {UnitState.active: 1})

    async def test_a_unit_becomes_active_on_its_first_heartbeat(self) -> None:
        """Existing is not enough: a unit that never reports stays pending."""
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.pending: 1})

        self.__heartbeat_all(self.controller, self.provisioner)
        self.assertEqual(self.__states(self.controller), {UnitState.active: 1})

    async def test_a_unit_that_never_sends_a_heartbeat_is_destroyed_after_the_startup_timeout(self) -> None:
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        (unit_id,) = self.controller._units
        self.controller._units[unit_id].state_since -= STARTUP_TIMEOUT_SECONDS + 1

        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.destroyed, [unit_id])
        self.assertEqual(self.controller._consecutive_unit_losses, 1)

    async def test_a_silent_active_unit_is_destroyed_and_counts_as_lost(self) -> None:
        """A unit that exists but stopped sending heartbeats, such as a deadlocked worker, is replaced."""
        (unit_id,) = await self.__active_units(1)
        self.controller._units[unit_id].last_heartbeat -= UNIT_TIMEOUT_SECONDS + 1

        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.destroyed, [unit_id])
        self.assertEqual(self.controller._consecutive_unit_losses, 1)

        self.controller._create_not_before = 0
        await self.__tick(self.controller)
        self.assertEqual(len(self.provisioner.created), 2)

    async def test_a_silent_draining_unit_is_destroyed_before_its_deadline_without_backoff(self) -> None:
        (unit_id,) = await self.__active_units(1)
        self.controller.set_desired_task_concurrency(0)
        await self.__tick(self.controller)
        self.controller._units[unit_id].last_heartbeat -= UNIT_TIMEOUT_SECONDS + 1

        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.destroyed, [unit_id])
        self.assertEqual(self.controller._consecutive_unit_losses, 0)

    async def test_a_unit_that_sends_heartbeats_is_not_silent(self) -> None:
        (unit_id,) = await self.__active_units(1)
        self.controller._units[unit_id].last_heartbeat -= UNIT_TIMEOUT_SECONDS + 1
        self.__heartbeat_all(self.controller, self.provisioner)

        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.destroyed, [])

    async def test_a_lost_unit_is_removed_and_replaced_after_the_backoff(self) -> None:
        (lost_unit_id,) = await self.__active_units(1)
        self.controller._units[lost_unit_id].last_heartbeat -= UNIT_TIMEOUT_SECONDS + 1

        await self.__tick(self.controller)
        self.assertNotIn(lost_unit_id, self.controller._units)
        self.assertEqual(self.provisioner.created, [lost_unit_id], "the backoff holds the replacement back")

        self.controller._create_not_before = 0
        await self.__tick(self.controller)
        self.assertEqual(len(self.provisioner.created), 2)

    async def test_a_heartbeat_from_a_unit_stable_for_a_while_resets_the_backoff(self) -> None:
        """A unit that stays active proves the unit recipe works, so earlier losses stop doubling the backoff."""
        (unit_id,) = await self.__active_units(1)
        self.controller._consecutive_unit_losses = 3

        self.__heartbeat_all(self.controller, self.provisioner)
        self.assertEqual(self.controller._consecutive_unit_losses, 3, "a unit just made active proves nothing yet")

        self.controller._units[unit_id].state_since -= UNIT_STABLE_SECONDS + 1
        self.__heartbeat_all(self.controller, self.provisioner)
        self.assertEqual(self.controller._consecutive_unit_losses, 0)

    async def test_a_failed_create_leaves_no_unit_and_arms_the_backoff(self) -> None:
        self.provisioner.fail_creates = True
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        self.assertEqual(self.controller._units, {})

        self.provisioner.fail_creates = False
        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.created, [])

    async def __active_units(self, count: int) -> List[str]:
        self.controller.set_desired_task_concurrency(count * TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        self.__heartbeat_all(self.controller, self.provisioner)
        return list(self.controller._units)

    async def test_scale_down_drains_the_least_occupied_units(self) -> None:
        busy, idle, half = await self.__active_units(3)
        self.controller.on_unit_heartbeat(busy, TASK_CONCURRENCY_PER_UNIT, occupancy=2)
        self.controller.on_unit_heartbeat(half, TASK_CONCURRENCY_PER_UNIT, occupancy=1)

        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)

        self.assertFalse(self.controller.is_unit_serving(idle))
        self.assertFalse(self.controller.is_unit_serving(half))
        self.assertTrue(self.controller.is_unit_serving(busy))
        self.assertEqual(self.provisioner.destroyed, [], "a drain destroys nothing")

    async def test_a_draining_unit_is_not_supply(self) -> None:
        """A long drain does not make each routine drain one more unit."""
        await self.__active_units(2)
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        for _ in range(3):
            await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.active: 1, UnitState.draining: 1})

    async def test_a_drain_past_its_deadline_destroys_the_unit(self) -> None:
        (unit_id,) = await self.__active_units(1)
        self.controller.set_desired_task_concurrency(0)
        await self.__tick(self.controller)

        self.controller._units[unit_id].drain_deadline = 0
        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.destroyed, [unit_id])

    async def test_a_unit_that_reports_its_fleet_gone_is_destroyed(self) -> None:
        (unit_id,) = await self.__active_units(1)
        self.controller.set_desired_task_concurrency(0)
        await self.__tick(self.controller)

        self.controller.on_unit_disconnect(unit_id)
        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.destroyed, [unit_id])
        self.assertEqual(self.controller._consecutive_unit_losses, 0, "a finished drain is not a loss")

    async def test_a_unit_that_exits_while_serving_is_destroyed_and_counts_as_lost(self) -> None:
        """A disconnect notification from a unit nobody told to drain means it gave up on its own."""
        (unit_id,) = await self.__active_units(1)

        self.controller.on_unit_disconnect(unit_id)
        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.destroyed, [unit_id])
        self.assertNotIn(unit_id, self.controller._units)
        self.assertEqual(self.controller._consecutive_unit_losses, 1)

    async def test_an_unknown_unit_is_not_serving(self) -> None:
        self.assertFalse(self.controller.is_unit_serving("unknown"))

    async def test_units_are_filled_before_the_last_one_takes_the_remainder(self) -> None:
        first, second = await self.__active_units(2)
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT + 1)
        await self.__tick(self.controller)
        self.assertEqual(self.controller.get_unit_desired_task_concurrency(first), TASK_CONCURRENCY_PER_UNIT)
        self.assertEqual(self.controller.get_unit_desired_task_concurrency(second), 1)

    async def test_shutdown_drains_every_unit_and_ignores_new_counts(self) -> None:
        units = await self.__active_units(2)
        self.controller.begin_shutdown()
        self.controller.set_desired_task_concurrency(10 * TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.draining: 2})
        self.assertFalse(self.controller.is_shut_down())

        for unit_id in units:
            self.controller.on_unit_disconnect(unit_id)
        await self.__tick(self.controller)
        self.assertTrue(self.controller.is_shut_down())
        self.assertEqual(len(self.provisioner.created), 2)

    async def test_scale_down_waits_for_the_cooldown(self) -> None:
        controller = self.__make_controller(self.provisioner, scale_down_cooldown_seconds=60)
        controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(controller)
        self.__heartbeat_all(controller, self.provisioner)

        controller.set_desired_task_concurrency(0)
        await self.__tick(controller)
        self.assertEqual(self.provisioner.destroyed, [])

    async def test_a_failed_destroy_is_retried(self) -> None:
        await self.__active_units(1)

        self.provisioner.fail_destroys = True
        (unit_id,) = self.controller._units
        self.controller.set_desired_task_concurrency(0)
        self.controller.on_unit_disconnect(unit_id)
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.stopping: 1})

        self.provisioner.fail_destroys = False
        await self.__tick(self.controller)
        self.assertEqual(self.controller._units, {})
        self.assertEqual(len(self.provisioner.destroyed), 1)

    async def test_terminate_destroys_every_unit_including_one_still_being_created(self) -> None:
        await self.__active_units(1)

        self.provisioner.hold_creates = True
        self.controller.set_desired_task_concurrency(2 * TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)

        terminate = asyncio.ensure_future(self.controller.terminate())
        await asyncio.sleep(0)
        self.provisioner.release_creates.set()
        await terminate

        self.assertEqual(sorted(self.provisioner.destroyed), sorted(self.provisioner.created))
        self.assertEqual(len(self.provisioner.destroyed), 2)
        self.assertEqual(self.controller._units, {})
