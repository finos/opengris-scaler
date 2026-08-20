"""What a drain looks like from outside: the monitor and the web UI read it from here."""

import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import WorkerManagerHeartbeat
from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController

_SOURCE = b"manager-source"
_MANAGER_ID = b"manager-aaa"


def _heartbeat(**overrides) -> WorkerManagerHeartbeat:
    fields = dict(
        maxTaskConcurrency=8,
        capabilities=[],
        workerManagerID=_MANAGER_ID,
        activeTaskConcurrency=3,
        occupancy=5,
        activeUnits=3,
        pendingUnits=1,
        drainingUnits=2,
    )
    fields.update(overrides)
    return WorkerManagerHeartbeat(**fields)


class TestManagerFleetStatus(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        config_controller = MagicMock()
        self.policy_controller = MagicMock()
        self.policy_controller.get_scaling_commands.return_value = []
        self.policy_controller.get_scaling_status.return_value = MagicMock()

        self.controller = WorkerManagerController(config_controller, self.policy_controller)

        task_controller = MagicMock()
        task_controller._task_id_to_task = {}

        self.worker_controller = MagicMock()
        self.worker_controller.get_workers_by_manager_id.return_value = []
        self.worker_controller._worker_alive_since = {}

        self.controller.register(AsyncMock(), task_controller, self.worker_controller)

    def _detail(self):
        status = self.controller.get_status()
        return list(status.workerManagerDetails)[0]

    async def test_the_managers_own_unit_counts_reach_the_status(self) -> None:
        await self.controller.on_heartbeat(_SOURCE, _heartbeat())

        detail = self._detail()
        self.assertEqual(detail.activeUnits, 3)
        self.assertEqual(detail.pendingUnits, 1)
        self.assertEqual(detail.drainingUnits, 2)
        self.assertEqual(detail.occupancy, 5)

    async def test_the_counts_survive_the_capability_conversion(self) -> None:
        """on_heartbeat rewrites capabilities, which detaches the struct from its capnp source.

        Anything read after that point is lost, so the numbers are captured before it happens.
        """
        await self.controller.on_heartbeat(_SOURCE, _heartbeat(drainingUnits=7))

        self.assertEqual(self._detail().drainingUnits, 7)

    async def test_a_heartbeat_without_the_fields_does_not_break_the_controller(self) -> None:
        partial = WorkerManagerHeartbeat(maxTaskConcurrency=4, capabilities=[], workerManagerID=_MANAGER_ID)

        await self.controller.on_heartbeat(_SOURCE, partial)

        self.assertEqual(self._detail().drainingUnits, 0)

    async def test_later_heartbeats_replace_the_earlier_counts(self) -> None:
        await self.controller.on_heartbeat(_SOURCE, _heartbeat(drainingUnits=2))
        await self.controller.on_heartbeat(_SOURCE, _heartbeat(drainingUnits=0))

        self.assertEqual(self._detail().drainingUnits, 0)

    async def test_a_disconnected_manager_leaves_no_fleet_behind(self) -> None:
        await self.controller.on_heartbeat(_SOURCE, _heartbeat())
        await self.controller._disconnect_manager(_SOURCE)

        self.assertNotIn(_SOURCE, self.controller._manager_fleet)


if __name__ == "__main__":
    unittest.main()
