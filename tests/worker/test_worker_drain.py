"""The worker half of the drain protocol.

A drain is a shutdown that keeps the work. WorkerShutdown starts it; the worker reports draining
so the scheduler stops assigning to it and reclaims its queue, finishes the one task it is running,
and only then exits. These tests drive that from a stand-in manager binder.
"""

import unittest
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import WorkerHeartbeat, WorkerShutdown
from scaler.utility.identifiers import WorkerID
from scaler.worker.agent.heartbeat_manager import VanillaHeartbeatManager

TASK_QUEUE_SIZE = 10


def _make_heartbeat_manager(connector_manager: Optional[AsyncMock] = None) -> VanillaHeartbeatManager:
    manager = VanillaHeartbeatManager(
        object_storage_address=None, capabilities={}, task_queue_size=TASK_QUEUE_SIZE, worker_manager_id=b"test-wm"
    )

    task_manager = MagicMock()
    task_manager.get_queued_size.return_value = 0

    processor_manager = MagicMock()
    processor_manager.can_accept_task.return_value = True
    processor_manager.processors.return_value = []
    processor_manager.num_suspended_processors.return_value = 0

    manager.register(
        connector_external=AsyncMock(),
        connector_storage=MagicMock(),
        worker_task_manager=task_manager,
        timeout_manager=MagicMock(),
        processor_manager=processor_manager,
        connector_manager=connector_manager,
    )
    return manager


def _sent_heartbeats(connector) -> List[WorkerHeartbeat]:
    return [call.args[0] for call in connector.send.await_args_list if isinstance(call.args[0], WorkerHeartbeat)]


class TestWorkerDrainFlag(unittest.IsolatedAsyncioTestCase):
    async def test_a_worker_does_not_start_out_draining(self) -> None:
        self.assertFalse(_make_heartbeat_manager().is_draining())

    async def test_the_flag_shows_up_in_the_next_heartbeat(self) -> None:
        manager = _make_heartbeat_manager()
        manager.set_draining()

        await manager.routine()

        heartbeats = _sent_heartbeats(manager._connector_external)
        self.assertTrue(heartbeats[-1].draining)

    async def test_an_undrained_worker_reports_false(self) -> None:
        manager = _make_heartbeat_manager()

        await manager.routine()

        self.assertFalse(_sent_heartbeats(manager._connector_external)[-1].draining)

    async def test_a_drain_cannot_be_reversed(self) -> None:
        manager = _make_heartbeat_manager()
        manager.set_draining()
        manager.set_draining()

        self.assertTrue(manager.is_draining())

    async def test_the_report_also_reaches_the_manager(self) -> None:
        """The manager reads occupancy from the same heartbeat, so the link needs no new message."""
        connector_manager = AsyncMock()
        manager = _make_heartbeat_manager(connector_manager)

        await manager.routine()

        self.assertEqual(len(_sent_heartbeats(connector_manager)), 1)

    async def test_a_worker_with_no_manager_still_reports_to_the_scheduler(self) -> None:
        manager = _make_heartbeat_manager(connector_manager=None)

        await manager.routine()

        self.assertEqual(len(_sent_heartbeats(manager._connector_external)), 1)


class TestWorkerShutdownMessage(unittest.IsolatedAsyncioTestCase):
    """WorkerShutdown carries no payload: it means "you", and the binder identifies the sender."""

    async def test_it_round_trips(self) -> None:
        from scaler.io.utility import deserialize, serialize

        message = deserialize(serialize(WorkerShutdown()))
        self.assertIsInstance(message, WorkerShutdown)


class TestDrainExitCondition(unittest.IsolatedAsyncioTestCase):
    """A drained worker leaves when the task it is running is done, not before."""

    def _worker(self, draining: bool, queued: int, idle: bool):
        from scaler.worker.worker import Worker

        worker = Worker.__new__(Worker)
        worker._connector_manager = MagicMock()  # a worker with no manager link can never drain
        worker._heartbeat_manager = MagicMock()
        worker._heartbeat_manager.is_draining.return_value = draining
        worker._task_manager = MagicMock()
        worker._task_manager.get_queued_size.return_value = queued
        worker._processor_manager = MagicMock()
        worker._processor_manager.can_accept_task.return_value = idle
        worker._ident = WorkerID(b"worker-under-test")
        return worker

    async def _run_drain_routine(self, worker) -> bool:
        from scaler.utility.exceptions import ClientQuitException

        routine = getattr(worker, "_Worker__drain_routine")
        try:
            await routine()
        except ClientQuitException:
            return True
        return False

    async def test_a_worker_with_no_manager_link_never_drains(self) -> None:
        worker = self._worker(draining=True, queued=0, idle=True)
        worker._connector_manager = None

        self.assertFalse(await self._run_drain_routine(worker))

    async def test_a_worker_that_is_not_draining_stays(self) -> None:
        quit_now = await self._run_drain_routine(self._worker(draining=False, queued=0, idle=True))
        self.assertFalse(quit_now)

    async def test_a_draining_worker_with_a_running_task_stays(self) -> None:
        quit_now = await self._run_drain_routine(self._worker(draining=True, queued=0, idle=False))
        self.assertFalse(quit_now)

    async def test_a_draining_worker_with_a_queue_stays(self) -> None:
        quit_now = await self._run_drain_routine(self._worker(draining=True, queued=3, idle=True))
        self.assertFalse(quit_now)

    async def test_a_drained_and_idle_worker_leaves(self) -> None:
        quit_now = await self._run_drain_routine(self._worker(draining=True, queued=0, idle=True))
        self.assertTrue(quit_now)


if __name__ == "__main__":
    unittest.main()
