import unittest
from typing import Optional
from unittest.mock import MagicMock, patch

from scaler.worker_manager_adapter.baremetal.native import TASK_CONCURRENCY_PER_WORKER, NativeWorkerProvisioner
from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS


def _make_provisioner(max_task_concurrency: int = UNLIMITED_UNITS) -> NativeWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.worker_manager_config.max_task_concurrency = max_task_concurrency
    config.worker_manager_config.worker_manager_id = "test-wm"
    config.worker_type = "NAT"
    return NativeWorkerProvisioner(config)


class _FakeWorker:
    """Stands in for a Worker process: only the parts the provisioner touches."""

    def __init__(self, alive: bool = True, exitcode: Optional[int] = None, pid: int = 4242) -> None:
        self._alive = alive
        self.exitcode = exitcode
        self.pid = pid
        self.started = False
        self.joined = False

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self._alive

    def join(self) -> None:
        self.joined = True

    def die(self, exitcode: int) -> None:
        self._alive = False
        self.exitcode = exitcode


class TestNativeWorkerProvisionerCreate(unittest.IsolatedAsyncioTestCase):
    async def test_the_unit_id_exists_before_the_process_does(self) -> None:
        """No registration handshake is needed, because the manager names the worker first."""
        provisioner = _make_provisioner()
        seen = {}

        def capture(name: str) -> _FakeWorker:
            seen["name"] = name
            return _FakeWorker()

        with patch.object(provisioner, "_create_worker", side_effect=capture):
            unit_id = await provisioner.create_unit()

        self.assertEqual(unit_id, seen["name"])
        self.assertTrue(unit_id.startswith("NAT|"))

    async def test_a_created_unit_is_started_and_tracked(self) -> None:
        provisioner = _make_provisioner()
        worker = _FakeWorker()

        with patch.object(provisioner, "_create_worker", return_value=worker):
            unit_id = await provisioner.create_unit()

        self.assertTrue(worker.started)
        self.assertEqual(await provisioner.poll_units(), {unit_id})


class TestNativeWorkerProvisionerPoll(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.provisioner = _make_provisioner()
        self.workers = {}
        for _ in range(2):
            worker = _FakeWorker()
            with patch.object(self.provisioner, "_create_worker", return_value=worker):
                self.workers[await self.provisioner.create_unit()] = worker

    async def test_a_worker_that_died_drops_out_of_the_poll(self) -> None:
        dead_id, dead = next(iter(self.workers.items()))
        dead.die(exitcode=1)

        alive = await self.provisioner.poll_units()

        self.assertNotIn(dead_id, alive)
        self.assertEqual(len(alive), 1)

    async def test_a_dead_worker_is_joined(self) -> None:
        _, dead = next(iter(self.workers.items()))
        dead.die(exitcode=0)

        await self.provisioner.poll_units()

        self.assertTrue(dead.joined)

    async def test_a_clean_exit_is_not_a_warning(self) -> None:
        _, dead = next(iter(self.workers.items()))
        dead.die(exitcode=0)

        with patch("scaler.worker_manager_adapter.baremetal.native.logger") as mock_logger:
            await self.provisioner.poll_units()

        mock_logger.warning.assert_not_called()

    async def test_an_unexpected_exit_warns(self) -> None:
        _, dead = next(iter(self.workers.items()))
        dead.die(exitcode=-9)

        with patch("scaler.worker_manager_adapter.baremetal.native.logger") as mock_logger:
            await self.provisioner.poll_units()

        mock_logger.warning.assert_called_once()

    async def test_repeated_polls_stay_consistent(self) -> None:
        dead_id, dead = next(iter(self.workers.items()))
        dead.die(exitcode=1)

        first = await self.provisioner.poll_units()
        second = await self.provisioner.poll_units()

        self.assertEqual(first, second)
        self.assertNotIn(dead_id, second)


class TestNativeWorkerProvisionerTeardown(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.provisioner = _make_provisioner()
        self.worker = _FakeWorker()
        with patch.object(self.provisioner, "_create_worker", return_value=self.worker):
            self.unit_id = await self.provisioner.create_unit()

    async def test_destroying_an_unknown_unit_is_safe(self) -> None:
        await self.provisioner.destroy_unit("never-existed")

    async def test_destroy_forgets_the_unit(self) -> None:
        with patch("os.kill"), patch("psutil.Process"):
            await self.provisioner.destroy_unit(self.unit_id)

        self.assertEqual(await self.provisioner.poll_units(), set())

    async def test_shutdown_of_an_unknown_unit_is_safe(self) -> None:
        await self.provisioner.shutdown_unit("never-existed")

    async def test_shutdown_keeps_the_unit_until_it_exits(self) -> None:
        with patch("os.kill"), patch("psutil.Process"):
            await self.provisioner.shutdown_unit(self.unit_id)

        # A drain is not a teardown: the worker is still there, finishing its task.
        self.assertEqual(await self.provisioner.poll_units(), {self.unit_id})


class TestNativeWorkerProvisionerConstants(unittest.TestCase):
    def test_one_worker_supplies_one_task_slot(self) -> None:
        self.assertEqual(_make_provisioner().task_concurrency_per_unit(), TASK_CONCURRENCY_PER_WORKER)

    def test_max_units_follows_max_task_concurrency(self) -> None:
        self.assertEqual(_make_provisioner(max_task_concurrency=6).max_units(), 6)

    def test_unlimited_stays_unlimited(self) -> None:
        self.assertEqual(_make_provisioner(max_task_concurrency=-1).max_units(), UNLIMITED_UNITS)


if __name__ == "__main__":
    unittest.main()
