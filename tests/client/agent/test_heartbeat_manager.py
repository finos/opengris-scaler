import asyncio
import time
import unittest
from concurrent.futures import Future
from unittest.mock import AsyncMock

from scaler.client.agent.heartbeat_manager import ClientHeartbeatManager
from scaler.io.mixins import AsyncConnector
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name


def _make_heartbeat_manager(death_timeout_seconds: int) -> ClientHeartbeatManager:
    return ClientHeartbeatManager(death_timeout_seconds=death_timeout_seconds, storage_address_future=Future())


class TestClientHeartbeatManagerDeathTimeout(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.heartbeat_manager = _make_heartbeat_manager(death_timeout_seconds=5)
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.heartbeat_manager.register(connector_external=self.connector_external)

    async def test_death_timeout_routine_raises_once_timeout_elapsed(self) -> None:
        self.heartbeat_manager._last_scheduler_contact = time.time() - 10

        with self.assertRaises(TimeoutError):
            await self.heartbeat_manager.death_timeout_routine()

    async def test_death_timeout_routine_does_not_raise_within_window(self) -> None:
        self.heartbeat_manager._last_scheduler_contact = time.time()

        await self.heartbeat_manager.death_timeout_routine()

    async def test_death_timeout_routine_does_not_send_heartbeat(self) -> None:
        # death_timeout_routine() must never depend on send_heartbeat() completing: a heartbeat
        # send that hangs (e.g. writing to an already-dead scheduler) must not be able to block
        # this wall-clock check from firing.
        self.heartbeat_manager._last_scheduler_contact = time.time() - 10

        with self.assertRaises(TimeoutError):
            await self.heartbeat_manager.death_timeout_routine()

        self.connector_external.send.assert_not_called()


class TestClientHeartbeatManagerLoopIntegration(unittest.IsolatedAsyncioTestCase):
    """Runs routine() and death_timeout_routine() together the same way ClientAgent.__get_loops()
    does (via create_async_loop_routine + asyncio.gather), with a heartbeat send that never
    completes -- reproducing what a dead scheduler does when the OS is slow to fail the write
    (observed to take ~50s on windows-latest CI, vs. sub-second on Linux/macOS).

    Regression coverage for: a hung send_heartbeat() used to prevent the death-timeout check from
    ever re-running, because both lived in the same routine() loop iteration."""

    async def test_death_timeout_fires_promptly_even_when_heartbeat_send_never_completes(self) -> None:
        death_timeout_seconds = 1
        heartbeat_manager = _make_heartbeat_manager(death_timeout_seconds=death_timeout_seconds)
        connector_external = AsyncMock(spec=AsyncConnector)

        async def hang_forever(*args, **kwargs):
            await asyncio.Event().wait()  # never set, so this never resolves

        connector_external.send.side_effect = hang_forever
        heartbeat_manager.register(connector_external=connector_external)

        loops = [
            create_async_loop_routine(heartbeat_manager.routine, 0),
            create_async_loop_routine(heartbeat_manager.death_timeout_routine, 1),
        ]

        # The outer bound is only a safety net so a regression fails the test instead of hanging
        # the suite; the real assertion is that we come in well under it, close to death_timeout.
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(asyncio.gather(*loops), timeout=10)
        elapsed = time.monotonic() - start

        self.assertLess(
            elapsed,
            death_timeout_seconds + 2,
            f"death timeout took {elapsed:.1f}s to fire; expected ~{death_timeout_seconds}s "
            f"-- the death-timeout check is being blocked by the hung heartbeat send",
        )
