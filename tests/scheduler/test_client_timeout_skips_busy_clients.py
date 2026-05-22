"""Unit test for the scheduler's heartbeat-timeout cleanup logic.

Verifies the fix in ``VanillaClientController.__routine_cleanup_clients``: a
client that is stale on heartbeats but still has outstanding tasks must NOT
be disconnected. This is the exact failure mode hit by browser / Pyodide
clients whose single asyncio event loop is starved by long synchronous user
code and therefore can't send heartbeats while a workload runs.
"""

import asyncio
import time
import unittest
from typing import Any

from scaler.protocol.capnp import ClientHeartbeat, Resource
from scaler.scheduler.controllers.client_controller import VanillaClientController
from scaler.utility.identifiers import ClientID, TaskID


class _StubConfigController:
    def __init__(self, client_timeout_seconds: int):
        self._values = {"client_timeout_seconds": client_timeout_seconds}

    def get_config(self, path: str) -> Any:
        return self._values[path]


class _StubObjectController:
    def __init__(self) -> None:
        self.cleaned: list = []

    def clean_client(self, client_id: ClientID) -> None:
        self.cleaned.append(client_id)


class _StubTaskController:
    def __init__(self) -> None:
        self.canceled: list = []

    async def on_task_cancel(self, client_id: ClientID, request: Any) -> None:
        self.canceled.append((client_id, request))


def _make_controller(timeout_seconds: int) -> VanillaClientController:
    controller = VanillaClientController(_StubConfigController(timeout_seconds))
    # The cleanup path may walk into disconnect → cancel-all-tasks → object
    # cleanup. Stub just enough of those collaborators that the negative
    # case (no in-flight tasks) cleanly exercises a full disconnect, and
    # that a regression in the busy-client case surfaces as a clean
    # ``assertIn`` failure rather than an ``AttributeError``.
    controller._object_controller = _StubObjectController()  # type: ignore[assignment]
    controller._task_controller = _StubTaskController()  # type: ignore[assignment]
    return controller


def _heartbeat() -> ClientHeartbeat:
    return ClientHeartbeat(resource=Resource(cpu=0, rss=0), latencyUS=0)


class TestClientTimeoutSkipsBusyClients(unittest.TestCase):
    def test_stale_client_with_in_flight_task_is_not_disconnected(self):
        """The scheduler must not time out a client that still owns tasks."""
        timeout = 1
        controller = _make_controller(timeout)
        client = ClientID.generate_client_id("busy")
        task = TaskID.generate_task_id()

        # Heartbeat was a long time ago — well past client_timeout_seconds.
        controller._client_last_seen[client] = (time.time() - (timeout + 10), _heartbeat())
        controller.on_task_begin(client, task)

        asyncio.new_event_loop().run_until_complete(controller.routine())

        self.assertIn(client, controller._client_last_seen, "busy client must remain registered")
        self.assertIn(task, controller.get_client_task_ids(client))

    def test_stale_client_with_no_tasks_is_disconnected(self):
        """The original timeout behaviour still applies to idle stale clients."""
        timeout = 1
        controller = _make_controller(timeout)
        client = ClientID.generate_client_id("idle")

        controller._client_last_seen[client] = (time.time() - (timeout + 10), _heartbeat())

        asyncio.new_event_loop().run_until_complete(controller.routine())

        self.assertNotIn(client, controller._client_last_seen, "idle stale client must be disconnected")

    def test_fresh_client_is_not_disconnected(self):
        """A client within the timeout window is never touched."""
        timeout = 60
        controller = _make_controller(timeout)
        client = ClientID.generate_client_id("fresh")

        controller._client_last_seen[client] = (time.time(), _heartbeat())

        asyncio.new_event_loop().run_until_complete(controller.routine())

        self.assertIn(client, controller._client_last_seen)


if __name__ == "__main__":
    unittest.main()
