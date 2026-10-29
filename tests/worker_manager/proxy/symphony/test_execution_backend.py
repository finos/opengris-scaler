"""Covers how the Symphony backend refuses cancels and gives IBM Spectrum Symphony back when the worker exits.

A worker that exits with its SOAM session still open dies of SIGABRT with
``malloc_consolidate(): invalid chunk size``: the ``soamapi`` shared libraries tear themselves down at
process exit in an order that corrupts the heap. Closing in order is what avoids that, so the order and
the resilience of each step are worth pinning.

``SymphonyExecutionBackend.__init__`` connects to a real cluster, so these build the object without it
and fill in the handles each test uses.
"""

import unittest
from typing import Any, List
from unittest.mock import MagicMock

from scaler.protocol.capnp import TaskCancel
from scaler.utility.exceptions import TaskCancelUnsupportedError
from scaler.utility.identifiers import TaskID
from scaler.worker_manager.proxy.symphony.execution_backend import SymphonyExecutionBackend

DESTROY_ON_CLOSE = "destroy-on-close"


def _backend_with_recording_handles(calls: List[str]) -> Any:
    backend = object.__new__(SymphonyExecutionBackend)

    soamapi = MagicMock()
    soamapi.SessionCloseFlags.DESTROY_ON_CLOSE = DESTROY_ON_CLOSE
    soamapi.uninitialize.side_effect = lambda: calls.append("uninitialize")

    session = MagicMock()
    session.close.side_effect = lambda flags: calls.append(f"session:{flags}")

    connection = MagicMock()
    connection.close.side_effect = lambda: calls.append("connection")

    backend._soamapi = soamapi
    backend._ibm_soam_session = session
    backend._ibm_soam_connection = connection
    return backend


class CloseTest(unittest.TestCase):
    def test_the_session_is_destroyed_then_the_connection_then_the_api(self) -> None:
        calls: List[str] = []

        _backend_with_recording_handles(calls).close()

        self.assertEqual(calls, [f"session:{DESTROY_ON_CLOSE}", "connection", "uninitialize"])

    def test_a_session_that_will_not_close_still_leaves_the_api_shut_down(self) -> None:
        """Stopping at the first failure would leave the API initialized, which is the state that aborts."""
        calls: List[str] = []
        backend = _backend_with_recording_handles(calls)
        backend._ibm_soam_session.close.side_effect = RuntimeError("session is gone")

        backend.close()

        self.assertEqual(calls, ["connection", "uninitialize"])

    def test_closing_reports_a_failed_step_rather_than_raising(self) -> None:
        backend = _backend_with_recording_handles([])
        backend._ibm_soam_connection.close.side_effect = RuntimeError("connection is gone")

        # "scaler" rather than the root logger: setup_logger sets propagate=False on it, so a test that
        # watches root passes alone and fails once anything in the run has configured logging.
        with self.assertLogs("scaler", level="WARNING") as captured:
            backend.close()

        self.assertTrue(any("connection is gone" in message for message in captured.output))


class CancelTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_force_cancel_raises_so_the_task_keeps_running_and_reports_its_result(self) -> None:
        """soamapi cannot stop a task, so a cancel that returned would confirm a task Symphony still runs."""
        backend = object.__new__(SymphonyExecutionBackend)
        task_cancel = TaskCancel(taskId=TaskID.generate_task_id(), flags=TaskCancel.TaskCancelFlags(force=True))

        with self.assertRaises(TaskCancelUnsupportedError):
            await backend.on_cancel(task_cancel)


if __name__ == "__main__":
    unittest.main()
