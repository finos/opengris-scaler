"""Regression tests for the GIL dead-lock when a ymq socket is torn down at interpreter exit.

A socket that is still alive when the interpreter starts finalizing is torn down by ``tp_dealloc``,
which blocks the main thread until the C++ event loop thread signals shutdown completion. That thread
has to re-enter Python first (to fail the pending receive and send callbacks), so it calls
``PyGILState_Ensure()``. Up to and including CPython 3.13, a non-Python thread that asks for the GIL
once finalization has started is killed with ``pthread_exit()``, so the completion is never signalled
and the main thread waits for it forever. See finos/opengris-scaler#945.

The failure only exists across a whole interpreter lifetime, so there is nothing to assert in-process:
each test runs ``tests.io.interpreter_shutdown_subject`` and asserts on how that process terminated.
Both known symptoms are covered, because they are the same defect. The forced unwind aborts the
process where it crosses a ``noexcept`` frame, and leaves it hanging where it does not.

These reproduce on CPython up to 3.13 only. 3.14 narrowed the window in which it refuses the GIL to a
foreign thread, so the scenarios happen to survive it and the tests pass without proving anything.
The CI test job pins 3.10, which is affected, so they still have teeth there; if that pin ever moves
past 3.13, this file stops being a regression test.
"""

import subprocess
import sys
import unittest

from scaler.utility.logging.utility import setup_logger
from tests.io import interpreter_shutdown_subject
from tests.utility.utility import logging_test_name

INTERPRETER_EXIT_TIMEOUT_SECONDS = 60


class TestYMQInterpreterShutdown(unittest.TestCase):
    """A ymq socket still alive at interpreter exit must not hang or abort the process."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def _assert_exits_cleanly(self, scenario: str) -> None:
        self.assertIn(scenario, interpreter_shutdown_subject.SCENARIOS)

        process = subprocess.Popen(
            [sys.executable, "-m", interpreter_shutdown_subject.__name__, scenario],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            stdout, stderr = process.communicate(timeout=INTERPRETER_EXIT_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            self.fail(
                f"{scenario}: interpreter did not exit within {INTERPRETER_EXIT_TIMEOUT_SECONDS}s, the shutdown "
                f"drain is waiting on an event loop thread that can no longer acquire the GIL"
            )

        self.assertEqual(
            process.returncode,
            0,
            f"{scenario}: interpreter exited with {process.returncode}\n"
            f"--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        )

    def test_binder_with_pending_receive_exits_cleanly(self) -> None:
        self._assert_exits_cleanly("binder_with_pending_receive")

    def test_binder_without_python_del_exits_cleanly(self) -> None:
        self._assert_exits_cleanly("binder_without_python_del")

    def test_connector_with_pending_receive_exits_cleanly(self) -> None:
        self._assert_exits_cleanly("connector_with_pending_receive")


if __name__ == "__main__":
    unittest.main()
