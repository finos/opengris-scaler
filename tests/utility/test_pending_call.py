import threading
import time
import unittest

from scaler.utility import pending_call


class TestPendingCall(unittest.TestCase):
    def test_schedule_runs_on_main_thread(self):
        # Schedule from a non-main thread; the trampoline must run on the main thread
        # at the next CPython eval-breaker check.
        result = {}

        def callback():
            result["thread_ident"] = threading.get_ident()

        def schedule_from_other_thread():
            pending_call.schedule(callback)

        thread = threading.Thread(target=schedule_from_other_thread)
        thread.start()
        thread.join()

        # Drive the eval loop so the pending call fires. time.sleep yields and gives the
        # interpreter a chance to drain the pending queue.
        deadline = time.monotonic() + 1.0
        while "thread_ident" not in result and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertIn("thread_ident", result, "pending call did not fire within 1s")
        self.assertEqual(result["thread_ident"], threading.main_thread().ident)

    def test_schedule_rejects_non_callable(self):
        with self.assertRaises(TypeError):
            pending_call.schedule(123)  # type: ignore[arg-type]

    def test_schedule_propagates_callable_returning_none(self):
        flag = {}

        def callback():
            flag["ran"] = True

        pending_call.schedule(callback)

        deadline = time.monotonic() + 1.0
        while "ran" not in flag and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertTrue(flag.get("ran", False))


if __name__ == "__main__":
    unittest.main()
