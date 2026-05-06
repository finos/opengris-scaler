import threading
import time
import unittest

from scaler.utility import pending_call


class TestPendingCall(unittest.TestCase):
    def test_schedules_callable_on_main_thread(self):
        result = []

        main_thread_id = threading.get_ident()

        def callback():
            result.append(threading.get_ident())

        def producer():
            pending_call.schedule(callback)

        thread = threading.Thread(target=producer)
        thread.start()
        thread.join()

        # The trampoline runs at the next CPython eval-breaker tick. A short busy-poll
        # in pure Python (each loop iteration is a safe point) gives the eval loop
        # plenty of opportunities to drain the pending call.
        deadline = time.monotonic() + 1.0
        while not result and time.monotonic() < deadline:
            time.sleep(0.005)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], main_thread_id)

    def test_rejects_non_callable(self):
        with self.assertRaises(TypeError):
            pending_call.schedule(42)  # type: ignore[arg-type]

    def test_callable_exception_does_not_propagate(self):
        # Pending-call handlers cannot propagate exceptions. The trampoline routes
        # them through PyErr_WriteUnraisable. Subsequent schedules must still work.
        ran = []

        def boom():
            raise RuntimeError("boom")

        def good():
            ran.append(True)

        pending_call.schedule(boom)
        pending_call.schedule(good)

        deadline = time.monotonic() + 1.0
        while not ran and time.monotonic() < deadline:
            time.sleep(0.005)

        self.assertEqual(ran, [True])


if __name__ == "__main__":
    unittest.main()
