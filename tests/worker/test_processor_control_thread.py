import multiprocessing
import threading
import time
import unittest

from scaler.worker.agent.processor.control_thread import ProcessorControlThread


class TestProcessorControlThread(unittest.TestCase):
    """Verify the cross-platform replacement for POSIX SIGUSR1/SIGTERM signal
    delivery to the processor's main interpreter thread.

    The thread observes parent-set multiprocessing.Events and relays the
    intent to the main interpreter thread via pending_call.schedule. Here we
    simulate the main thread by polling for callable invocations after a
    pending call is queued.
    """

    def test_suspend_event_triggers_on_suspend(self):
        ctx = multiprocessing.get_context("spawn")
        suspend_event = ctx.Event()
        terminate_event = ctx.Event()

        suspend_calls = []
        terminate_calls = []

        thread = ProcessorControlThread(
            suspend_event=suspend_event,
            terminate_event=terminate_event,
            on_suspend=lambda: suspend_calls.append(threading.get_ident()),
            on_terminate=lambda: terminate_calls.append(threading.get_ident()),
        )
        thread.start()

        try:
            suspend_event.set()

            deadline = time.monotonic() + 1.0
            while not suspend_calls and time.monotonic() < deadline:
                time.sleep(0.01)

            self.assertEqual(len(suspend_calls), 1)
            self.assertEqual(suspend_calls[0], threading.get_ident(), "callback should run on main thread")
            self.assertFalse(suspend_event.is_set(), "suspend_event should be cleared by the control thread")
            self.assertEqual(terminate_calls, [])
        finally:
            terminate_event.set()
            thread.join(timeout=2.0)

    def test_terminate_event_ends_thread(self):
        ctx = multiprocessing.get_context("spawn")
        suspend_event = ctx.Event()
        terminate_event = ctx.Event()

        terminate_calls = []

        thread = ProcessorControlThread(
            suspend_event=suspend_event,
            terminate_event=terminate_event,
            on_suspend=lambda: None,
            on_terminate=lambda: terminate_calls.append(threading.get_ident()),
        )
        thread.start()

        terminate_event.set()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive(), "control thread should exit after terminate_event")

        deadline = time.monotonic() + 1.0
        while not terminate_calls and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertEqual(len(terminate_calls), 1)
        self.assertEqual(terminate_calls[0], threading.get_ident())

    def test_multiple_suspends_rearm(self):
        ctx = multiprocessing.get_context("spawn")
        suspend_event = ctx.Event()
        terminate_event = ctx.Event()

        suspend_calls = []

        thread = ProcessorControlThread(
            suspend_event=suspend_event,
            terminate_event=terminate_event,
            on_suspend=lambda: suspend_calls.append(time.monotonic()),
            on_terminate=lambda: None,
        )
        thread.start()

        try:
            for _ in range(3):
                suspend_event.set()
                deadline = time.monotonic() + 1.0
                while suspend_event.is_set() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertFalse(suspend_event.is_set(), "control thread should clear after dispatch")

            deadline = time.monotonic() + 1.0
            while len(suspend_calls) < 3 and time.monotonic() < deadline:
                time.sleep(0.01)

            self.assertEqual(len(suspend_calls), 3)
        finally:
            terminate_event.set()
            thread.join(timeout=2.0)


if __name__ == "__main__":
    unittest.main()
