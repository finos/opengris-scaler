import asyncio
import multiprocessing
import threading
import time
import unittest

from scaler.utility.process_signal import register_async_shutdown


class TestProcessSignalShutdownEvent(unittest.TestCase):
    """Verify the shutdown_event channel works on whichever platform we're running on.

    Signal-driven Ctrl+C / SIGTERM behavior is hard to test from inside a unittest
    process; the event-driven path is the contract that worker provisioners depend on
    cross-platform, so we exercise it here.
    """

    def test_shutdown_event_dispatches_into_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        shutdown_event = multiprocessing.get_context("spawn").Event()
        triggered = threading.Event()

        async def _main():
            register_async_shutdown(loop, triggered.set, shutdown_event=shutdown_event)
            # Park the loop so the event watcher daemon thread has a real loop to dispatch into.
            try:
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                pass

        def _trigger_after_brief_delay():
            time.sleep(0.1)
            shutdown_event.set()

        threading.Thread(target=_trigger_after_brief_delay, daemon=True).start()

        async def _watch():
            deadline = time.monotonic() + 2.0
            while not triggered.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            for task in asyncio.all_tasks(loop):
                if task is not asyncio.current_task():
                    task.cancel()

        async def _runner():
            try:
                await asyncio.gather(_main(), _watch())
            except asyncio.CancelledError:
                pass

        try:
            loop.run_until_complete(_runner())
        finally:
            asyncio.set_event_loop(None)
            loop.close()

        self.assertTrue(triggered.is_set(), "register_async_shutdown did not invoke on_shutdown")


if __name__ == "__main__":
    unittest.main()
