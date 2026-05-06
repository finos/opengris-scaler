import asyncio
import logging
import signal
import sys
import threading
from multiprocessing.synchronize import Event as MultiprocessingEvent
from typing import Callable, Optional

# Periodic wakeup interval used on Windows so the asyncio Proactor loop is forced
# back into Python land at least once per tick. Without this, signal.signal-based
# Ctrl+C handlers can be delayed indefinitely while IocpProactor.select() waits.
_WINDOWS_WAKEUP_INTERVAL_SECONDS = 0.25


def register_async_shutdown(
    loop: asyncio.AbstractEventLoop,
    on_shutdown: Callable[[], None],
    *,
    shutdown_event: Optional[MultiprocessingEvent] = None,
) -> None:
    """Register a graceful-shutdown handler for the given asyncio loop.

    POSIX path: uses ``loop.add_signal_handler`` for SIGINT and SIGTERM (the
    asyncio-native mechanism, signal-fd backed).

    Windows path: ``loop.add_signal_handler`` is not implemented, and the
    Proactor event loop does not wake on ``signal.signal`` handlers during a
    long IOCP wait. We emulate the two channels separately:

    - SIGINT (Ctrl+C from a console): registered via ``signal.signal``. A
      lightweight asyncio "wakeup" task ensures the proactor returns to Python
      at least every ``_WINDOWS_WAKEUP_INTERVAL_SECONDS`` so the queued
      ``call_soon_threadsafe`` callback runs promptly.
    - SIGTERM-equivalent: Windows does not actually deliver SIGTERM, so a
      caller-supplied ``shutdown_event`` (``multiprocessing.Event``) is the
      out-of-band channel. A daemon thread blocks on it and dispatches the
      shutdown into the loop on set.

    ``shutdown_event`` is also wired up on POSIX (so callers don't need a
    platform branch), giving a unified provisioner-driven shutdown channel.
    """
    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, on_shutdown)
        loop.add_signal_handler(signal.SIGTERM, on_shutdown)
    else:

        def _signal_handler(signum, frame):  # type: ignore[no-untyped-def]
            loop.call_soon_threadsafe(on_shutdown)

        try:
            signal.signal(signal.SIGINT, _signal_handler)
        except ValueError:
            # signal.signal must be called from the main thread; if we're not
            # the main thread (e.g. tests), fall back to event-only shutdown.
            logging.debug("register_async_shutdown: SIGINT handler not installed (not main thread)")

        loop.create_task(_windows_wakeup())

    if shutdown_event is not None:
        _start_event_watcher(loop, on_shutdown, shutdown_event)


async def _windows_wakeup() -> None:
    try:
        while True:
            await asyncio.sleep(_WINDOWS_WAKEUP_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        pass


def _start_event_watcher(
    loop: asyncio.AbstractEventLoop, on_shutdown: Callable[[], None], shutdown_event: MultiprocessingEvent
) -> None:
    def _watch():
        shutdown_event.wait()
        loop.call_soon_threadsafe(on_shutdown)

    thread = threading.Thread(target=_watch, name="shutdown-event-watcher", daemon=True)
    thread.start()
