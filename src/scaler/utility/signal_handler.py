import asyncio
import signal
import sys
import threading
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event as EventType


def install_async_shutdown_handler(
    loop: asyncio.AbstractEventLoop, callback: Callable[[], None], shutdown_event: Optional["EventType"] = None
) -> None:
    """Register `callback` to run on the asyncio event loop when the process receives
    SIGINT or SIGTERM, or when the optional `shutdown_event` is set. Cross-platform:
    uses `loop.add_signal_handler` on POSIX and `signal.signal` plus
    `loop.call_soon_threadsafe` on Windows, where the asyncio loops do not support
    direct signal handler registration. The event-based path synthesizes a
    signal-disposition for parent processes that cannot deliver a real signal to a
    specific subprocess on Windows (multiprocessing.Process.terminate() is
    TerminateProcess, which never runs Python handlers).
    """
    if sys.platform == "win32":

        def _handler(*_args):
            loop.call_soon_threadsafe(callback)

        # Windows accepts SIGINT, SIGTERM, SIGBREAK among others for signal.signal.
        # SIGTERM is not delivered by the OS from external commands on Windows but
        # can still be raised in-process; registering it is harmless.
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

        if shutdown_event is not None:

            def _wait_for_event():
                shutdown_event.wait()
                loop.call_soon_threadsafe(callback)

            threading.Thread(target=_wait_for_event, name="ShutdownEventWatcher", daemon=True).start()
    else:
        loop.add_signal_handler(signal.SIGINT, callback)
        loop.add_signal_handler(signal.SIGTERM, callback)
