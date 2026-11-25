import asyncio
import signal
import sys
from typing import Callable


def install_async_shutdown_handler(loop: asyncio.AbstractEventLoop, callback: Callable[[], None]) -> None:
    """Register `callback` to run on the asyncio event loop when the process receives
    SIGINT or SIGTERM. Cross-platform: uses `loop.add_signal_handler` on POSIX and
    `signal.signal` plus `loop.call_soon_threadsafe` on Windows, where the asyncio
    loops do not support direct signal handler registration.
    """
    if sys.platform == "win32":

        def _handler(*_args):
            loop.call_soon_threadsafe(callback)

        # Windows accepts SIGINT, SIGTERM, SIGBREAK among others for signal.signal.
        # SIGTERM is not delivered by the OS from external commands on Windows but
        # can still be raised in-process; registering it is harmless.
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)
    else:
        loop.add_signal_handler(signal.SIGINT, callback)
        loop.add_signal_handler(signal.SIGTERM, callback)
