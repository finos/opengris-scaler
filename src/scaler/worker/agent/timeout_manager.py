import time
from typing import Callable

from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import TimeoutManager


class VanillaTimeoutManager(Looper, TimeoutManager):
    """Calls `on_timeout` once, when no peer message arrived for `death_timeout_seconds`."""

    def __init__(self, death_timeout_seconds: int, on_timeout: Callable[[], None]):
        self._death_timeout_seconds = death_timeout_seconds
        self._on_timeout = on_timeout
        self._last_seen_time = time.time()
        self._timed_out = False

    def update_last_seen_time(self):
        self._last_seen_time = time.time()

    async def routine(self):
        if self._timed_out or (time.time() - self._last_seen_time) < self._death_timeout_seconds:
            return

        self._timed_out = True
        self._on_timeout()
