import time

from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import TimeoutManager


class VanillaTimeoutManager(Looper, TimeoutManager):
    def __init__(self, death_timeout_seconds: int):
        self._death_timeout_seconds = death_timeout_seconds
        self._last_seen_time = time.time()

    def update_last_seen_time(self):
        self._last_seen_time = time.time()

    async def routine(self):
        if (time.time() - self._last_seen_time) < self._death_timeout_seconds:
            return

        raise TimeoutError("timeout when connect to scheduler, quiting")
