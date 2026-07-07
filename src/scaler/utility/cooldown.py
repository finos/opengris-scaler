import time
from typing import Optional


class Cooldown:
    """A restartable cooldown timer.

    Call `start_if_not_running` to arm the timer if it is not already running, then poll
    `remaining_seconds` to check how much time is left. `reset` disarms the timer.

    Args:
        duration_seconds: How long the timer runs once armed. None disables the timer
            entirely: it never arms, and `remaining_seconds` always returns None.
    """

    def __init__(self, duration_seconds: Optional[float]) -> None:
        self._duration_seconds = duration_seconds
        self._started_at: Optional[float] = None

    def start_if_not_running(self) -> None:
        if self._duration_seconds is None:
            return
        if self._started_at is None:
            self._started_at = time.time()

    def reset(self) -> None:
        self._started_at = None

    def remaining_seconds(self) -> Optional[float]:
        """Return None if disabled, not running, or already elapsed, else the seconds left."""
        if self._duration_seconds is None or self._started_at is None:
            return None
        remaining = self._duration_seconds - (time.time() - self._started_at)
        return remaining if remaining > 0 else None

    @property
    def is_running(self) -> bool:
        return self._started_at is not None
