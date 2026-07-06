"""Shared polling helpers for the AWS integration tests."""

from __future__ import annotations

import asyncio
import time
from typing import Callable

DEFAULT_WAIT_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.05


async def async_wait_until(
    predicate: Callable[[], bool], timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS, message: str = ""
) -> None:
    """Yield between predicate checks until the condition is true or the timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(message or f"condition not met within {timeout:.1f}s")
