"""Shared task callables for every backend.

The tasks are nested functions so cloudpickle serializes them BY VALUE -- none of the task images mount the
test module, so a module-level (by-reference) task would fail to import on the worker. ``square_tagged``
reports which unit ran it via ``SCALER_IT_MACHINE_ID or hostname``: the container backend sets that env per
machine, the floci backends set none, so the one expression covers both attribution styles.
"""

from __future__ import annotations

import dataclasses
from typing import Callable, Tuple


@dataclasses.dataclass(frozen=True)
class Tasks:
    square: Callable[[int], int]
    square_tagged: Callable[[int], Tuple[int, str]]


def make_tasks(task_seconds: float) -> Tasks:
    def square(value: int) -> int:
        import time

        time.sleep(task_seconds)
        return value * value

    def square_tagged(value: int) -> Tuple[int, str]:
        import os
        import socket
        import time

        time.sleep(task_seconds)
        return value * value, os.environ.get("SCALER_IT_MACHINE_ID") or socket.gethostname()

    return Tasks(square=square, square_tagged=square_tagged)
