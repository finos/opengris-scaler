import datetime
from typing import Dict, List, Tuple

from scaler.protocol.python.common import TaskState
from scaler.ui.setting_page import Settings

COMPLETED_TASK_STATUSES = {
    TaskState.Success,
    TaskState.Canceled,
    TaskState.CanceledNotFound,
    TaskState.Failed,
    TaskState.FailedWorkerDied,
}


def format_timediff(a: datetime.datetime, b: datetime.datetime) -> float:
    return (b - a).total_seconds()


def format_worker_name(worker_name: str, cutoff: int = 15) -> str:
    return worker_name[:cutoff] + "+" if len(worker_name) > cutoff else worker_name


def get_bounds(now: datetime.datetime, start_time: datetime.datetime, settings: Settings) -> Tuple[int, int]:
    upper_range = now - start_time
    lower_range = upper_range - settings.stream_window

    bound_upper_seconds = max(upper_range.seconds, settings.stream_window.seconds)
    bound_lower_seconds = 0 if bound_upper_seconds == settings.stream_window.seconds else lower_range.seconds

    return bound_lower_seconds, bound_upper_seconds


def make_taskstream_ticks(lower_bound: int, upper_bound: int) -> List[int]:
    distance = (upper_bound - lower_bound) // 6
    return list(range(lower_bound, upper_bound + 1, distance))


def make_memory_ticks(max_bytes: int) -> Tuple[List[int], List[str]]:
    units = ["B", "KB", "MB", "GB", "TB"]
    vals: List[int] = [0]
    texts: List[str] = ["0"]
    v = 1
    i = 0
    # ensure at least up to 1GB on empty data
    target = max(1024 * 1024 * 1024, max_bytes)
    while i < len(units) and v <= target:
        vals.append(v)
        texts.append(f"1{units[i]}")
        v *= 1024
        i += 1
    return vals, texts


def make_tick_text(window_length: int) -> List[int]:
    upper = 0
    lower = -1 * window_length
    distance = (upper - lower) // 6
    return list(range(lower, upper + 1, distance))


def display_capabilities(capabilities: Dict[str, int]) -> str:
    if not capabilities or len(capabilities) == 0:
        return "<no capabilities>"

    # Capabilities is just the keys, value is ignored.
    return " & ".join(sorted(capabilities.keys()))
