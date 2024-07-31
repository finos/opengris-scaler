import datetime
from typing import List, Tuple

from scaler.ui.setting_page import Settings


def format_timediff(a: datetime.datetime, b: datetime.datetime) -> float:
    return (b - a).total_seconds()


def format_worker_name(worker_name: str) -> str:
    pid, host, hash_code = worker_name.split("|")
    return f"{host}|{pid}"


def get_bounds(now: datetime.datetime, start_time: datetime.datetime, settings: Settings) -> Tuple[int, int]:
    upper_range = now - start_time
    lower_range = upper_range - settings.stream_window

    bound_upper_seconds = max(upper_range.seconds, settings.stream_window.seconds)
    bound_lower_seconds = 0 if bound_upper_seconds == settings.stream_window.seconds else lower_range.seconds

    return bound_lower_seconds, bound_upper_seconds


def make_ticks(lower_bound: int, upper_bound: int) -> List[int]:
    distance = (upper_bound - lower_bound) // 6
    return list(range(lower_bound, upper_bound + 1, distance))


def make_tick_text(window_length: int) -> List[int]:
    upper = 0
    lower = -1 * window_length
    distance = (upper - lower) // 6
    return list(range(lower, upper + 1, distance))
