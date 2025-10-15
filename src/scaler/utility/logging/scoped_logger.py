import datetime
import logging
import time
from typing import Optional


class ScopedLogger:
    def __init__(self, message: str, logging_level=logging.INFO):
        self.timer = TimedLogger(message=message, logging_level=logging_level)

    def __enter__(self):
        self.timer.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.timer.end()


class TimedLogger:
    def __init__(self, message: str, logging_level=logging.INFO):
        self.message = message
        self.logging_level = logging_level
        self.timer: Optional[int] = None

    def begin(self):
        self.timer = time.perf_counter_ns()
        logging.log(self.logging_level, f"beginning {self.message}")

    def end(self):
        elapsed = time.perf_counter_ns() - self.timer
        offset = datetime.timedelta(
            seconds=int(elapsed / 1e9), milliseconds=int(elapsed % 1e9 / 1e6), microseconds=int(elapsed % 1e6 / 1e3)
        )
        logging.log(self.logging_level, f"completed {self.message} in {offset}")
