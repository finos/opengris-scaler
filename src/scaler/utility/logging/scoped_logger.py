import datetime
import logging
import time
from types import TracebackType
from typing import Optional, Type

_DEFAULT_LOGGER = logging.getLogger(__name__)


class ScopedLogger:
    def __init__(self, message: str, logging_level: int = logging.INFO, logger: Optional[logging.Logger] = None):
        self.timer = TimedLogger(message=message, logging_level=logging_level, logger=logger)

    def __enter__(self):
        self.timer.begin()

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ):
        self.timer.end()


class TimedLogger:
    def __init__(self, message: str, logging_level: int = logging.INFO, logger: Optional[logging.Logger] = None):
        self.message = message
        self.logging_level = logging_level
        self.logger = logger if logger is not None else _DEFAULT_LOGGER
        self.timer: Optional[int] = None

    def begin(self):
        self.timer = time.perf_counter_ns()
        self.logger.log(self.logging_level, f"beginning {self.message}")

    def end(self):
        assert self.timer is not None, "end() needs a begin() first"
        elapsed = time.perf_counter_ns() - self.timer
        offset = datetime.timedelta(
            seconds=int(elapsed / 1e9), milliseconds=int(elapsed % 1e9 / 1e6), microseconds=int(elapsed % 1e6 / 1e3)
        )
        self.logger.log(self.logging_level, f"completed {self.message} in {offset}")
