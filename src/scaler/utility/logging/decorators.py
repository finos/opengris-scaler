import functools
import inspect
import logging
import typing

from scaler.utility.logging.scoped_logger import ScopedLogger


def log_function(level_number: int = 2, logging_level: int = logging.INFO) -> typing.Callable:
    def decorator(func: typing.Callable) -> typing.Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with ScopedLogger(
                f"execute {func.__name__} at {get_caller_location(level_number)}", logging_level=logging_level
            ):
                return func(*args, **kwargs)

        return wrapper

    return decorator


def get_caller_location(stack_level: int):
    caller = inspect.getframeinfo(inspect.stack()[stack_level][0])
    return f"{caller.filename}:{caller.lineno}"
