import functools
import inspect
import logging
import typing

from scaler.utility.logging.scoped_logger import ScopedLogger


def log_function(
    level_number: int = 2, logging_level: int = logging.INFO
) -> typing.Callable[[typing.Callable[..., typing.Any]], typing.Callable[..., typing.Any]]:
    def decorator(func: typing.Callable[..., typing.Any]) -> typing.Callable[..., typing.Any]:
        @functools.wraps(func)
        def wrapper(*args: typing.Any, **kwargs: typing.Any):
            with ScopedLogger(
                f"execute {func.__name__} at {get_caller_location(level_number)}",
                logging_level=logging_level,
                logger=logging.getLogger(func.__module__),
            ):
                return func(*args, **kwargs)

        return wrapper

    return decorator


def get_caller_location(stack_level: int):
    caller = inspect.getframeinfo(inspect.stack()[stack_level][0])
    return f"{caller.filename}:{caller.lineno}"
