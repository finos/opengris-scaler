import asyncio
import concurrent.futures
import logging
from typing import Any, Awaitable, Callable, Optional, TypeVar, Union

try:
    from typing import Concatenate, ParamSpec  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Concatenate, ParamSpec  # type: ignore[assignment]

from scaler.config.common.security import SecurityConfig
from scaler.io.ymq import TLSConfig

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


async def call_async(
    func: Callable[Concatenate[Callable[[Union[T, BaseException]], None], P], None],  # type: ignore
    *args: P.args,  # type: ignore
    **kwargs: P.kwargs,  # type: ignore
) -> T:
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def callback(result: Union[T, BaseException]):
        if loop.is_closed():
            return

        if isinstance(result, BaseException):
            loop.call_soon_threadsafe(_safe_set_exception, future, result)
        else:
            loop.call_soon_threadsafe(_safe_set_result, future, result)

    func(callback, *args, **kwargs)
    return await future


# about the ignore directives: mypy cannot properly handle typing extension's ParamSpec and Concatenate in python <=3.9
# these type hints are correctly understood in Python 3.10+
def call_sync(  # type: ignore[valid-type]
    func: Callable[Concatenate[Callable[[Union[T, BaseException]], None], P], None],  # type: ignore
    *args: P.args,  # type: ignore
    timeout: Optional[float] = None,
    **kwargs: P.kwargs,  # type: ignore
) -> T:  # type: ignore
    future: concurrent.futures.Future = concurrent.futures.Future()

    def callback(result: Union[T, BaseException]):
        if future.done():
            return

        if isinstance(result, BaseException):
            future.set_exception(result)
        else:
            future.set_result(result)

    func(callback, *args, **kwargs)
    return future.result(timeout)


def run_detached(
    awaitable: Awaitable[Any],
    description: str,
    on_done_callback: Optional[Callable[["asyncio.Task"], None]] = None,
) -> "asyncio.Task":
    """Schedule an awaitable on the running event loop without waiting for its completion.

    This turns an otherwise blocking asyncio operation into a fire-and-forget one.

    Failures are logged instead of being propagated, as the caller does not wait for the result.

    If provided, `on_done_callback` is called with the completed task once it finishes, including
    when it is cancelled. It is typically used to remove the task from the caller's pending set.

    The returned task must be kept referenced by the caller until completion, as asyncio only keeps weak references to
    running tasks. Callers are also responsible for cancelling it on teardown.
    """

    task = asyncio.ensure_future(awaitable)

    def on_done(completed: "asyncio.Task") -> None:
        if on_done_callback is not None:
            on_done_callback(completed)

        if completed.cancelled():
            return

        exception = completed.exception()
        if exception is not None:
            logger.debug(f"{description}: detached operation failed: {exception!r}")

    task.add_done_callback(on_done)

    return task


def _safe_set_result(future: asyncio.Future, result: Any) -> None:
    if future.done():
        return
    future.set_result(result)


def _safe_set_exception(future: asyncio.Future, exc: BaseException) -> None:
    if future.done():
        return
    future.set_exception(exc)


def to_tls_config(security_config: Optional[SecurityConfig]) -> Optional[TLSConfig]:
    """Convert a Scaler ``SecurityConfig`` into a YMQ ``TLSConfig``."""

    if security_config is None or not security_config.has_credentials():
        return None

    return TLSConfig(cert_chain=security_config.tls_cert, private_key=security_config.tls_key)
