import asyncio
import concurrent.futures
from typing import Any, Callable, Optional, TypeVar, Union

try:
    from typing import Concatenate, ParamSpec  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import ParamSpec, Concatenate  # type: ignore[assignment]


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


def _safe_set_result(future: asyncio.Future, result: Any) -> None:
    if future.done():
        return
    future.set_result(result)


def _safe_set_exception(future: asyncio.Future, exc: BaseException) -> None:
    if future.done():
        return
    future.set_exception(exc)
