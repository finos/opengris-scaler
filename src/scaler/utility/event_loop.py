import asyncio
import enum
import logging
from typing import Awaitable, Callable


class EventLoopType(enum.Enum):
    builtin = enum.auto()
    uvloop = enum.auto()

    @staticmethod
    def allowed_types():
        return {m.name for m in EventLoopType}


def register_event_loop(event_loop_type: str):
    if event_loop_type not in EventLoopType.allowed_types():
        raise TypeError(f"allowed event loop types are: {EventLoopType.allowed_types()}")

    event_loop_type_enum = EventLoopType[event_loop_type]
    if event_loop_type_enum == EventLoopType.uvloop:
        try:
            import uvloop  # noqa
        except ImportError:
            raise ImportError("please use pip install uvloop if try to use uvloop as event loop")

        uvloop.install()

    assert event_loop_type in EventLoopType.allowed_types()

    logging.info(f"use event loop: {event_loop_type}")


def create_async_loop_routine(routine: Callable[[], Awaitable], seconds: int):
    """create async loop routine,

    - if seconds is negative, means disable
    - 0 means looping without any wait, as fast as possible
    - positive number means execute routine every positive seconds, if passing 1 means run once every 1 seconds"""

    async def loop():
        if seconds < 0:
            logging.info(f"{routine.__self__.__class__.__name__}: disabled")  # type: ignore[attr-defined]
            return

        logging.info(f"{routine.__self__.__class__.__name__}: started")  # type: ignore[attr-defined]
        try:
            while True:
                await routine()
                await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass

        logging.info(f"{routine.__self__.__class__.__name__}: exited")  # type: ignore[attr-defined]

    return loop()


def run_task_forever(loop: asyncio.AbstractEventLoop, task: Awaitable[None]) -> None:
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()
