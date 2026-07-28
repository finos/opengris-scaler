import asyncio
import enum
import logging
from typing import Awaitable, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


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

    logger.info(f"use event loop: {event_loop_type}")


def create_async_loop_routine(routine: Callable[[], Awaitable], seconds: int, swallow_routine_errors: bool = False):
    """create async loop routine,

    - if seconds is negative, means disable
    - 0 means looping without any wait, as fast as possible
    - positive number means execute routine every positive seconds, if passing 1 means run once every 1 seconds

    swallow_routine_errors: when True, an exception raised by the routine is logged per-iteration and the
    loop keeps running. This is for the SCHEDULER, which serves many peers and must survive a bug in any
    single routine -- including a message handler, since the binder routine dispatches inbound messages.
    An escape would otherwise propagate through asyncio.gather and take the whole scheduler down. It must
    stay False for the client/worker agents, which serve only themselves and should crash-and-restart."""

    async def loop():
        if seconds < 0:
            logger.info(f"{routine.__self__.__class__.__name__}: disabled")  # type: ignore[attr-defined]
            return

        logger.info(f"{routine.__self__.__class__.__name__}: started")  # type: ignore[attr-defined]
        try:
            while True:
                try:
                    await routine()
                except Exception:
                    if not swallow_routine_errors:
                        raise
                    routine_owner = routine.__self__.__class__.__name__  # type: ignore[attr-defined]
                    logger.exception(f"{routine_owner}: routine raised, continuing")
                await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass

        logger.info(f"{routine.__self__.__class__.__name__}: exited")  # type: ignore[attr-defined]

    return loop()


def run_task_forever(
    loop: asyncio.AbstractEventLoop, task: Awaitable[T], cleanup_callback: Optional[Callable[[], None]] = None
) -> T:
    """
    run task until completion and close the loop

    - loop: the event loop to run the task
    - task: the task to run until completion
    - cleanup_callback: optional callback to call before closing the loop
    """

    try:
        return loop.run_until_complete(task)
    finally:
        pending = asyncio.all_tasks(loop)
        for pending_task in pending:
            pending_task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        if cleanup_callback is not None:
            cleanup_callback()

        loop.close()
