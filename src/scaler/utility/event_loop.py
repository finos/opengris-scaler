import asyncio
import enum
import logging
from typing import Awaitable, Callable, Optional, TypeVar

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError

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


def create_async_loop_routine(routine: Callable[[], Awaitable], seconds: int, swallow_peer_departed: bool = False):
    """create async loop routine,

    - if seconds is negative, means disable
    - 0 means looping without any wait, as fast as possible
    - positive number means execute routine every positive seconds, if passing 1 means run once every 1 seconds

    swallow_peer_departed: when True, a ConnectorSocketClosedByRemoteEndError raised by the routine is
    swallowed per-iteration and the loop keeps running. This is for the SCHEDULER, whose timer loops send
    to many peers (workers/clients) that come and go: one departed peer must not escape to asyncio.gather
    and tear the whole scheduler down. It must stay False for the client/worker agents -- there the error
    means their single scheduler peer is gone, and it needs to propagate so the agent can surface a clear
    disconnect (e.g. DisconnectedError) instead of silently looping against a dead connection."""

    async def loop():
        if seconds < 0:
            logger.info(f"{routine.__self__.__class__.__name__}: disabled")  # type: ignore[attr-defined]
            return

        logger.info(f"{routine.__self__.__class__.__name__}: started")  # type: ignore[attr-defined]
        try:
            while True:
                try:
                    await routine()
                except ConnectorSocketClosedByRemoteEndError:
                    if not swallow_peer_departed:
                        raise
                    # A peer departed and this routine's send raced the disconnect. Same policy as the
                    # binder receive loop: peer-gone is routine, handled by the controllers' own
                    # timeout/cleanup paths. A real peer death is still caught by the heartbeat-based
                    # timeouts, not by this send error.
                    routine_owner = routine.__self__.__class__.__name__  # type: ignore[attr-defined]
                    logger.info(f"{routine_owner}: peer departed mid-routine, continuing")
                except Exception:
                    if not swallow_peer_departed:
                        raise
                    # Scheduler loops (swallow_peer_departed=True) must survive a bug in any single routine
                    # -- including a message handler, since the binder routine dispatches inbound messages.
                    # An unhandled exception here would otherwise propagate through asyncio.gather and take
                    # the whole scheduler down. Log the traceback and keep looping so the scheduler stays
                    # alive; agents keep swallow_peer_departed=False and still crash-and-restart cleanly.
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
