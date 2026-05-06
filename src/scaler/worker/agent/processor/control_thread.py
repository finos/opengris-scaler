import threading
from multiprocessing.synchronize import Event as MultiprocessingEvent
from typing import Callable

from scaler.utility import pending_call

# Poll interval for the multiprocessing.Event watch loop. multiprocessing.Event
# does not expose a wait-any primitive across its handles, and the underlying
# semaphore handle types differ between Linux and Windows; a short timed wait
# is the simplest cross-platform approach. Wakeups while idle are cheap.
_EVENT_POLL_INTERVAL_SECONDS = 0.05


class ProcessorControlThread(threading.Thread):
    """Daemon thread inside a Processor subprocess that translates
    parent-issued ``multiprocessing.Event`` requests into main-thread
    callbacks via ``pending_call.schedule``.

    The trampoline scheduled by ``pending_call.schedule`` runs at the same
    safe points as Python signal handlers (next CPython eval-breaker tick,
    GIL held), so semantics match the previous POSIX SIGUSR1/SIGTERM design.
    Used on both POSIX and Windows since SIGUSR1 has no Windows equivalent
    and unifying simplifies the code path.
    """

    def __init__(
        self,
        suspend_event: MultiprocessingEvent,
        terminate_event: MultiprocessingEvent,
        on_suspend: Callable[[], None],
        on_terminate: Callable[[], None],
    ) -> None:
        super().__init__(name="processor-control", daemon=True)
        self._suspend_event = suspend_event
        self._terminate_event = terminate_event
        self._on_suspend = on_suspend
        self._on_terminate = on_terminate

    def run(self) -> None:
        while not self._terminate_event.is_set():
            if self._suspend_event.wait(timeout=_EVENT_POLL_INTERVAL_SECONDS):
                # Clear before scheduling so a subsequent suspend() can re-arm.
                # ProcessorHolder.suspend asserts the processor is not already
                # suspended, so this is safe (no concurrent re-set racing the clear).
                self._suspend_event.clear()
                pending_call.schedule(self._on_suspend)

        pending_call.schedule(self._on_terminate)
