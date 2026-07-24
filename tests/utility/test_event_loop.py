import asyncio
import unittest

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, ErrorCode
from scaler.utility.event_loop import create_async_loop_routine


class _Routine:
    """Holds a bound ``routine`` method; create_async_loop_routine reads routine.__self__.__class__."""

    def __init__(self, behavior):
        self.calls = 0
        self._behavior = behavior

    async def routine(self):
        self.calls += 1
        self._behavior(self.calls)


class TestCreateAsyncLoopRoutine(unittest.TestCase):
    """A departed peer surfaces as ConnectorSocketClosedByRemoteEndError on the send. On a scheduler
    timer loop that would otherwise escape to asyncio.gather and tear the whole scheduler down, so the
    loop wrapper must swallow it per-iteration and keep looping -- mirroring the binder receive loop."""

    def test_departed_peer_error_is_swallowed_and_loop_continues(self):
        def behavior(call_number: int) -> None:
            if call_number == 1:
                raise ConnectorSocketClosedByRemoteEndError(ErrorCode.ConnectorSocketClosedByRemoteEnd, "peer gone")
            if call_number >= 3:
                raise asyncio.CancelledError()  # normal shutdown path; ends the loop once we've proven it continued

        routine = _Routine(behavior)
        # swallow_peer_departed=True is the scheduler's mode: a departed peer must not kill the loop.
        asyncio.new_event_loop().run_until_complete(
            create_async_loop_routine(routine.routine, 0, swallow_peer_departed=True)
        )

        self.assertEqual(routine.calls, 3, "the loop must run again after a departed-peer send, not die on it")

    def test_other_errors_still_propagate(self):
        # The swallow is narrow: a genuine bug must still surface, not be hidden as a routine peer departure.
        def behavior(_call_number: int) -> None:
            raise ValueError("real bug")

        routine = _Routine(behavior)
        with self.assertRaises(ValueError):
            asyncio.new_event_loop().run_until_complete(create_async_loop_routine(routine.routine, 0))

    def test_departed_peer_error_propagates_by_default(self):
        # Default (swallow_peer_departed=False) is the client/worker agent mode: their single peer is the
        # scheduler, so a departed-peer error must propagate for the agent to surface a clear disconnect,
        # not be swallowed into an endless loop against a dead connection.
        def behavior(_call_number: int) -> None:
            raise ConnectorSocketClosedByRemoteEndError(ErrorCode.ConnectorSocketClosedByRemoteEnd, "peer gone")

        routine = _Routine(behavior)
        with self.assertRaises(ConnectorSocketClosedByRemoteEndError):
            asyncio.new_event_loop().run_until_complete(create_async_loop_routine(routine.routine, 0))


if __name__ == "__main__":
    unittest.main()
