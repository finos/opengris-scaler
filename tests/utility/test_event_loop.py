import asyncio
import unittest

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
    """A routine that raises must not take its loop down when the loop belongs to the scheduler: the
    exception would escape to asyncio.gather and tear down every connected client and worker with it."""

    def test_routine_error_is_swallowed_and_loop_continues(self):
        def behavior(call_number: int) -> None:
            if call_number == 1:
                raise ValueError("bug in a routine")
            if call_number >= 3:
                raise asyncio.CancelledError()  # normal shutdown path; ends the loop once we've proven it continued

        routine = _Routine(behavior)
        # swallow_routine_errors=True is the scheduler's mode: one bad iteration must not kill the loop.
        asyncio.new_event_loop().run_until_complete(
            create_async_loop_routine(routine.routine, 0, swallow_routine_errors=True)
        )

        self.assertEqual(routine.calls, 3, "the loop must run again after a routine raised, not die on it")

    def test_routine_error_propagates_by_default(self):
        # Default is the client/worker agent mode: they serve only themselves, so a bug should surface as a
        # clean crash-and-restart rather than an endless loop over a broken routine.
        def behavior(_call_number: int) -> None:
            raise ValueError("real bug")

        routine = _Routine(behavior)
        with self.assertRaises(ValueError):
            asyncio.new_event_loop().run_until_complete(create_async_loop_routine(routine.routine, 0))


if __name__ == "__main__":
    unittest.main()
