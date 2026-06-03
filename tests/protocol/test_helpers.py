"""Tests that capabilities survive a Cap'n Proto serialise/deserialise round-trip.

Verifies the behaviour of ``dict_to_capabilities`` / ``capabilities_to_dict`` against
the wire format rather than the helper internals, so an alternative implementation
that preserves the same round-trip semantics would still satisfy these tests.
"""

import unittest

from scaler.protocol.capnp import StateTask, StateWorker, Task, TaskState, WorkerState
from scaler.protocol.helpers import capabilities_to_dict, dict_to_capabilities


class TestCapabilitiesWireRoundTrip(unittest.TestCase):
    def test_capabilities_survive_wire_round_trip(self):
        original = {"gpu": 4, "linux": -1}

        sent = StateWorker(workerId=b"w1", state=WorkerState.connected, capabilities=dict_to_capabilities(original))
        received = StateWorker.from_bytes(sent.to_bytes())

        self.assertEqual(capabilities_to_dict(received.capabilities), original)

    def test_empty_capabilities_survive_wire_round_trip(self):
        sent = StateWorker(workerId=b"w1", state=WorkerState.connected, capabilities=dict_to_capabilities({}))
        received = StateWorker.from_bytes(sent.to_bytes())

        self.assertEqual(capabilities_to_dict(received.capabilities), {})

    def test_capabilities_forwarded_from_task_to_state_task(self):
        # The scheduler builds a StateTask using capabilities sourced from a Task it
        # received off the wire. The values must reach the wire intact when forwarded.
        original = {"gpu": 1}
        task = Task.from_bytes(
            Task(
                taskId=b"t" * 32,
                source=b"src1",
                metadata=b"",
                funcObjectId=b"f" * 32,
                functionArgs=[],
                capabilities=dict_to_capabilities(original),
            ).to_bytes()
        )

        forwarded = StateTask.from_bytes(
            StateTask(
                taskId=task.taskId,
                functionName=b"fn",
                state=TaskState.running,
                worker=b"w",
                capabilities=dict_to_capabilities(task.capabilities),
                metadata=b"",
            ).to_bytes()
        )

        self.assertEqual(capabilities_to_dict(forwarded.capabilities), original)


if __name__ == "__main__":
    unittest.main()
