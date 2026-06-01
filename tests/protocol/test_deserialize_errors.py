"""Coverage for issue #761: unrecognised enum ordinals on the wire surface as a
Python ValueError, not a process abort. With lazy deserialization the error is
deferred to first field access rather than raised at from_bytes / deserialize
time, but is still guaranteed to surface before the bad value can be used."""

import unittest

from scaler.io.utility import deserialize
from scaler.protocol.capnp import StateTask, TaskState
from scaler.utility.exceptions import CapnpDeserializationError


def _state_task_bytes_with_state_ordinal(ordinal: int) -> bytes:
    """Build a StateTask wire payload, then patch the encoded TaskState ordinal
    to `ordinal`. The state field is laid out at a fixed offset in the data
    section of the StateTask struct, so we can rewrite it directly."""
    msg = StateTask(state=TaskState.inactive, taskId=b"t", functionName=b"f", worker=b"w").to_bytes()
    # Locate the encoded ordinal byte (currently 0 for `inactive`) by searching
    # for a known marker pair: the leading bytes of taskId/functionName/worker
    # are easy to keep stable; we set the state field explicitly to inactive so
    # there is exactly one byte equal to 0 in the struct's first data word that
    # we want to bump. Easier: serialize once with success (=4), once with the
    # target ordinal patched in. We mutate the byte that differs from a baseline.
    baseline = StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w").to_bytes()
    diff_indices = [i for i in range(min(len(msg), len(baseline))) if msg[i] != baseline[i]]
    if len(diff_indices) != 1:
        raise RuntimeError(f"unexpected diff between baseline and target: {diff_indices}")
    out = bytearray(msg)
    out[diff_indices[0]] = ordinal
    return bytes(out)


class TestDeserializeUnknownEnumOrdinal(unittest.TestCase):
    def test_deserialize_known_ordinal_succeeds(self):
        msg = StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w")
        # deserialize() expects a Message-wrapped payload; build a Message envelope.
        from scaler.io.utility import serialize

        payload = serialize(msg)
        decoded = deserialize(payload)
        assert isinstance(decoded, StateTask)
        self.assertEqual(decoded.state, TaskState.success)

    def test_struct_from_bytes_unknown_ordinal_raises(self):
        # Deserialization succeeds (lazy); the ValueError is raised on first
        # access of the invalid enum field, before the value can be used.
        bad_payload = _state_task_bytes_with_state_ordinal(99)
        st = StateTask.from_bytes(bad_payload)
        with self.assertRaises(ValueError):
            _ = st.state

    def test_deserialize_unknown_ordinal_raises_on_field_access(self):
        # Wrap StateTask in a Message envelope, then patch the inner state ordinal.
        # deserialize() itself succeeds (lazy); the ValueError surfaces when the
        # caller first reads the invalid enum field.
        from scaler.io.utility import serialize

        good = serialize(StateTask(state=TaskState.inactive, taskId=b"t", functionName=b"f", worker=b"w"))
        baseline = serialize(StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w"))
        diff_indices = [i for i in range(min(len(good), len(baseline))) if good[i] != baseline[i]]
        if not diff_indices:
            self.skipTest("could not locate enum ordinal byte in Message envelope")
        bad = bytearray(good)
        bad[diff_indices[-1]] = 99
        result = deserialize(bytes(bad))
        assert isinstance(result, StateTask)
        with self.assertRaises(ValueError):
            _ = result.state

    def test_deserialize_translates_malformed_buffer(self):
        # Random garbage triggers kj::Exception inside FlatArrayMessageReader,
        # which the C++ catches and surfaces as RuntimeError, which the Python
        # wrapper translates to CapnpDeserializationError.
        with self.assertRaises(CapnpDeserializationError):
            deserialize(b"not a real capnp payload xxxxxxxxxxxxxxxxxxxxxxxxx")


if __name__ == "__main__":
    unittest.main()
