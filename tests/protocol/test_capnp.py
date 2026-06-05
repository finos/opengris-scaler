"""Tests for the capnp C extension: lazy deserialization, zero-copy buffer
semantics, union variant resolution, and error paths."""

import gc
import sys
import unittest

from scaler.io.utility import deserialize, serialize
from scaler.protocol.capnp import Message, StateTask, TaskState


class TestCapnp(unittest.TestCase):
    def test_struct_from_bytes_reads_from_original_buffer(self):
        # Prove zero-copy: deserialize from a bytearray, mutate the buffer
        # before accessing any field, then verify the field reflects the mutation.
        inactive_bytes = StateTask(state=TaskState.inactive, taskId=b"t", functionName=b"f", worker=b"w").to_bytes()
        success_bytes = StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w").to_bytes()
        self.assertEqual(len(inactive_bytes), len(success_bytes))

        buf = bytearray(inactive_bytes)
        st = StateTask.from_bytes(buf)
        buf[:] = success_bytes
        self.assertEqual(st.state, TaskState.success)

    def test_lazy_struct_keeps_buffer_alive(self):
        # The lazy struct stores a memoryview of the source buffer as
        # _capnp_source, keeping the underlying bytes alive even after the
        # caller's reference is dropped.  Field access must still succeed.
        buf = bytearray(StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w").to_bytes())
        st = StateTask.from_bytes(buf)
        del buf
        gc.collect()
        self.assertEqual(st.state, TaskState.success)

    def test_lazy_union_to_bytes_round_trips(self):
        # to_bytes() on a lazily-deserialized union (capnp_union_to_bytes path)
        # must produce a valid, re-deserializable payload identical to the
        # original wire bytes.
        original = StateTask(state=TaskState.success, taskId=b"task", functionName=b"func", worker=b"w")
        wire = serialize(original)
        lazy_msg = Message.from_bytes(wire)
        result = deserialize(lazy_msg.to_bytes())
        assert isinstance(result, StateTask)
        self.assertEqual(result.state, original.state)
        self.assertEqual(result.taskId, original.taskId)
        self.assertEqual(result.functionName, original.functionName)
        self.assertEqual(result.worker, original.worker)

    def test_union_which_is_lazy(self):
        # _variant_name must not be pre-set on the shell at from_bytes time.
        # which() resolves from the live buffer on first call, consistent with
        # how field values work.
        original = StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w")
        msg = Message.from_bytes(serialize(original))
        self.assertFalse(hasattr(msg, "_variant_name"))
        self.assertEqual(msg.which(), "stateTask")
        self.assertTrue(hasattr(msg, "_variant_name"))

    def test_inactive_union_field_raises_attribute_error(self):
        # Accessing a union field that is not the active variant must raise
        # AttributeError.  This exercises the load_struct_field inner check
        # that replaced the redundant outer check in capnp_union_get_attr.
        original = StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w")
        msg = Message.from_bytes(serialize(original))
        self.assertEqual(msg.which(), "stateTask")
        with self.assertRaises(AttributeError):
            _ = msg.task

    def test_from_bytes_length_not_multiple_of_eight_raises(self):
        # A buffer whose length is not a multiple of 8 is not a valid Cap'n Proto
        # flat array; from_bytes must raise ValueError before touching the data.
        valid = StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w").to_bytes()
        self.assertEqual(len(valid) % 8, 0, "baseline payload must already be word-aligned")
        with self.assertRaises(ValueError):
            StateTask.from_bytes(valid[:-1])

    def test_nested_lazy_path_traversal(self):
        # Accessing a field on a nested lazy struct exercises two-level path
        # replay: Message shell (path=()), stateTask shell (path=("stateTask",)),
        # then field read navigates the full path from the root each time.
        original = StateTask(state=TaskState.success, taskId=b"task", functionName=b"func", worker=b"w")
        msg = Message.from_bytes(serialize(original))
        self.assertEqual(msg.stateTask.taskId, b"task")
        self.assertEqual(msg.stateTask.functionName, b"func")

    def test_union_init_does_not_leak_variant_name(self):
        # Constructing a union struct stamps _variant_name on the instance. The
        # string returned by PyUnicode_FromString must be handed to SetAttrString
        # without an extra reference: SetAttrString increfs (it does not steal), so
        # passing the freshly-created reference straight in leaks one ref of the
        # variant name on every construction (i.e. on every serialize()). The only
        # live references to the string are the instance dict and the temporary
        # created for the getrefcount() call, so a correct implementation reads 2.
        msg = Message(stateTask=StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w"))
        # _variant_name is an internal runtime attribute (not in the type stub).
        self.assertEqual(sys.getrefcount(getattr(msg, "_variant_name")), 2)


if __name__ == "__main__":
    unittest.main()
