"""Tests that ``__repr__`` on Cap'n Proto protocol structs produces a readable,
schema-ordered ``ClassName(field=value, ...)`` form instead of the default
``<ClassName object at 0x...>``. Regression coverage for issue #731.
"""

import unittest

from scaler.protocol.capnp import (
    ClientHeartbeatEcho,
    Message,
    ObjectStorageAddress,
    Resource,
    StateTask,
    Task,
    TaskCancel,
    TaskCapability,
    TaskState,
    WorkerManagerHeartbeatEcho,
)


class TestCapnpStructRepr(unittest.TestCase):
    def test_simple_struct_matches_issue_example(self):
        self.assertEqual(
            repr(ObjectStorageAddress(host="127.0.0.1", port=9000, scheme="tcp")),
            "ObjectStorageAddress(host='127.0.0.1', port=9000, scheme='tcp')",
        )

    def test_str_falls_back_to_repr(self):
        addr = ObjectStorageAddress(host="127.0.0.1", port=9000, scheme="tcp")
        self.assertEqual(str(addr), repr(addr))

    def test_field_order_is_schema_order_not_kwarg_order(self):
        # Schema declares host, port, scheme (in that order). Repr must reflect
        # that regardless of how the caller passes kwargs.
        addr1 = ObjectStorageAddress(host="a", port=1, scheme="tcp")
        addr2 = ObjectStorageAddress(scheme="tcp", port=1, host="a")
        self.assertEqual(repr(addr1), repr(addr2))
        self.assertEqual(repr(addr1), "ObjectStorageAddress(host='a', port=1, scheme='tcp')")

    def test_missing_optional_fields_are_omitted(self):
        # Fields not passed at construction do not appear in __dict__, so the
        # repr only lists what was assigned (matching to_bytes/HasAttr semantics).
        self.assertEqual(repr(ObjectStorageAddress(host="h")), "ObjectStorageAddress(host='h')")

    def test_empty_struct(self):
        self.assertEqual(repr(WorkerManagerHeartbeatEcho()), "WorkerManagerHeartbeatEcho()")

    def test_bytes_int_intenum_fields(self):
        state_task = StateTask(taskId=b"t1", functionName=b"fn", state=TaskState.success, worker=b"w1")
        rendered = repr(state_task)
        self.assertTrue(rendered.startswith("StateTask("))
        self.assertIn("taskId=b't1'", rendered)
        self.assertIn("functionName=b'fn'", rendered)
        # IntEnum uses Python's default repr (e.g. <TaskState.success: 4>); we
        # just inherit that.
        self.assertIn("state=", rendered)
        self.assertIn("TaskState.success", rendered)
        self.assertIn("worker=b'w1'", rendered)

    def test_nested_struct_field_uses_its_own_repr(self):
        echo = ClientHeartbeatEcho(objectStorageAddress=ObjectStorageAddress(host="h", port=1, scheme="tcp"))
        self.assertEqual(
            repr(echo), "ClientHeartbeatEcho(objectStorageAddress=ObjectStorageAddress(host='h', port=1, scheme='tcp'))"
        )

    def test_list_field_of_structs_recurses(self):
        # functionArgs is a list of Task.Argument; capabilities is a list of TaskCapability.
        task = Task(
            taskId=b"t" * 4,
            source=b"src",
            metadata=b"",
            funcObjectId=b"f" * 4,
            functionArgs=[],
            capabilities=[TaskCapability(name="gpu", value=1)],
        )
        rendered = repr(task)
        self.assertTrue(rendered.startswith("Task("))
        self.assertIn("capabilities=[TaskCapability(name='gpu', value=1)]", rendered)
        self.assertIn("functionArgs=[]", rendered)

    def test_repr_is_idempotent_after_round_trip(self):
        # The first round-trip fills in defaults for any unspecified fields, so
        # repr(original) and repr(round_tripped) can legitimately differ. But
        # once an object has been deserialised, a second round-trip must
        # produce a byte-identical repr.
        original = StateTask(taskId=b"id", functionName=b"fn", state=TaskState.running, worker=b"w")
        once = StateTask.from_bytes(original.to_bytes())
        twice = StateTask.from_bytes(once.to_bytes())
        self.assertEqual(repr(once), repr(twice))

    def test_union_struct_shows_only_active_variant(self):
        # Message is a Cap'n Proto union. Only the active variant should appear,
        # and no internal _variant_name should leak.
        message = Message(taskCancel=TaskCancel(taskId=b"id"))
        rendered = repr(message)
        self.assertTrue(rendered.startswith("Message("))
        self.assertIn("taskCancel=TaskCancel(taskId=b'id')", rendered)
        self.assertNotIn("_variant_name", rendered)
        # Other Message variants must NOT appear.
        self.assertNotIn("taskResult=", rendered)
        self.assertNotIn("stateTask=", rendered)

    def test_resource_simple(self):
        self.assertEqual(repr(Resource(cpu=4, rss=1024)), "Resource(cpu=4, rss=1024)")


if __name__ == "__main__":
    unittest.main()
