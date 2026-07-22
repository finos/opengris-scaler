import threading
import unittest

from scaler.utility.exceptions import TaskExceptionNotSerializableError
from scaler.utility.serialization import deserialize_failure, serialize_failure


class TestSerializeFailure(unittest.TestCase):
    def test_picklable_exception_round_trips_unchanged(self):
        restored = deserialize_failure(serialize_failure(ValueError("boom")))
        self.assertIsInstance(restored, ValueError)
        self.assertEqual(str(restored), "boom")

    def test_unpicklable_exception_degrades_instead_of_raising(self):
        # An exception holding an unpicklable object (a lock) must not raise out of serialize_failure --
        # otherwise the worker's failure path itself throws, the processor dies, and the client sees an
        # opaque ProcessorDiedError instead of the real error.
        restored = deserialize_failure(serialize_failure(RuntimeError("bad", threading.Lock())))
        self.assertIsInstance(restored, TaskExceptionNotSerializableError)
        self.assertIn("RuntimeError", str(restored))
        self.assertIn("bad", str(restored))

    def test_locally_defined_exception_class_degrades(self):
        class LocallyDefinedError(Exception):
            pass

        restored = deserialize_failure(serialize_failure(LocallyDefinedError("nope")))
        self.assertIsInstance(restored, TaskExceptionNotSerializableError)
        self.assertIn("LocallyDefinedError", str(restored))

    def test_exception_with_broken_str_still_serializes(self):
        class BrokenStr(Exception):
            def __str__(self):
                raise RuntimeError("str blew up")

        # Even if str(exp) fails while building the degraded message, bytes must still be produced.
        restored = deserialize_failure(serialize_failure(BrokenStr(threading.Lock())))
        self.assertIsInstance(restored, TaskExceptionNotSerializableError)
        self.assertIn("BrokenStr", str(restored))


if __name__ == "__main__":
    unittest.main()
