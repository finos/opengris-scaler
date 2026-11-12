import unittest

from scaler.io.utility import serialize
from scaler.protocol.capnp import ClientHeartbeatEcho, ObjectStorageAddress

# ObjectStorageAddress.port is a UInt16
OUT_OF_RANGE_PORT = 65536


class TestSerializeErrors(unittest.TestCase):
    def setUp(self) -> None:
        self.address = ObjectStorageAddress(host="127.0.0.1", port=OUT_OF_RANGE_PORT, scheme="tcp")

    def test_a_message_serialization_failure_names_the_rejected_value(self) -> None:
        """serialize() reports the Cap'n Proto reason and the value it rejected."""
        with self.assertRaisesRegex(RuntimeError, f"out-of-range.*value = {OUT_OF_RANGE_PORT}"):
            serialize(ClientHeartbeatEcho(objectStorageAddress=self.address))

    def test_a_struct_serialization_failure_names_the_rejected_value(self) -> None:
        """to_bytes() reports the Cap'n Proto reason and the value it rejected."""
        with self.assertRaisesRegex(RuntimeError, f"out-of-range.*value = {OUT_OF_RANGE_PORT}"):
            self.address.to_bytes()
