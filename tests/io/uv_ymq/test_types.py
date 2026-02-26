import unittest

from scaler.io.uv_ymq import Address, AddressType, IOContext


class TestTypes(unittest.TestCase):
    def test_address(self):
        addr = Address("tcp://127.0.0.1:9000")
        self.assertEqual(addr.type, AddressType.TCP)
        self.assertEqual(repr(addr), "tcp://127.0.0.1:9000")

        addr = Address("tcp://::1:9000")
        self.assertEqual(addr.type, AddressType.TCP)
        self.assertEqual(repr(addr), "tcp://::1:9000")

        addr = Address("ipc://my_socket")
        self.assertEqual(addr.type, AddressType.IPC)
        self.assertEqual(str(addr), "ipc://my_socket")

        with self.assertRaises(ValueError):
            Address("invalid://address")

    def test_io_context(self):
        ctx = IOContext()
        self.assertEqual(ctx.num_threads, 1)

        ctx = IOContext(num_threads=3)
        self.assertEqual(ctx.num_threads, 3)
