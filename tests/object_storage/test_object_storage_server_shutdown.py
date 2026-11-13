import socket
import struct
import time
import unittest

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.types.address import AddressConfig
from scaler.io.ymq import MAGIC_STRING, IOContext
from scaler.io.ymq_sync_object_storage_connector import YMQSyncObjectStorageConnector
from scaler.protocol.capnp import ObjectRequestHeader
from scaler.protocol.helpers import to_capnp_object_id
from scaler.utility.identifiers import ClientID, ObjectID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name

# More than loopback's socket buffers hold, so a response to a client that does not read stays in flight.
LARGE_OBJECT_BYTES = 64 * 1024 * 1024
RESPONSE_SETTLE_SECONDS = 0.5
EXIT_TIMEOUT_SECONDS = 10


def _ymq_frame(payload: bytes) -> bytes:
    return struct.pack("<Q", len(payload)) + payload


class TestObjectStorageServerShutdown(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_terminates_while_a_client_has_stopped_reading(self) -> None:
        """SIGTERM stops the server even with a large response in flight to a client that stopped reading."""
        host = "127.0.0.1"
        port = get_available_tcp_port()
        address = AddressConfig.from_string(f"tcp://{host}:{port}")

        server = ObjectStorageServerProcess(
            bind_address=address,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_level="INFO",
            logging_config_file=None,
        )
        server.start()
        self.addCleanup(server.join)
        self.addCleanup(server.kill)
        server.wait_until_ready()

        object_id = ObjectID.generate_object_id(ClientID.generate_client_id("test"))
        writer = YMQSyncObjectStorageConnector(IOContext(), b"writer", address)
        writer.set_object(object_id, b"x" * LARGE_OBJECT_BYTES)
        writer.destroy()

        get_request = ObjectRequestHeader(
            objectID=to_capnp_object_id(object_id),
            payloadLength=LARGE_OBJECT_BYTES,
            requestID=0,
            requestType=ObjectRequestHeader.ObjectRequestType.getObject,
        )

        with socket.create_connection((host, port)) as stalled_reader:
            stalled_reader.sendall(MAGIC_STRING + _ymq_frame(b"stalled-reader") + _ymq_frame(get_request.to_bytes()))
            time.sleep(RESPONSE_SETTLE_SECONDS)

            server.terminate()
            server.join(EXIT_TIMEOUT_SECONDS)

            self.assertFalse(server.is_alive(), "the server waits on a client that stopped reading")
