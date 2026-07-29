"""``YMQSyncObjectStorageConnector`` must report a transport failure as an object storage failure.

A peer that aborts the connection mid-write cancels the send, which YMQ reports as ``SocketStopRequested``.
The read side has always translated YMQ errors into ``ObjectStorageException``; the write side used to let
them through raw, so a failed storage write surfaced to callers as whatever YMQ error they happened to
handle -- in the processor's case, as "agent connector stop requested", naming a connection that was fine.
"""

import unittest
from typing import Any
from unittest import mock

from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.ymq import SocketStopRequestedError
from scaler.io.ymq_sync_object_storage_connector import YMQSyncObjectStorageConnector
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ClientID, ObjectID


def _make_connector() -> Any:
    # Typed as Any: the tests reach the connector's mocked-out private socket.
    with mock.patch("scaler.io.ymq_sync_object_storage_connector.ConnectorSocket") as socket_class:
        socket_class.connect.return_value = mock.Mock()
        connector = YMQSyncObjectStorageConnector(
            context=mock.Mock(), identity=b"test-connector", address=AddressConfig.from_string("tcp://127.0.0.1:0")
        )

    return connector


class SyncObjectStorageConnectorSendFailureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = _make_connector()
        self.object_id = ObjectID.generate_object_id(ClientID.generate_client_id("test"))

    def tearDown(self) -> None:
        # the connector's __del__ would otherwise shut down the mocked socket during garbage collection
        self.connector._socket = None

    def test_a_canceled_write_is_reported_as_a_storage_failure(self) -> None:
        canceled = SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "connection aborted mid-write")
        self.connector._socket.send_message_sync.side_effect = canceled

        with self.assertRaises(ObjectStorageException) as context:
            self.connector.set_object(self.object_id, b"payload")

        self.assertIs(context.exception.__cause__, canceled, "the underlying YMQ error was dropped")

    def test_a_canceled_payload_write_is_reported_too(self) -> None:
        # The header is small enough to leave in a socket buffer, so a big payload is the write that fails.
        self.connector._socket.send_message_sync.side_effect = [
            None,
            SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "connection aborted mid-write"),
        ]

        with self.assertRaises(ObjectStorageException):
            self.connector.set_object(self.object_id, b"payload")

    def test_a_read_failure_is_still_reported_as_a_storage_failure(self) -> None:
        self.connector._socket.recv_message_sync.side_effect = SocketStopRequestedError(
            ymq.ErrorCode.SocketStopRequested, "socket shut down"
        )

        with self.assertRaises(ObjectStorageException):
            self.connector.set_object(self.object_id, b"payload")


if __name__ == "__main__":
    unittest.main()
