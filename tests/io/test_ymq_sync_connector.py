import gc
import unittest
from unittest import mock

from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError
from scaler.io.ymq_sync_connector import YMQSyncConnector


class YMQSyncConnectorConnectFailureTest(unittest.TestCase):
    def test_a_failed_connect_is_collected_without_an_error(self) -> None:
        """A connector whose connect raised is garbage collected without its destructor raising."""
        connect_error = ConnectorSocketClosedByRemoteEndError(
            ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd, "Retried times has reached maximum"
        )

        with mock.patch("scaler.io.ymq_sync_connector.ConnectorSocket.connect", side_effect=connect_error):
            with mock.patch("sys.unraisablehook") as unraisable_hook:
                with self.assertRaises(ConnectorSocketClosedByRemoteEndError):
                    YMQSyncConnector(
                        context=mock.Mock(),
                        identity=b"test-connector",
                        address=AddressConfig.from_string("tcp://127.0.0.1:0"),
                    )
                gc.collect()

        unraisable_hook.assert_not_called()
