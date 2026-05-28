"""Object-storage parity test: ``YMQSyncObjectStorageConnector`` against
both ``tcp://`` and ``ws://`` C++ ``ObjectStorageServer`` instances.

The wasm shim is exercised separately (``test_ymq_wasm.py`` /
``test_ymq_parity.py``); this test confirms that the C++ object storage
server's ``ws://`` bind path (added by the websockets->wasm merge) accepts
the same wire protocol the native YMQ transport uses, and that the public
``YMQSyncObjectStorageConnector`` produces byte-equal results across both
transports.

If wasm and native ever diverge at the wire level, the matching byte-for-byte
assertions here catch it.
"""

import unittest

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.types.address import AddressConfig
from scaler.io.network_backends import YMQNetworkBackend
from scaler.utility.identifiers import ClientID, ObjectID
from scaler.utility.network_util import get_available_tcp_port


def _start_server(address: AddressConfig) -> ObjectStorageServerProcess:
    server = ObjectStorageServerProcess(
        bind_address=address,
        identity="ObjectStorageServer",
        logging_paths=("/dev/stdout",),
        logging_config_file=None,
        logging_level="WARNING",
    )
    server.start()
    server.wait_until_ready()
    return server


def _stop_server(server: ObjectStorageServerProcess) -> None:
    server.kill()
    server.join()


class ObjectStorageWsParityTest(unittest.TestCase):
    """Exercise the same operations against ``tcp://`` and ``ws://`` servers."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._tcp_address = AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}")
        cls._ws_address = AddressConfig.from_string(f"ws://127.0.0.1:{get_available_tcp_port()}")

        cls._tcp_server = _start_server(cls._tcp_address)
        cls._ws_server = _start_server(cls._ws_address)

        cls._backend = YMQNetworkBackend(num_threads=1)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._backend.destroy()
        _stop_server(cls._tcp_server)
        _stop_server(cls._ws_server)

    def _run_session(self, address: AddressConfig) -> bytes:
        connector = self._backend.create_sync_object_storage_connector(identity=b"parity-client", address=address)
        try:
            object_id = ObjectID.generate_object_id(ClientID(b"parity-client"))
            payload = b"parity-payload-" + b"x" * 1024
            connector.set_object(object_id, payload)
            fetched = bytes(connector.get_object(object_id))
            self.assertEqual(fetched, payload)
            self.assertTrue(connector.delete_object(object_id))
            self.assertFalse(connector.delete_object(object_id))
            return fetched
        finally:
            connector.destroy()

    def test_tcp_and_ws_yield_identical_payloads(self) -> None:
        tcp_result = self._run_session(self._tcp_address)
        ws_result = self._run_session(self._ws_address)
        self.assertEqual(tcp_result, ws_result)


if __name__ == "__main__":
    unittest.main()
