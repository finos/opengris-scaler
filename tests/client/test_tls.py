import pathlib
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.security import SecurityConfig
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name

# Reuse the self-signed certificate/key pair shipped for the C++ OpenSSL tests.
_OPENSSL_TEST_DIR = pathlib.Path(__file__).resolve().parents[1] / "cpp" / "wrapper" / "openssl"
_CERT_CHAIN = str(_OPENSSL_TEST_DIR / "sample_cert.pem")
_PRIVATE_KEY = str(_OPENSSL_TEST_DIR / "sample_private_key.pem")


def increment(value: int) -> int:
    return value + 1


class TestClientTLS(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        scheduler_address = f"tls://127.0.0.1:{get_available_tcp_port()}"
        object_storage_address = f"tls://127.0.0.1:{get_available_tcp_port()}"

        self.combo = SchedulerClusterCombo(
            n_workers=1,
            address=scheduler_address,
            object_storage_address=object_storage_address,
            event_loop="builtin",
            security_config=SecurityConfig(tls_cert=_CERT_CHAIN, tls_key=_PRIVATE_KEY),
        )
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    @unittest.skip("OSS fail with EPROTO when started as a Combo")
    def test_submit(self):
        self.assertTrue(self.address.startswith("tls://"))

        with Client(self.address) as client:
            self.assertEqual(client.submit(increment, 41).result(), 42)


if __name__ == "__main__":
    unittest.main()
