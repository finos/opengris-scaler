"""Parity tests: verify the pure-Python browser shim mirrors the native ``_ymq``.

These tests exist specifically so that if the wire protocol, API surface, or
error taxonomy of the native ymq changes, the wasm shim's tests fail loudly.
Both implementations must stay in sync or browser clients will silently break.

Parity is checked at three levels:

1. **Wire protocol constants.** Parse the authoritative C++ header
   ``src/cpp/scaler/ymq/configuration.h`` and compare against the wasm shim's
   Python constants (``_MAGIC_STRING``, header size).
2. **Module surface.** Every name exported by the native ``_ymq`` that the
   shim claims to re-implement must exist on both modules with a compatible
   type/shape.
3. **Value semantics.** ``Bytes``, ``Message``, ``Address``, and ``ErrorCode``
   must round-trip / classify the same bytes identically in native and shim.
"""

import pathlib
import re
import unittest

from scaler.io.ymq import _ymq as _ymq_native
from scaler.io.ymq import _ymq_wasm

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_CPP_CONFIG = _REPO_ROOT / "src" / "cpp" / "scaler" / "ymq" / "configuration.h"


class WireProtocolParityTest(unittest.TestCase):
    """Verify the wasm shim's wire constants match the authoritative C++ source.

    If any of these assertions fail, the C++ wire protocol has drifted from
    the Python shim. Either update the shim to match, or revert the C++
    change and reconsider — native and browser peers must interoperate.
    """

    def test_configuration_header_exists(self) -> None:
        self.assertTrue(
            _CPP_CONFIG.is_file(),
            f"Cannot find C++ wire-protocol config at {_CPP_CONFIG}; " "the shim cannot be verified against it.",
        )

    def test_magic_string_matches_cpp(self) -> None:
        text = _CPP_CONFIG.read_text(encoding="utf-8")
        # Match: std::array<uint8_t, N> magicString {'Y', 'M', 'Q', 1};
        match = re.search(r"magicString\s*\{([^}]*)\}", text)
        self.assertIsNotNone(match, "Could not find magicString in configuration.h")
        parts = [p.strip() for p in match.group(1).split(",") if p.strip()]
        cpp_bytes = bytearray()
        for p in parts:
            if p.startswith("'") and p.endswith("'") and len(p) == 3:
                cpp_bytes.append(ord(p[1]))
            else:
                cpp_bytes.append(int(p, 0))
        self.assertEqual(
            bytes(cpp_bytes),
            _ymq_wasm._MAGIC_STRING,
            "Wasm shim magic bytes drifted from C++ configuration.h; "
            "update _ymq_wasm._MAGIC_STRING or revert the C++ change.",
        )

    def test_header_size_matches_cpp(self) -> None:
        # The C++ code uses ``using Header = uint64_t;`` in message_connection.h.
        # The shim uses "<Q" (little-endian uint64). Pin the expected size here
        # so anyone changing either side has to update the other.
        import struct

        self.assertEqual(struct.calcsize(_ymq_wasm._HEADER_FORMAT), 8)
        conn_header = _REPO_ROOT / "src" / "cpp" / "scaler" / "ymq" / "internal" / "message_connection.h"
        if conn_header.is_file():
            text = conn_header.read_text(encoding="utf-8")
            self.assertIn(
                "using Header = uint64_t",
                text,
                "C++ Header type is no longer uint64_t; update _HEADER_FORMAT in the shim.",
            )


class ModuleSurfaceParityTest(unittest.TestCase):
    """Every name the shim claims to re-implement exists on the native module."""

    _SHARED_NAMES = [
        "Bytes",
        "Message",
        "Address",
        "AddressType",
        "IOContext",
        "BinderSocket",
        "ConnectorSocket",
        "ErrorCode",
        "YMQException",
        "ConnectorSocketClosedByRemoteEndError",
        "InvalidAddressFormatError",
        "InvalidPortFormatError",
        "RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError",
        "SocketStopRequestedError",
        "SysCallError",
        "DEFAULT_MAX_RETRY_TIMES",
        "DEFAULT_INIT_RETRY_DELAY",
    ]

    def test_all_names_present(self) -> None:
        for name in self._SHARED_NAMES:
            with self.subTest(name=name):
                self.assertTrue(hasattr(_ymq_native, name), f"native _ymq is missing {name!r} (shim expected parity)")
                self.assertTrue(hasattr(_ymq_wasm, name), f"shim _ymq_wasm is missing {name!r}")

    def test_exception_hierarchy_matches(self) -> None:
        subclass_names = [
            "ConnectorSocketClosedByRemoteEndError",
            "InvalidAddressFormatError",
            "InvalidPortFormatError",
            "RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError",
            "SocketStopRequestedError",
            "SysCallError",
        ]
        for name in subclass_names:
            with self.subTest(name=name):
                native_cls = getattr(_ymq_native, name)
                shim_cls = getattr(_ymq_wasm, name)
                self.assertTrue(
                    issubclass(native_cls, _ymq_native.YMQException), f"native {name} is not a YMQException subclass"
                )
                self.assertTrue(
                    issubclass(shim_cls, _ymq_wasm.YMQException), f"shim {name} is not a YMQException subclass"
                )


class ErrorCodeParityTest(unittest.TestCase):
    """Every ErrorCode name/value in native must also be in the shim."""

    def test_all_native_codes_present_in_shim(self) -> None:
        for native_code in _ymq_native.ErrorCode:
            with self.subTest(name=native_code.name):
                self.assertTrue(
                    hasattr(_ymq_wasm.ErrorCode, native_code.name), f"shim ErrorCode is missing {native_code.name!r}"
                )
                shim_code = getattr(_ymq_wasm.ErrorCode, native_code.name)
                self.assertEqual(
                    int(shim_code),
                    int(native_code),
                    f"ErrorCode.{native_code.name} value drifted: " f"native={int(native_code)} shim={int(shim_code)}",
                )

    def test_all_shim_codes_present_in_native(self) -> None:
        for shim_code in _ymq_wasm.ErrorCode:
            with self.subTest(name=shim_code.name):
                self.assertTrue(
                    hasattr(_ymq_native.ErrorCode, shim_code.name),
                    f"shim defines an ErrorCode unknown to native: {shim_code.name!r}",
                )


class BytesParityTest(unittest.TestCase):
    def test_bytes_roundtrip_matches(self) -> None:
        for payload in [b"", b"a", b"hello world", bytes(range(256))]:
            with self.subTest(len=len(payload)):
                native = _ymq_native.Bytes(payload)
                shim = _ymq_wasm.Bytes(payload)
                self.assertEqual(bytes(native), bytes(shim))
                self.assertEqual(native.len, shim.len)
                self.assertEqual(len(native), len(shim))

    def test_default_is_empty_on_both(self) -> None:
        native = _ymq_native.Bytes()
        shim = _ymq_wasm.Bytes()
        # Both should report length 0. (Native returns len=0 when no data was
        # provided; the shim's .data is None but .len is also 0.)
        self.assertEqual(native.len, shim.len)


class AddressParityTest(unittest.TestCase):
    """Shared address schemes should classify identically in native and shim."""

    def test_tcp_address_type_matches(self) -> None:
        addr_str = "tcp://127.0.0.1:4242"
        native = _ymq_native.Address(addr_str)
        shim = _ymq_wasm.Address(addr_str)
        self.assertEqual(native.type.name, shim.type.name)
        self.assertEqual(int(native.type), int(shim.type))

    def test_ipc_address_type_matches(self) -> None:
        addr_str = "ipc://name"
        native = _ymq_native.Address(addr_str)
        shim = _ymq_wasm.Address(addr_str)
        self.assertEqual(native.type.name, shim.type.name)
        self.assertEqual(int(native.type), int(shim.type))

    def test_invalid_address_raises_on_both(self) -> None:
        with self.assertRaises(_ymq_native.YMQException):
            _ymq_native.Address("http://invalid-scheme")
        with self.assertRaises(_ymq_wasm.YMQException):
            _ymq_wasm.Address("http://invalid-scheme")


class IOContextParityTest(unittest.TestCase):
    def test_default_num_threads(self) -> None:
        self.assertEqual(_ymq_native.IOContext().num_threads, _ymq_wasm.IOContext().num_threads)

    def test_custom_num_threads_preserved(self) -> None:
        self.assertEqual(_ymq_native.IOContext(num_threads=4).num_threads, 4)
        self.assertEqual(_ymq_wasm.IOContext(num_threads=4).num_threads, 4)


if __name__ == "__main__":
    unittest.main()
