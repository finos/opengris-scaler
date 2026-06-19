"""
Browser-client import tests.

These tests exercise the import paths that the browser-based Scaler client
exercises on first load:

  1. ``import scaler``
  2. ``from scaler.io.ymq import ConnectorSocket``
  3. ``from scaler.client.client import Client, check_browser_runtime``

If any of these imports regresses (for example because someone reintroduces a
hard dependency on ``psutil``, ``pyzmq``, ``multiprocessing._multiprocessing``
or replaces one of the hardened ``static const char[]`` literals in the
``capnp`` C extension with a bare string literal), the wasm wheel will fail
to load in the browser and these tests will fail.

The tests are designed to run inside a Pyodide virtualenv (``pyodide venv``)
where the C extension is the wasm build. They also run unchanged on CPython
so developers can sanity-check the import surface locally without spinning
up Pyodide -- on CPython the same modules must remain importable.
"""

import importlib
import unittest


class BrowserClientImportTests(unittest.TestCase):
    """Import tests for the browser-side Scaler client surface."""

    def test_import_scaler(self) -> None:
        scaler = importlib.import_module("scaler")
        self.assertTrue(hasattr(scaler, "__version__"), "scaler module is missing __version__")

    def test_import_connector_socket(self) -> None:
        ymq = importlib.import_module("scaler.io.ymq")
        self.assertTrue(
            hasattr(ymq, "ConnectorSocket"), "scaler.io.ymq must expose ConnectorSocket for the browser client"
        )

    def test_import_client(self) -> None:
        client_module = importlib.import_module("scaler.client.client")
        self.assertTrue(hasattr(client_module, "Client"), "scaler.client.client.Client must be importable")
        self.assertTrue(
            hasattr(client_module, "check_browser_runtime"),
            "scaler.client.client.check_browser_runtime must be importable for the JSPI preflight",
        )

    def test_capnp_extension_loads(self) -> None:
        # The ``scaler.protocol.capnp`` C extension is the most fragile piece on
        # wasm: its module name and every attribute string are deliberately
        # materialised as ``static const char[]`` arrays in
        # ``src/cpp/scaler/protocol/pymod/`` to dodge a Pyodide SIDE_MODULE
        # relocator bug. Importing it here catches regressions to those fixes.
        capnp_mod = importlib.import_module("scaler.protocol.capnp")
        self.assertEqual(capnp_mod.__name__, "scaler.protocol.capnp")
        # ``BaseMessage`` is exposed by the C extension via PyModule_AddObjectRef
        # using a hardened attribute-name array. Verify it survives the relocator.
        self.assertTrue(hasattr(capnp_mod, "BaseMessage"))

    def test_capnp_struct_keyword_init(self) -> None:
        # Regression test for the SIDE_MODULE relocator corrupting the inline
        # "__init__" literal passed to PyObject_SetAttrString in bootstrap.cpp.
        # When that happened, the descriptor was registered under a garbled key
        # and ``Resource(cpu=1, rss=2)`` fell through to ``object.__init__``,
        # raising ``TypeError: Resource() takes no arguments``. Construct a few
        # representative structs (plain + nested + union-bearing) to exercise
        # both CapnpStruct.__init__ and CapnpUnionStruct.__init__.
        capnp_mod = importlib.import_module("scaler.protocol.capnp")

        resource = capnp_mod.Resource(cpu=42, rss=1024)
        self.assertEqual(resource.cpu, 42)
        self.assertEqual(resource.rss, 1024)

        heartbeat = capnp_mod.ClientHeartbeat(resource=resource, latencyUS=7)
        self.assertEqual(heartbeat.resource.cpu, 42)
        self.assertEqual(heartbeat.latencyUS, 7)


if __name__ == "__main__":
    unittest.main()
