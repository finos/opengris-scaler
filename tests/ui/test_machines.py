"""The Machines page: one row per host, however many workers it runs."""

import unittest
from typing import Any, Dict

from scaler.config.types.address import AddressConfig
from scaler.ui.app import WebGUIConfig, WebUIApp
from tests.ui.test_table_columns import columns, headers


def make_app(workers: Dict[str, Dict[str, Any]]) -> WebUIApp:
    app = WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))
    app._workers_data = workers
    return app


class TestMachines(unittest.TestCase):
    def test_a_host_was_last_seen_when_its_freshest_worker_was(self) -> None:
        app = make_app(
            {
                "worker-a": {"host": "box-1", "last_seen_seconds": 40},
                "worker-b": {"host": "box-1", "last_seen_seconds": 3},
                "worker-c": {"host": "box-2", "last_seen_seconds": 75},
            }
        )

        rows = {row["host"]: row for row in app._machines_section()["machines"]}
        self.assertEqual(rows["box-1"]["last_seen"], "3s")
        self.assertEqual(rows["box-2"]["last_seen"], "1m15s")

    def test_every_column_has_a_header_and_a_field(self) -> None:
        row = make_app({"worker-a": {"host": "box-1", "last_seen_seconds": 1}})._machines_section()["machines"][0]

        self.assertEqual(len(headers("machines-table")), len(columns("MACHINE_FIELDS")))
        self.assertEqual([field for field in columns("MACHINE_FIELDS") if field not in row], [])


if __name__ == "__main__":
    unittest.main()
