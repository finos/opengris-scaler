"""What the monitor shows about the object storage server, and about the objects it holds.

The store reports what it holds through its `infoGetTotal` request, and the scheduler reports which tasks
name each object, because the store never sees a task.
"""

import struct
import unittest

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import ObjectStorageTotals
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    ObjectMetadata,
    Resource,
    ScalingManagerStatus,
    StateObject,
    StateScheduler,
    TaskManagerStatus,
    WorkerManagerStatus,
)
from scaler.ui.app import WebGUIConfig, WebUIApp


def make_app() -> WebUIApp:
    return WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))


def make_status(object_manager: ObjectManagerStatus) -> StateScheduler:
    """A status frame as the GUI receives it: field reads need a deserialized struct."""
    return StateScheduler.from_bytes(
        StateScheduler(
            binder=BinderStatus(received=[], sent=[]),
            scheduler=Resource(cpu=0, rss=0),
            rssFree=0,
            clientManager=ClientManagerStatus(clients=[]),
            objectManager=object_manager,
            taskManager=TaskManagerStatus(stateToCount=[]),
            workerManager=WorkerManagerStatus(workers=[]),
            scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
        ).to_bytes()
    )


class TestObjectStorageTotals(unittest.TestCase):
    def test_a_full_payload_reads_every_field(self) -> None:
        totals = ObjectStorageTotals.from_payload(struct.pack("<6Q", 10, 7, 4096, 2, 1, 42))
        self.assertEqual(totals.object_count, 10)
        self.assertEqual(totals.unique_object_count, 7)
        self.assertEqual(totals.total_bytes, 4096)
        self.assertEqual(totals.pending_request_count, 2)
        self.assertEqual(totals.pending_object_count, 1)
        self.assertEqual(totals.oldest_pending_seconds, 42)

    def test_a_server_answering_with_fewer_fields_stays_readable(self) -> None:
        totals = ObjectStorageTotals.from_payload(struct.pack("<3Q", 10, 7, 4096))
        self.assertEqual(totals.total_bytes, 4096)
        self.assertEqual(totals.pending_request_count, 0)
        self.assertEqual(totals.oldest_pending_seconds, 0)

    def test_a_server_answering_with_more_fields_stays_readable(self) -> None:
        totals = ObjectStorageTotals.from_payload(struct.pack("<8Q", 10, 7, 4096, 2, 1, 42, 99, 99))
        self.assertEqual(totals.oldest_pending_seconds, 42)


class TestStorageCard(unittest.TestCase):
    def test_the_card_reports_what_the_store_holds(self) -> None:
        app = make_app()
        app._process_scheduler(
            make_status(
                ObjectManagerStatus(
                    numberOfObjects=4,
                    storageObjectCount=10,
                    storageUniqueCount=7,
                    storageTotalBytes=4096,
                    storagePendingRequests=2,
                    storagePendingObjects=1,
                    storageOldestPendingS=42,
                )
            )
        )

        storage = app._storage_data
        self.assertEqual(storage["objects"], 10)
        self.assertEqual(storage["unique_objects"], 7)
        self.assertEqual(storage["shared"], 3)
        self.assertEqual(storage["pending"], 2)
        self.assertEqual(storage["oldest_pending"], "42s")

    def test_nothing_waiting_reports_no_age(self) -> None:
        app = make_app()
        app._process_scheduler(make_status(ObjectManagerStatus(numberOfObjects=0)))

        self.assertEqual(app._storage_data["pending"], 0)
        self.assertEqual(app._storage_data["oldest_pending"], "0s")


class TestObjectsView(unittest.TestCase):
    def test_an_object_row_names_its_client_and_its_tasks(self) -> None:
        app = make_app()
        state = StateObject.from_bytes(
            StateObject(
                objects=[
                    StateObject.ObjectDetail(
                        objectId=b"a" * 32,
                        name=b"heavy_frame",
                        objectType=ObjectMetadata.ObjectContentType.object,
                        size=2_000_000,
                        creator=b"Client|one",
                        taskIds=[b"b" * 32, b"c" * 32],
                        taskCount=57,
                    )
                ],
                totalObjects=9,
            ).to_bytes()
        )
        app._process_objects(state)

        section = app._objects_section()
        self.assertEqual(section["objects_total"], 9)
        row = section["objects"][0]
        self.assertEqual(row["name"], "heavy_frame")
        self.assertEqual(row["size"], "1.9M")
        self.assertEqual(row["full_client"], "Client|one")
        self.assertEqual(row["tasks"], 57, "the count is the whole set, not the sample that travels with it")
        self.assertEqual(row["task_ids"], ["626262626262", "636363636363"])


if __name__ == "__main__":
    unittest.main()
