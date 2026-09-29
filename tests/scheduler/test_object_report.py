"""What the scheduler reports about the objects it tracks."""

import unittest

from scaler.config import defaults
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import ObjectMetadata
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.object_controller import VanillaObjectController
from scaler.utility.identifiers import ClientID, ObjectID


def make_config_controller() -> VanillaConfigController:
    return VanillaConfigController(
        SchedulerConfig(
            bind_address=AddressConfig.from_string("tcp://127.0.0.1:6378"),
            object_storage_address=AddressConfig.from_string("tcp://127.0.0.1:6379"),
        )
    )


def make_object_controller(sizes) -> VanillaObjectController:
    controller = VanillaObjectController(config_controller=make_config_controller())
    client_id = ClientID.generate_client_id()
    for name, object_id, size in sizes:
        controller.on_add_object(client_id, object_id, ObjectMetadata.ObjectContentType.object, name, size)
    return controller


class TestLargestObjects(unittest.TestCase):
    def test_the_biggest_objects_come_first_and_the_list_is_bounded(self) -> None:
        client_id = ClientID.generate_client_id()
        ids = [ObjectID.generate_object_id(client_id) for _ in range(4)]
        controller = make_object_controller(
            [(b"small", ids[0], 10), (b"huge", ids[1], 4000), (b"medium", ids[2], 500), (b"tiny", ids[3], 1)]
        )

        largest = controller.get_largest_objects(2)

        self.assertEqual([detail.name for detail in largest], [b"huge", b"medium"])
        self.assertEqual([detail.size for detail in largest], [4000, 500])
        self.assertEqual(controller.object_count(), 4)
        self.assertEqual(controller.get_object_size(ids[1]), 4000)

    def test_an_object_the_client_released_is_no_longer_reported(self) -> None:
        client_id = ClientID.generate_client_id()
        ids = [ObjectID.generate_object_id(client_id) for _ in range(2)]
        controller = VanillaObjectController(config_controller=make_config_controller())
        controller.on_add_object(client_id, ids[0], ObjectMetadata.ObjectContentType.object, b"kept", 10)
        controller.on_add_object(client_id, ids[1], ObjectMetadata.ObjectContentType.object, b"released", 4_000)

        controller.on_del_objects(client_id, {ids[1]})

        self.assertEqual([detail.name for detail in controller.get_largest_objects(10)], [b"kept"])
        self.assertEqual(controller.get_object_size(ids[1]), 0)

    def test_an_object_whose_client_reports_no_size_still_appears(self) -> None:
        client_id = ClientID.generate_client_id()
        object_id = ObjectID.generate_object_id(client_id)
        controller = VanillaObjectController(config_controller=make_config_controller())
        controller.on_add_object(client_id, object_id, ObjectMetadata.ObjectContentType.object, b"unsized", 0)

        largest = controller.get_largest_objects(10)

        self.assertEqual([detail.name for detail in largest], [b"unsized"])
        self.assertEqual(largest[0].size, 0)


class TestObjectReportLimit(unittest.TestCase):
    def test_the_report_carries_what_the_scheduler_is_configured_to_send(self) -> None:
        """The limit is what a monitor can page through, and what each report costs."""
        controller = make_config_controller()
        self.assertEqual(controller.get_config("object_report_limit"), defaults.OBJECT_REPORT_LIMIT)

        controller.update_config("object_report_limit", 25)
        self.assertEqual(controller.get_config("object_report_limit"), 25)


if __name__ == "__main__":
    unittest.main()
