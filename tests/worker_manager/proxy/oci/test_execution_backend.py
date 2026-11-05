import unittest
from unittest.mock import MagicMock

from scaler.protocol.capnp import TaskCancel
from scaler.utility.identifiers import TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.proxy.oci.execution_backend import OCIExecutionBackend
from tests.utility.utility import logging_test_name

INSTANCE_ID = "ocid1.computecontainerinstance.oc1..example"


def _make_backend() -> OCIExecutionBackend:
    backend = OCIExecutionBackend(
        compartment_id="ocid1.compartment.oc1..example",
        availability_domain="AD-1",
        subnet_id="ocid1.subnet.oc1..example",
        container_image="example/image:latest",
        oci_region="us-ashburn-1",
        object_storage_namespace="namespace",
        object_storage_bucket="bucket",
    )
    backend._container_instances_client = MagicMock()
    backend._object_storage_client = MagicMock()
    return backend


def _force_cancel(task_id: TaskID) -> TaskCancel:
    return TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=True))


class TestOCIExecutionBackendCancel(unittest.IsolatedAsyncioTestCase):
    """Pins that a cancel whose instance delete fails raises and keeps the instance, so a later cancel can retry it."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.task_id = TaskID.generate_task_id()
        self.backend._task_id_to_instance_id[self.task_id] = INSTANCE_ID

    async def test_a_cancel_deletes_the_instance_and_forgets_it(self) -> None:
        await self.backend.on_cancel(_force_cancel(self.task_id))

        self.backend._container_instances_client.delete_container_instance.assert_called_once_with(
            container_instance_id=INSTANCE_ID
        )
        self.assertNotIn(self.task_id, self.backend._task_id_to_instance_id)

    async def test_a_cancel_whose_delete_fails_raises_and_keeps_the_instance(self) -> None:
        self.backend._container_instances_client.delete_container_instance.side_effect = RuntimeError("delete failed")

        with self.assertRaises(RuntimeError):
            await self.backend.on_cancel(_force_cancel(self.task_id))

        self.assertEqual(self.backend._task_id_to_instance_id[self.task_id], INSTANCE_ID)

    async def test_a_cancel_retried_after_a_failed_delete_deletes_the_instance(self) -> None:
        delete = self.backend._container_instances_client.delete_container_instance
        delete.side_effect = [RuntimeError("delete failed"), None]

        with self.assertRaises(RuntimeError):
            await self.backend.on_cancel(_force_cancel(self.task_id))
        await self.backend.on_cancel(_force_cancel(self.task_id))

        self.assertEqual(delete.call_count, 2)
        self.assertNotIn(self.task_id, self.backend._task_id_to_instance_id)
