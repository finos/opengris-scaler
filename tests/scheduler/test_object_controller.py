import asyncio
import unittest
from unittest.mock import MagicMock

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, ErrorCode
from scaler.scheduler.controllers.object_controller import VanillaObjectController
from scaler.utility.identifiers import ObjectID, WorkerID


class TestObjectControllerDeleteResilience(unittest.TestCase):
    """The object-delete broadcast runs on a timer loop and sends to every worker. A worker that
    departed during scale-down (socket closed, not yet timed out) must be skipped, not crash the whole
    scheduler, and must not stop the delete from reaching the live workers or the object store."""

    def test_departed_worker_skipped_in_delete_broadcast(self):
        controller = VanillaObjectController(config_controller=MagicMock())

        good = WorkerID(b"good-worker")
        departed = WorkerID(b"departed-worker")
        sent: list = []

        async def send(to, _message):
            if to == departed:
                raise ConnectorSocketClosedByRemoteEndError(ErrorCode.ConnectorSocketClosedByRemoteEnd, "gone")
            sent.append(to)

        deleted: list = []

        async def delete_object(object_id):
            deleted.append(object_id)

        controller._binder = MagicMock()
        controller._binder.send = send
        controller._worker_manager = MagicMock()
        controller._worker_manager.get_worker_ids.return_value = {good, departed}
        controller._connector_storage = MagicMock()
        controller._connector_storage.delete_object = delete_object

        object_id = ObjectID(b"object-to-delete".ljust(32, b"0"))  # Scaler object IDs are 32 bytes
        controller._queue_deleted_object_ids.put_nowait(object_id)

        # Must not raise even though one worker's socket is closed.
        routine = controller._VanillaObjectController__routine_send_objects_deletions  # type: ignore[attr-defined]
        asyncio.new_event_loop().run_until_complete(routine())

        self.assertEqual(sent, [good], "the live worker must still receive the delete")
        self.assertNotIn(departed, sent)
        self.assertEqual(deleted, [object_id], "the object store deletion must still run")


if __name__ == "__main__":
    unittest.main()
