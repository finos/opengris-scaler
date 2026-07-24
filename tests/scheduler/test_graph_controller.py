import asyncio
import unittest
from unittest.mock import MagicMock

from scaler.protocol.capnp import TaskCancelConfirm, TaskCancelConfirmType, TaskResult, TaskResultType
from scaler.scheduler.controllers.graph_controller import VanillaGraphTaskController
from scaler.utility.identifiers import TaskID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class TestGraphControllerOrphanedSubtask(unittest.TestCase):
    """A late result/cancel-confirm for a subtask whose graph was already cleaned up must not crash.

    A cancelled subtask id can linger in _task_id_to_graph_task_id after its graph is popped, so
    is_graph_subtask stays true. A subsequent result/cancel-confirm that indexed _graph_task_id_to_graph
    directly would KeyError and, escaping to asyncio.gather, tear the whole scheduler down. This is
    reachable when a client running a graph is killed and a late subtask result/confirm arrives.
    """

    @staticmethod
    def _controller_with_orphan(subtask_id: TaskID) -> VanillaGraphTaskController:
        controller = VanillaGraphTaskController(config_controller=MagicMock())
        # Orphan: the subtask still maps to a graph id, but that graph is gone (already cleaned up).
        controller._task_id_to_graph_task_id[subtask_id] = TaskID(b"already-cleaned-up-graph")
        return controller

    def test_cancel_confirm_for_orphaned_subtask_does_not_crash(self):
        subtask_id = TaskID(b"orphan-subtask")
        controller = self._controller_with_orphan(subtask_id)
        self.assertTrue(controller.is_graph_subtask(subtask_id))
        _run(
            controller.on_graph_sub_task_cancel_confirm(
                TaskCancelConfirm(taskId=subtask_id, cancelConfirmType=TaskCancelConfirmType.canceled)
            )
        )

    def test_result_for_orphaned_subtask_does_not_crash(self):
        subtask_id = TaskID(b"orphan-subtask")
        controller = self._controller_with_orphan(subtask_id)
        _run(
            controller.on_graph_sub_task_result(
                TaskResult(taskId=subtask_id, resultType=TaskResultType.failed, metadata=b"", results=[])
            )
        )


if __name__ == "__main__":
    unittest.main()
