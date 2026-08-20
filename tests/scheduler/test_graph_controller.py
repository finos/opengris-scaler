import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import (
    GraphTask,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
)
from scaler.scheduler.controllers.graph_controller import VanillaGraphTaskController
from scaler.utility.identifiers import ClientID, ObjectID, TaskID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _async_mock() -> MagicMock:
    """A collaborator whose every method is awaitable, so the controller can drive it end to end."""
    return MagicMock(**{name: AsyncMock() for name in ("send", "on_task_new", "on_task_cancel", "duplicate_object_id")})


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


class TestGraphControllerCleanUp(unittest.TestCase):
    """A graph that reaches an end state must leave nothing behind in the subtask id map.

    A subtask leaves the map when its result arrives, but a cancelled subtask has no result to arrive, so a
    cancelled graph used to leave one entry per node behind for as long as the scheduler ran.
    """

    def test_cancelled_graph_leaves_no_subtask_ids_behind(self):
        client_id = ClientID(b"client")
        graph_task_id = TaskID(b"graph")
        subtask_ids = [TaskID(f"subtask-{i}".encode()) for i in range(3)]

        controller = VanillaGraphTaskController(config_controller=MagicMock())
        controller.register(
            binder=_async_mock(),
            binder_monitor=_async_mock(),
            connector_storage=_async_mock(),
            client_controller=MagicMock(),
            task_controller=_async_mock(),
            object_controller=MagicMock(),
        )

        graph_task = GraphTask(
            taskId=graph_task_id,
            source=client_id,
            targets=[subtask_ids[-1]],
            graph=[
                Task(
                    taskId=subtask_id,
                    source=client_id,
                    metadata=b"",
                    funcObjectId=ObjectID.generate_object_id(client_id),
                    functionArgs=[],
                    capabilities={},
                )
                for subtask_id in subtask_ids
            ],
        )

        _run(controller.on_graph_task(client_id, graph_task))
        _run(controller.routine())
        for subtask_id in subtask_ids:
            self.assertTrue(controller.is_graph_subtask(subtask_id))

        _run(controller.on_graph_task_cancel(TaskCancel(taskId=graph_task_id, flags=TaskCancel.TaskCancelFlags())))

        # The workers answer the cancels the graph just sent; the last confirm finishes the graph.
        for subtask_id in subtask_ids:
            _run(
                controller.on_graph_sub_task_cancel_confirm(
                    TaskCancelConfirm(taskId=subtask_id, cancelConfirmType=TaskCancelConfirmType.canceled)
                )
            )

        self.assertEqual(controller._task_id_to_graph_task_id, {})
        self.assertEqual(controller._graph_task_id_to_graph, {})


class TestGraphControllerCancelWithUnreadyTarget(unittest.TestCase):
    """A cancel must also answer the nodes that only become ready because of the cancel.

    __cancel_whole_graph confirms the inactive nodes that are ready when it runs. A node waiting on a
    running one is not ready then, and only becomes ready when that running node's cancel confirm comes
    back -- which re-enters __cancel_whole_graph. When that re-entry was refused outright, such a node was
    never confirmed, the graph never reached its end state, and the client's future was never answered:
    Client.disconnect() then blocked forever waiting for the confirm.
    """

    def test_cancel_answers_a_node_waiting_on_a_running_one(self):
        client_id = ClientID(b"client")
        graph_task_id = TaskID(b"graph")
        running_task_id = TaskID(b"running-subtask")
        waiting_task_id = TaskID(b"waiting-subtask")

        binder = _async_mock()
        task_controller = _async_mock()
        controller = VanillaGraphTaskController(config_controller=MagicMock())
        controller.register(
            binder=binder,
            binder_monitor=_async_mock(),
            connector_storage=_async_mock(),
            client_controller=MagicMock(),
            task_controller=task_controller,
            object_controller=MagicMock(),
        )

        graph_task = GraphTask(
            taskId=graph_task_id,
            source=client_id,
            targets=[waiting_task_id],
            graph=[
                Task(
                    taskId=running_task_id,
                    source=client_id,
                    metadata=b"",
                    funcObjectId=ObjectID.generate_object_id(client_id),
                    functionArgs=[],
                    capabilities={},
                ),
                Task(
                    taskId=waiting_task_id,
                    source=client_id,
                    metadata=b"",
                    funcObjectId=ObjectID.generate_object_id(client_id),
                    functionArgs=[Task.Argument(type=Task.Argument.ArgumentType.task, data=running_task_id)],
                    capabilities={},
                ),
            ],
        )

        _run(controller.on_graph_task(client_id, graph_task))
        _run(controller.routine())

        # only the first node can run, the second one is waiting on it
        task_controller.on_task_new.assert_awaited_once()

        _run(controller.on_graph_task_cancel(TaskCancel(taskId=graph_task_id, flags=TaskCancel.TaskCancelFlags())))

        # the worker answers the cancel of the running node, which is what makes the waiting node ready
        _run(
            controller.on_graph_sub_task_cancel_confirm(
                TaskCancelConfirm(taskId=running_task_id, cancelConfirmType=TaskCancelConfirmType.canceled)
            )
        )

        confirmed_task_ids = [
            call.args[1].taskId for call in binder.send.await_args_list if isinstance(call.args[1], TaskCancelConfirm)
        ]
        self.assertIn(waiting_task_id, confirmed_task_ids, "the waiting node was never answered")
        self.assertIn(graph_task_id, confirmed_task_ids, "the graph itself was never answered")
        self.assertEqual(controller._graph_task_id_to_graph, {})
        self.assertEqual(controller._task_id_to_graph_task_id, {})


if __name__ == "__main__":
    unittest.main()
