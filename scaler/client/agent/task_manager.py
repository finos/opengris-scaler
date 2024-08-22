from typing import Optional, Set

from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.agent.mixins import ObjectManager, TaskManager
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import GraphTask, GraphTaskCancel, Task, TaskCancel, TaskResult


class ClientTaskManager(TaskManager):
    def __init__(self):
        self._task_ids: Set[bytes] = set()
        self._graph_task_ids: Set[bytes] = set()

        self._connector_external: Optional[AsyncConnector] = None
        self._object_manager: Optional[ObjectManager] = None
        self._future_manager: Optional[ClientFutureManager] = None

    def register(
        self, connector_external: AsyncConnector, object_manager: ObjectManager, future_manager: ClientFutureManager
    ):
        self._connector_external = connector_external
        self._object_manager = object_manager
        self._future_manager = future_manager

    async def on_new_task(self, task: Task):
        self._task_ids.add(task.task_id)
        await self._connector_external.send(task)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        # We might receive a cancel task event on a previously finished task if:
        # -	The scheduler sends a TaskResult message to the client agent
        # - The client sends a TaskCancel message to the client agent, as it's not yet aware the task finished.
        # - The client agent processes the TaskResult message and removes the task from self._task_ids
        # - The client agent processes the TaskCancel message (that was already queued before processing the
        #   TaskResult), and fails on self._task_ids.remove() as the task_id no longer exists.

        if task_cancel.task_id not in self._task_ids:
            return

        self._task_ids.remove(task_cancel.task_id)
        await self._connector_external.send(task_cancel)

    async def on_new_graph_task(self, task: GraphTask):
        self._graph_task_ids.add(task.task_id)
        self._task_ids.update(set(task.targets))
        await self._connector_external.send(task)

    async def on_cancel_graph_task(self, task_cancel: GraphTaskCancel):
        if task_cancel.task_id not in self._graph_task_ids:
            return

        self._graph_task_ids.remove(task_cancel.task_id)
        await self._connector_external.send(task_cancel)

    async def on_task_result(self, result: TaskResult):
        if result.task_id not in self._task_ids:
            return

        self._task_ids.remove(result.task_id)

        if result.status != TaskStatus.Canceled:
            for result_object_id in result.results:
                self._object_manager.record_task_result(result.task_id, result_object_id)

        self._future_manager.on_task_result(result)
