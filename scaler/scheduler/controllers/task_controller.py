import logging
from typing import Dict, Optional, Set

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.io.async_connector_zmq import AsyncConnectorZMQ
from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import StateTask, Task, TaskCancel, TaskResult
from scaler.protocol.python.status import TaskManagerStatus
from scaler.scheduler.controllers.graph_controller import VanillaGraphTaskController
from scaler.scheduler.controllers.mixins import ClientController, ObjectController, TaskController, WorkerController
from scaler.utility.identifiers import ClientID, TaskID
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.queues.async_indexed_queue import AsyncIndexedQueue


class VanillaTaskController(TaskController, Looper, Reporter):
    def __init__(self, max_number_of_tasks_waiting: int):
        self._max_number_of_tasks_waiting = max_number_of_tasks_waiting
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnectorZMQ] = None

        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._worker_controller: Optional[WorkerController] = None
        self._graph_controller: Optional[VanillaGraphTaskController] = None

        self._task_id_to_task: Dict[TaskID, Task] = dict()

        self._unassigned: AsyncIndexedQueue = AsyncIndexedQueue()
        self._running: Set[TaskID] = set()

        self._success_count: int = 0
        self._failed_count: int = 0
        self._canceled_count: int = 0
        self._notfound_count: int = 0
        self._no_worker_count: int = 0
        self._died_count: int = 0

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnectorZMQ,
        client_controller: ClientController,
        object_controller: ObjectController,
        worker_controller: WorkerController,
        graph_controller: VanillaGraphTaskController,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._client_controller = client_controller
        self._object_controller = object_controller
        self._worker_controller = worker_controller
        self._graph_controller = graph_controller

    async def routine(self):
        task_id = await self._unassigned.get()

        # FIXME: As the assign_task_to_worker() call can be blocking (especially if there is no worker connected to the
        # scheduler), we might end up with the task object being in neither _running nor _unassigned.
        # In this case, the scheduler will answer any task cancellation request with a "task not found" error, which is
        # a bug.
        # https://github.com/Citi/scaler/issues/45

        if not await self._worker_controller.assign_task_to_worker(self._task_id_to_task[task_id]):
            await self._unassigned.put(task_id)
            return

        self._running.add(task_id)
        await self.__send_monitor(
            task_id,
            self._object_controller.get_object_name(self._task_id_to_task[task_id].func_object_id),
            TaskStatus.Running,
        )

    def get_status(self) -> TaskManagerStatus:
        return TaskManagerStatus.new_msg(
            unassigned=self._unassigned.qsize(),
            running=len(self._running),
            success=self._success_count,
            failed=self._failed_count,
            canceled=self._canceled_count,
            not_found=self._notfound_count,
        )

    async def on_task_new(self, client_id: ClientID, task: Task):
        if (
            0 <= self._max_number_of_tasks_waiting <= self._unassigned.qsize()
            and not self._worker_controller.has_available_worker()
        ):
            await self._binder.send(client_id, TaskResult.new_msg(task.task_id, TaskStatus.NoWorker))
            return

        self._client_controller.on_task_begin(client_id, task.task_id)
        self._task_id_to_task[task.task_id] = task

        await self._unassigned.put(task.task_id)
        await self.__send_monitor(
            task.task_id,
            self._object_controller.get_object_name(self._task_id_to_task[task.task_id].func_object_id),
            TaskStatus.Inactive,
        )

    async def on_task_cancel(self, client_id: ClientID, task_cancel: TaskCancel):
        if task_cancel.task_id not in self._task_id_to_task:
            logging.warning(f"cannot cancel, task not found: task_id={task_cancel.task_id.hex()}")
            await self.on_task_done(TaskResult.new_msg(task_cancel.task_id, TaskStatus.NotFound))
            return

        if task_cancel.task_id in self._unassigned:
            await self.on_task_done(TaskResult.new_msg(task_cancel.task_id, TaskStatus.Canceled))
            return

        await self._worker_controller.on_task_cancel(task_cancel)
        await self.__send_monitor(
            task_cancel.task_id,
            self._object_controller.get_object_name(self._task_id_to_task[task_cancel.task_id].func_object_id),
            TaskStatus.Canceling,
        )

    async def on_task_done(self, result: TaskResult):
        if result.status == TaskStatus.Success:
            self._success_count += 1
        elif result.status == TaskStatus.Failed:
            self._failed_count += 1
        elif result.status == TaskStatus.Canceled:
            self._canceled_count += 1
        elif result.status == TaskStatus.NotFound:
            self._notfound_count += 1
        elif result.status == TaskStatus.NoWorker:
            self._no_worker_count += 1
        elif result.status == TaskStatus.WorkerDied:
            self._died_count += 1

        if result.task_id in self._unassigned:
            self._unassigned.remove(result.task_id)
        elif result.task_id in self._running:
            self._running.remove(result.task_id)

        if result.task_id in self._task_id_to_task:
            func_object_name = self._object_controller.get_object_name(
                self._task_id_to_task.pop(result.task_id).func_object_id
            )
            client = self._client_controller.on_task_finish(result.task_id)
        else:
            func_object_name = b"<unknown func object>"
            client = None

        await self.__send_monitor(result.task_id, func_object_name, result.status, result.metadata)

        if self._graph_controller.is_graph_sub_task(result.task_id):
            await self._graph_controller.on_graph_sub_task_done(result)
            return

        if client is not None:
            await self._binder.send(client, result)

    async def on_task_reroute(self, task_id: TaskID):
        assert self._client_controller.get_client_id(task_id) is not None

        self._running.remove(task_id)
        await self._unassigned.put(task_id)
        await self.__send_monitor(
            task_id,
            self._object_controller.get_object_name(self._task_id_to_task[task_id].func_object_id),
            TaskStatus.Inactive,
        )

    async def __send_monitor(
        self, task_id: TaskID, function_name: bytes, status: TaskStatus, metadata: Optional[bytes] = b""
    ):
        worker = self._worker_controller.get_worker_by_task_id(task_id)
        await self._binder_monitor.send(StateTask.new_msg(task_id, function_name, status, worker, metadata))
