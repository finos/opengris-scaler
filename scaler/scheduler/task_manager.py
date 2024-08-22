import logging
from typing import Dict, Optional, Set

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import StateTask, Task, TaskCancel, TaskResult
from scaler.protocol.python.status import TaskManagerStatus
from scaler.scheduler.graph_manager import VanillaGraphTaskManager
from scaler.scheduler.mixins import ClientManager, ObjectManager, TaskManager, WorkerManager
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.queues.async_indexed_queue import AsyncIndexedQueue


class VanillaTaskManager(TaskManager, Looper, Reporter):
    def __init__(self, max_number_of_tasks_waiting: int):
        self._max_number_of_tasks_waiting = max_number_of_tasks_waiting
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None

        self._client_manager: Optional[ClientManager] = None
        self._object_manager: Optional[ObjectManager] = None
        self._worker_manager: Optional[WorkerManager] = None
        self._graph_manager: Optional[VanillaGraphTaskManager] = None

        self._task_id_to_task: Dict[bytes, Task] = dict()

        self._unassigned: AsyncIndexedQueue = AsyncIndexedQueue()
        self._running: Set[bytes] = set()

        self._success_count: int = 0
        self._failed_count: int = 0
        self._canceled_count: int = 0
        self._notfound_count: int = 0
        self._no_worker_count: int = 0
        self._died_count: int = 0

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        client_manager: ClientManager,
        object_manager: ObjectManager,
        worker_manager: WorkerManager,
        graph_manager: VanillaGraphTaskManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._client_manager = client_manager
        self._object_manager = object_manager
        self._worker_manager = worker_manager
        self._graph_manager = graph_manager

    async def routine(self):
        task_id = await self._unassigned.get()

        if not await self._worker_manager.assign_task_to_worker(self._task_id_to_task[task_id]):
            await self._unassigned.put(task_id)
            return

        self._running.add(task_id)
        await self.__send_monitor(
            task_id,
            self._object_manager.get_object_name(self._task_id_to_task[task_id].func_object_id),
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

    async def on_task_new(self, client: bytes, task: Task):
        if (
            0 <= self._max_number_of_tasks_waiting <= self._unassigned.qsize()
            and not self._worker_manager.has_available_worker()
        ):
            await self._binder.send(client, TaskResult.new_msg(task.task_id, TaskStatus.NoWorker))
            return

        self._client_manager.on_task_begin(client, task.task_id)
        self._task_id_to_task[task.task_id] = task

        await self._unassigned.put(task.task_id)
        await self.__send_monitor(
            task.task_id,
            self._object_manager.get_object_name(self._task_id_to_task[task.task_id].func_object_id),
            TaskStatus.Inactive,
        )

    async def on_task_cancel(self, client: bytes, task_cancel: TaskCancel):
        if task_cancel.task_id not in self._task_id_to_task:
            logging.warning(f"cannot cancel, task not found: task_id={task_cancel.task_id.hex()}")
            await self.on_task_done(TaskResult.new_msg(task_cancel.task_id, TaskStatus.NotFound))
            return

        if task_cancel.task_id in self._unassigned:
            await self.on_task_done(TaskResult.new_msg(task_cancel.task_id, TaskStatus.Canceled))
            return

        await self._worker_manager.on_task_cancel(task_cancel)
        await self.__send_monitor(
            task_cancel.task_id,
            self._object_manager.get_object_name(self._task_id_to_task[task_cancel.task_id].func_object_id),
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
            func_object_name = self._object_manager.get_object_name(
                self._task_id_to_task.pop(result.task_id).func_object_id
            )
            client = self._client_manager.on_task_finish(result.task_id)
        else:
            func_object_name = b"<unknown func object>"
            client = None

        await self.__send_monitor(result.task_id, func_object_name, result.status, result.metadata)

        if self._graph_manager.is_graph_sub_task(result.task_id):
            await self._graph_manager.on_graph_sub_task_done(result)
            return

        if client is not None:
            await self._binder.send(client, result)

    async def on_task_reroute(self, task_id: bytes):
        assert self._client_manager.get_client_id(task_id) is not None

        self._running.remove(task_id)
        await self._unassigned.put(task_id)
        await self.__send_monitor(
            task_id,
            self._object_manager.get_object_name(self._task_id_to_task[task_id].func_object_id),
            TaskStatus.Inactive,
        )

    async def __send_monitor(
        self, task_id: bytes, function_name: bytes, status: TaskStatus, metadata: Optional[bytes] = b""
    ):
        worker = self._worker_manager.get_worker_by_task_id(task_id)
        await self._binder_monitor.send(StateTask.new_msg(task_id, function_name, status, worker, metadata))
