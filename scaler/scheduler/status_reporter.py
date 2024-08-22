from typing import Optional

import psutil

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import StateScheduler
from scaler.protocol.python.status import Resource
from scaler.scheduler.mixins import ClientManager, ObjectManager, TaskManager, WorkerManager
from scaler.utility.mixins import Looper


class StatusReporter(Looper):
    def __init__(self, binder: AsyncConnector):
        self._monitor_binder: AsyncConnector = binder
        self._process = psutil.Process()

        self._binder: Optional[AsyncBinder] = None
        self._client_manager: Optional[ClientManager] = None
        self._object_manager: Optional[ObjectManager] = None
        self._task_manager: Optional[TaskManager] = None
        self._worker_manager: Optional[WorkerManager] = None

    def register_managers(
        self,
        binder: AsyncBinder,
        client_manager: ClientManager,
        object_manager: ObjectManager,
        task_manager: TaskManager,
        worker_manager: WorkerManager,
    ):
        self._binder = binder
        self._client_manager = client_manager
        self._object_manager = object_manager
        self._task_manager = task_manager
        self._worker_manager = worker_manager

    async def routine(self):
        await self._monitor_binder.send(
            StateScheduler.new_msg(
                binder=self._binder.get_status(),
                scheduler=Resource.new_msg(int(self._process.cpu_percent() * 10), self._process.memory_info().rss),
                rss_free=psutil.virtual_memory().available,
                client_manager=self._client_manager.get_status(),
                object_manager=self._object_manager.get_status(),
                task_manager=self._task_manager.get_status(),
                worker_manager=self._worker_manager.get_status(),
            )
        )
