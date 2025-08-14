from typing import Optional

import psutil

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import StateScheduler
from scaler.protocol.python.status import Resource
from scaler.scheduler.controllers.mixins import (
    ClientController,
    InformationController,
    ObjectController,
    TaskController,
    WorkerController,
)
from scaler.utility.mixins import Looper


class VanillaInformationController(InformationController, Looper):
    def __init__(self, binder: AsyncConnector):
        self._monitor_binder: AsyncConnector = binder
        self._process = psutil.Process()

        self._binder: Optional[AsyncBinder] = None
        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None

    def register_managers(
        self,
        binder: AsyncBinder,
        client_controller: ClientController,
        object_controller: ObjectController,
        task_controller: TaskController,
        worker_controller: WorkerController,
    ):
        self._binder = binder
        self._client_controller = client_controller
        self._object_controller = object_controller
        self._task_controller = task_controller
        self._worker_controller = worker_controller

    async def routine(self):
        await self._monitor_binder.send(
            StateScheduler.new_msg(
                binder=self._binder.get_status(),
                scheduler=Resource.new_msg(int(self._process.cpu_percent() * 10), self._process.memory_info().rss),
                rss_free=psutil.virtual_memory().available,
                client_manager=self._client_controller.get_status(),
                object_manager=self._object_controller.get_status(),
                task_manager=self._task_controller.get_status(),
                worker_manager=self._worker_controller.get_status(),
            )
        )
