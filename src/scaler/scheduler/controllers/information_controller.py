from typing import Optional

import psutil

from scaler.io.mixins import AsyncBinder, AsyncPublisher
from scaler.protocol.capnp import InformationRequest, Resource, StateScheduler
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import (
    ClientController,
    InformationController,
    ObjectController,
    TaskController,
    WorkerController,
)
from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController
from scaler.utility.mixins import Looper


class VanillaInformationController(InformationController, Looper):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller

        self._process = psutil.Process()

        self._monitor_binder: Optional[AsyncPublisher] = None
        self._binder: Optional[AsyncBinder] = None
        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None
        self._worker_manager_controller: Optional[WorkerManagerController] = None

    def register_managers(
        self,
        monitor_binder: AsyncPublisher,
        binder: AsyncBinder,
        client_controller: ClientController,
        object_controller: ObjectController,
        task_controller: TaskController,
        worker_controller: WorkerController,
        worker_manager_controller: WorkerManagerController,
    ):
        self._monitor_binder = monitor_binder
        self._binder = binder
        self._client_controller = client_controller
        self._object_controller = object_controller
        self._task_controller = task_controller
        self._worker_controller = worker_controller
        self._worker_manager_controller = worker_manager_controller

    async def on_request(self, request: InformationRequest):
        # TODO: implement commands
        pass

    async def routine(self):
        await self._monitor_binder.send(
            StateScheduler(
                binder=self._binder.get_status(),
                scheduler=Resource(cpu=int(self._process.cpu_percent() * 10), rss=self._process.memory_info().rss),
                rssFree=psutil.virtual_memory().available,
                clientManager=self._client_controller.get_status(),
                objectManager=self._object_controller.get_status(),
                taskManager=self._task_controller.get_status(),
                workerManager=self._worker_controller.get_status(),
                scalingManager=self._worker_manager_controller.get_status(),
            )
        )
