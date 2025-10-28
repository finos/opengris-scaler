from typing import Optional

import psutil

from scaler.io.mixins import AsyncBinder, AsyncConnector
from scaler.protocol.python.message import InformationRequest, InformationSnapshot, StateScheduler
from scaler.protocol.python.status import Resource
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import (
    ClientController,
    InformationController,
    ObjectController,
    TaskController,
    WorkerController,
)
from scaler.scheduler.controllers.scaling_policies.mixins import ScalingController
from scaler.utility.mixins import Looper


class VanillaInformationController(InformationController, Looper):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller

        self._process = psutil.Process()

        self._monitor_binder: Optional[AsyncConnector] = None
        self._binder: Optional[AsyncBinder] = None
        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None
        self._scaling_controller: Optional[ScalingController] = None

    def register_managers(
        self,
        monitor_binder: AsyncConnector,
        binder: AsyncBinder,
        client_controller: ClientController,
        object_controller: ObjectController,
        task_controller: TaskController,
        worker_controller: WorkerController,
        scaling_controller: ScalingController,
    ):
        self._monitor_binder = monitor_binder
        self._binder = binder
        self._client_controller = client_controller
        self._object_controller = object_controller
        self._task_controller = task_controller
        self._worker_controller = worker_controller
        self._scaling_controller = scaling_controller

    async def on_request(self, request: InformationRequest):
        # TODO: implement commands
        pass

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
                scaling_manager=self._scaling_controller.get_status(),
            )
        )

        await self._scaling_controller.on_snapshot(
            InformationSnapshot(
                tasks=self._task_controller._task_id_to_task,  # type: ignore # noqa: Expose this later
                workers={
                    worker_id: worker_heartbeat
                    for worker_id, (
                        _,
                        worker_heartbeat,
                    ) in self._worker_controller._worker_alive_since.items()  # type: ignore # noqa: Expose this later
                },
            )
        )
