import logging
from typing import Dict, List, Optional

from scaler.io.mixins import AsyncBinder, AsyncConnector
from scaler.protocol.python.message import StateBalanceAdvice
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import TaskController
from scaler.scheduler.controllers.policies.mixins import ScalerPolicy
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.mixins import Looper


class VanillaBalanceController(Looper):
    def __init__(self, config_controller: VanillaConfigController, scaler_policy: ScalerPolicy):
        self._config_controller = config_controller

        self._scaler_policy = scaler_policy

        self._last_balance_advice: Dict[WorkerID, List[TaskID]] = dict()
        self._same_load_balance_advice_count = 0

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None

        self._task_controller: Optional[TaskController] = None

    def register(self, binder: AsyncBinder, binder_monitor: AsyncConnector, task_controller: TaskController):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._task_controller = task_controller

    async def routine(self):
        current_advice = self._scaler_policy.balance()
        if not self.__should_balance(current_advice):
            return

        worker_to_num_tasks = {worker: len(task_ids) for worker, task_ids in current_advice.items()}
        logging.info(f"balancing task: {worker_to_num_tasks}")
        for worker, task_ids in current_advice.items():
            await self._binder_monitor.send(StateBalanceAdvice.new_msg(worker, task_ids))

        self._last_balance_advice = current_advice
        for worker, task_ids in current_advice.items():
            for task_id in task_ids:
                await self._task_controller.on_task_balance_cancel(task_id)

    def __should_balance(self, current_advice: Dict[WorkerID, List[TaskID]]) -> bool:
        # 1. if this is the same advise as last time, then we +1 on same advice count
        # 2. if there is another different advice come in, then we reset same advice count to 0
        if self._last_balance_advice == current_advice:
            self._same_load_balance_advice_count += 1
        else:
            self._last_balance_advice = current_advice
            self._same_load_balance_advice_count = 0

        # if we have same advice for more than trigger times, then we start doing the balancing
        if 0 < self._same_load_balance_advice_count < self._config_controller.get_config("load_balance_trigger_times"):
            return False

        # if current advice is empty, then we skip
        if not current_advice:
            return False

        return True
