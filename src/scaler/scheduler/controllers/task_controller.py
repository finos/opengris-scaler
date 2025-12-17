import asyncio
import logging
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from scaler.io.mixins import AsyncBinder, AsyncConnector
from scaler.protocol.python.common import TaskCancelConfirmType, TaskResultType, TaskState, TaskTransition
from scaler.protocol.python.message import StateTask, Task, TaskCancel, TaskCancelConfirm, TaskResult
from scaler.protocol.python.status import TaskManagerStatus
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import (
    ClientController,
    GraphTaskController,
    ObjectController,
    TaskController,
    WorkerController,
)
from scaler.scheduler.task.task_state_machine import TaskStateMachine
from scaler.scheduler.task.task_state_manager import TaskStateManager
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.mixins import Looper, Reporter


class VanillaTaskController(TaskController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None

        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._worker_controller: Optional[WorkerController] = None

        self._graph_controller: Optional[GraphTaskController] = None

        self._task_id_to_task: Dict[TaskID, Task] = dict()
        self._task_state_manager: TaskStateManager = TaskStateManager(debug=True)

        self._unassigned: Deque[TaskID] = deque()  # type: ignore[misc]

        self._state_functions: Dict[TaskState, Callable[[*Tuple[Any, ...]], Awaitable[None]]] = {
            TaskState.Inactive: self.__state_inactive,  # type: ignore[dict-item]
            TaskState.Running: self.__state_running,  # type: ignore[dict-item]
            TaskState.Canceling: self.__state_canceling,  # type: ignore[dict-item]
            TaskState.BalanceCanceling: self.__state_balance_canceling,  # type: ignore[dict-item]
            TaskState.WorkerDisconnecting: self.__state_worker_disconnecting,  # type: ignore[dict-item]
            TaskState.Canceled: self.__state_canceled,  # type: ignore[dict-item]
            TaskState.CanceledNotFound: self.__state_canceled_not_found,  # type: ignore[dict-item]
            TaskState.Success: self.__state_success,  # type: ignore[dict-item]
            TaskState.Failed: self.__state_failed,  # type: ignore[dict-item]
            TaskState.FailedWorkerDied: self.__state_failed_worker_died,  # type: ignore[dict-item]
        }
        self._task_result_transition_map = {
            TaskResultType.Success: TaskTransition.TaskResultSuccess,
            TaskResultType.Failed: TaskTransition.TaskResultFailed,
            TaskResultType.FailedWorkerDied: TaskTransition.TaskResultWorkerDied,
        }
        self._task_cancel_confirm_transition_map = {
            TaskCancelConfirmType.Canceled: TaskTransition.TaskCancelConfirmCanceled,
            TaskCancelConfirmType.CancelNotFound: TaskTransition.TaskCancelConfirmNotFound,
            TaskCancelConfirmType.CancelFailed: TaskTransition.TaskCancelConfirmFailed,
        }

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        client_controller: ClientController,
        object_controller: ObjectController,
        worker_controller: WorkerController,
        graph_controller: GraphTaskController,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._client_controller = client_controller
        self._object_controller = object_controller
        self._worker_controller = worker_controller
        self._graph_controller = graph_controller

    async def routine(self):
        # TODO: we don't need loop task anymore, but I will leave this routine API here in case we need in the future
        pass

    async def on_task_new(self, task: Task):
        if self._task_state_manager.get_state_machine(task.task_id) is not None:
            logging.error(
                f"{task.task_id!r}: state machine already exists: "
                f"{self._task_state_manager.get_state_machine(task.task_id)}"
            )
            return

        state_machine = self._task_state_manager.add_state_machine(task.task_id)
        await self.__state_inactive(task_id=task.task_id, state_machine=state_machine, task=task)

    async def on_task_cancel(self, client_id: ClientID, task_cancel: TaskCancel):
        state_machine = self._task_state_manager.get_state_machine(task_cancel.task_id)
        if state_machine is None:
            logging.error(f"{task_cancel.task_id!r}: task not exists while received TaskCancel, send TaskCancelConfirm")

            task_cancel_confirm = TaskCancelConfirm.new_msg(task_cancel.task_id, TaskCancelConfirmType.CancelNotFound)

            if self._graph_controller.is_graph_subtask(task_cancel.task_id):
                await self._graph_controller.on_graph_sub_task_cancel_confirm(task_cancel_confirm)

            await self._binder.send(client_id, task_cancel_confirm)
            return

        if state_machine.current_state() == TaskState.Inactive:
            await self.__routing(
                task_cancel.task_id,
                TaskTransition.TaskCancel,
                task_cancel_confirm=TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.Canceled
                ),
            )
            return

        await self.__routing(task_cancel.task_id, TaskTransition.TaskCancel, client=client_id, task_cancel=task_cancel)

    async def on_task_balance_cancel(self, task_id: TaskID):
        await self.__routing(task_id, TaskTransition.BalanceTaskCancel)

    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        transition = self._task_cancel_confirm_transition_map.get(task_cancel_confirm.cancel_confirm_type, None)
        if transition is None:
            raise ValueError(f"unknown TaskCancelConfirmType: {task_cancel_confirm.cancel_confirm_type}")

        state_machine = self._task_state_manager.get_state_machine(task_cancel_confirm.task_id)
        if state_machine is None:
            logging.error(
                f"{task_cancel_confirm.task_id!r}: task not exists while received TaskCancelTaskCancelConfirm, ignore"
            )
            return

        current_state = state_machine.current_state()
        if (
            current_state == TaskState.BalanceCanceling
            and task_cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.Canceled
        ):
            # if balance cancel success
            task = self._task_id_to_task[task_cancel_confirm.task_id]
            await self.__routing(task_cancel_confirm.task_id, transition, task=task)
            return

        if (
            current_state == TaskState.BalanceCanceling
            and task_cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.CancelFailed
        ):
            # balance cancel failed
            worker_id = self._worker_controller.get_worker_by_task_id(task_cancel_confirm.task_id)
            await self.__routing(task_cancel_confirm.task_id, transition, worker_id=worker_id)
            return

        await self.__routing(task_cancel_confirm.task_id, transition, task_cancel_confirm=task_cancel_confirm)

    async def on_task_result(self, task_result: TaskResult):
        transition = self._task_result_transition_map.get(task_result.result_type, None)
        if transition is None:
            raise ValueError(f"unknown TaskResultType: {task_result.result_type}")

        await self.__routing(task_result.task_id, transition, task_result=task_result)

    async def on_worker_connect(self, worker_id: WorkerID):
        await self.__retry_unassignable()

    async def on_worker_disconnect(self, task_id: TaskID, worker_id: WorkerID):
        await self.__routing(task_id, TaskTransition.WorkerDisconnect, worker_id=worker_id)

    def get_status(self) -> TaskManagerStatus:
        return TaskManagerStatus.new_msg(state_to_count=self._task_state_manager.get_statistics())

    async def __state_inactive(self, task_id: TaskID, state_machine: TaskStateMachine, task: Task):
        assert task_id == task.task_id
        assert state_machine.current_state() == TaskState.Inactive

        self._client_controller.on_task_begin(task.source, task.task_id)
        self._task_id_to_task[task.task_id] = task

        worker_id = self._worker_controller.acquire_worker(self._task_id_to_task[task_id])
        if not worker_id.is_valid():
            # put task on hold until there is worker is added or task is finished/canceled (means have capacity)
            self._unassigned.append(task_id)
            return

        await self.__routing(task_id, TaskTransition.HasCapacity, worker_id=worker_id)
        await self.__send_monitor(task.task_id, self._object_controller.get_object_name(task.func_object_id))

    async def __state_running(self, task_id: TaskID, state_machine: TaskStateMachine, worker_id: WorkerID):
        if state_machine.previous_state() in {TaskState.Canceling, TaskState.BalanceCanceling}:
            # if cancel failed (task is ongoing), we should wait here for the result
            return

        assert state_machine.current_state() == TaskState.Running

        task = self._task_id_to_task[task_id]
        await self._binder.send(worker_id, task)
        await self.__send_monitor(task_id, self._object_controller.get_object_name(task.func_object_id))

    async def __state_canceling(
        self, task_id: TaskID, state_machine: TaskStateMachine, client: ClientID, task_cancel: TaskCancel
    ):
        assert task_id == task_cancel.task_id
        assert state_machine.current_state() == TaskState.Canceling

        if state_machine.previous_state() == TaskState.BalanceCanceling:
            # we don't need to send another TaskCancel as it's already sent in previous state
            return

        # in case if task trying to cancel doesn't have task in scheduler, so we know which client we can send
        # confirm to
        self._client_controller.on_task_begin(client, task_id)

        if task_id not in self._unassigned:
            await self.__send_task_cancel_to_worker(task_cancel)
            return

        # task is either in unassigned or unassignable
        await self.__routing(
            task_id,
            TaskTransition.TaskCancelConfirmCanceled,
            task_cancel_confirm=TaskCancelConfirm.new_msg(task_id, TaskCancelConfirmType.Canceled),
        )

        if task_id in self._unassigned:
            self._unassigned.remove(task_id)

    async def __state_balance_canceling(self, task_id: TaskID, state_machine: TaskStateMachine):
        assert state_machine.current_state() == TaskState.BalanceCanceling
        await self.__send_task_cancel_to_worker(
            TaskCancel.new_msg(task_id=task_id, flags=TaskCancel.TaskCancelFlags(force=False))
        )

    async def __state_worker_disconnecting(self, task_id: TaskID, state_machine: TaskStateMachine, worker_id: WorkerID):
        assert isinstance(worker_id, WorkerID)
        assert state_machine.current_state() == TaskState.WorkerDisconnecting

        # this is where we decide to reroute or just send fail
        task = self._task_id_to_task.get(task_id)
        if task is None:
            await self.__routing(
                task_id,
                TaskTransition.SchedulerHasNoTask,
                task_result=TaskResult.new_msg(task_id, TaskResultType.FailedWorkerDied),
            )
        else:
            await self.__routing(task_id, TaskTransition.SchedulerHasTask, task=task)

    async def __state_canceled(
        self, task_id: TaskID, state_machine: TaskStateMachine, task_cancel_confirm: TaskCancelConfirm
    ):
        assert task_id == task_cancel_confirm.task_id
        assert task_cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.Canceled
        assert state_machine.current_state() == TaskState.Canceled

        if task_cancel_confirm.task_id in self._unassigned:
            # if task is not assigned to any worker, we don't need to deal with worker manager
            self._unassigned.remove(task_cancel_confirm.task_id)
        else:
            await self._worker_controller.on_task_done(task_cancel_confirm.task_id)

        await self.__send_task_cancel_confirm_to_client(task_cancel_confirm)

    async def __state_canceled_not_found(
        self, task_id: TaskID, state_machine: TaskStateMachine, task_cancel_confirm: TaskCancelConfirm
    ):
        assert task_id == task_cancel_confirm.task_id
        assert task_cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.CancelNotFound
        assert state_machine.current_state() == TaskState.CanceledNotFound

        await self.__send_task_cancel_confirm_to_client(task_cancel_confirm)

    async def __state_success(self, task_id: TaskID, state_machine: TaskStateMachine, task_result: TaskResult):
        assert task_id == task_result.task_id
        assert state_machine.current_state() == TaskState.Success
        await self.__send_task_result_to_client(task_result)

    async def __state_failed(self, task_id: TaskID, state_machine: TaskStateMachine, task_result: TaskResult):
        assert task_id == task_result.task_id
        assert state_machine.current_state() == TaskState.Failed
        await self.__send_task_result_to_client(task_result)

    async def __state_failed_worker_died(
        self, task_id: TaskID, state_machine: TaskStateMachine, task_result: TaskResult
    ):
        assert task_id == task_result.task_id
        assert state_machine.current_state() == TaskState.FailedWorkerDied
        await self.__send_task_result_to_client(task_result)

    async def __send_task_cancel_to_worker(self, task_cancel: TaskCancel):
        worker = await self._worker_controller.on_task_cancel(task_cancel)
        assert(isinstance(worker, WorkerID))
        if not worker.is_valid():
            logging.error(f"{task_cancel.task_id!r}: cannot find task in worker to cancel")
            await self.__routing(
                task_cancel.task_id,
                TaskTransition.TaskCancelConfirmNotFound,
                task_cancel_confirm=TaskCancelConfirm.new_msg(
                    task_cancel.task_id, TaskCancelConfirmType.CancelNotFound
                ),
            )
            return

        await self._binder.send(worker, task_cancel)
        await self.__send_monitor(task_cancel.task_id, b"")

    async def __send_task_result_to_client(self, task_result: TaskResult):
        await self._worker_controller.on_task_done(task_result.task_id)
        client = self._client_controller.on_task_finish(task_result.task_id)
        await self._binder.send(client, task_result)
        await self.__send_monitor(task_result.task_id, b"", task_result.metadata)

        self._task_state_manager.remove_state_machine(task_result.task_id)
        self._task_id_to_task.pop(task_result.task_id)

        if self._graph_controller.is_graph_subtask(task_result.task_id):
            await self._graph_controller.on_graph_sub_task_result(task_result)

        await self.__retry_unassignable()

    async def __send_task_cancel_confirm_to_client(self, task_cancel_confirm: TaskCancelConfirm):
        client = self._client_controller.on_task_finish(task_cancel_confirm.task_id)
        await self._binder.send(client, task_cancel_confirm)
        await self.__send_monitor(task_cancel_confirm.task_id, b"")
        self._task_state_manager.remove_state_machine(task_cancel_confirm.task_id)
        self._task_id_to_task.pop(task_cancel_confirm.task_id)

        if self._graph_controller.is_graph_subtask(task_cancel_confirm.task_id):
            await self._graph_controller.on_graph_sub_task_cancel_confirm(task_cancel_confirm)

        await self.__retry_unassignable()

    async def __send_monitor(self, task_id: TaskID, function_name: bytes, metadata: bytes = b""):
        worker = self._worker_controller.get_worker_by_task_id(task_id)
        task_state = self._task_state_manager.get_state_machine(task_id).current_state()
        capabilities = self._task_id_to_task[task_id].capabilities if task_id in self._task_id_to_task else {}
        await self._binder_monitor.send(
            StateTask.new_msg(task_id, function_name, task_state, worker, capabilities, metadata)
        )

    async def __routing(self, task_id: TaskID, transition: TaskTransition, **kwargs):
        state_machine = self._task_state_manager.on_transition(task_id, transition)
        if state_machine is None:
            logging.info(f"{task_id!r}: unknown transition: {transition}")
            return

        try:
            await self._state_functions[state_machine.current_state()](task_id, state_machine, **kwargs)  # noqa
        except Exception as e:
            logging.exception(
                f"{task_id!r}: exception happened, transition: {transition} path: {state_machine.get_path()}"
            )
            raise e

    async def __retry_unassignable(self):
        futures = [
            self.__routing(task_id, TaskTransition.HasCapacity, worker_id=worker_id)
            for task_id, worker_id in self.__acquire_workers()
        ]

        await asyncio.gather(*futures)

    def __acquire_workers(self) -> List[Tuple[TaskID, WorkerID]]:
        """please note this function has to be atomic, means no async decorated in order to make unassigned queue to be
        synced, also this function should return as list not generator because of atomic
        """

        ready_to_assign = list()
        while len(self._unassigned) > 0:
            worker_id = self._worker_controller.acquire_worker(self._task_id_to_task[self._unassigned[0]])
            if not worker_id.is_valid():
                break

            task_id = self._unassigned.popleft()
            ready_to_assign.append((task_id, worker_id))

        return ready_to_assign
