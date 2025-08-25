import logging
from typing import Awaitable, Callable, Optional, Dict, Any, Set

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import TaskResultType, TaskCancelConfirmType, TaskState, TaskTransition
from scaler.protocol.python.message import StateTask, Task, TaskCancel, TaskResult, TaskCancelConfirm
from scaler.protocol.python.status import TaskManagerStatus
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import (
    ClientController,
    ObjectController,
    TaskController,
    WorkerController,
    GraphTaskController,
)
from scaler.scheduler.task.task_state_machine import TaskTypeFlags, TaskStateMachine
from scaler.scheduler.task.task_state_manager import TaskStateManager
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.queues.async_indexed_queue import AsyncIndexedQueue


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

        self._unassigned: AsyncIndexedQueue[TaskID] = AsyncIndexedQueue()
        self._unassignable: Set[TaskID] = set()

        self._state_functions: Dict[TaskState, Callable[[TaskID, TaskStateMachine, Any, ...], Awaitable[None]]] = {
            TaskState.Inactive: self.__state_inactive,
            TaskState.Running: self.__state_running,
            TaskState.Canceling: self.__state_canceling,
            TaskState.BalanceCanceling: self.__state_balance_canceling,
            TaskState.WorkerDisconnecting: self.__state_worker_disconnecting,
            TaskState.Canceled: self.__state_canceled,
            TaskState.CanceledNotFound: self.__state_canceled_not_found,
            TaskState.Success: self.__state_success,
            TaskState.Failed: self.__state_failed,
            TaskState.FailedWorkerDied: self.__state_failed_worker_died,
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
        task_id = await self._unassigned.get()
        worker_id = self._worker_controller.acquire_worker(self._task_id_to_task[task_id])
        if not worker_id:
            self._unassignable.add(task_id)
            return

        await self.__routing(task_id, TaskTransition.HasCapacity, worker_id=worker_id)

    async def on_task_new(self, task: Task):
        if (
            0 <= self._config_controller.get_config("max_number_of_tasks_waiting") <= self._unassigned.qsize()
            and not self._worker_controller.has_available_worker()
        ):
            await self._binder.send(task.source, TaskResult.new_msg(task.task_id, TaskResultType.Failed))
            return

        if self._task_state_manager.has_state_machine(task.task_id):
            logging.error(
                f"{task.task_id!r}: state machine already exists: "
                f"{self._task_state_manager.get_state_machine(task.task_id)}"
            )
            return

        state_machine = self._task_state_manager.add_state_machine(task.task_id)
        await self.__state_inactive(task_id=task.task_id, state_machine=state_machine, task=task)

    async def on_task_cancel(self, client: ClientID, task_cancel: TaskCancel):
        if not self._task_state_manager.has_state_machine(task_cancel.task_id):
            logging.error(f"{task_cancel.task_id!r}: task not exists while received TaskCancel, send TaskCancelConfirm")
            await self._binder.send(
                client, TaskCancelConfirm.new_msg(task_cancel.task_id, TaskCancelConfirmType.CancelNotFound)
            )
            return

        if self._task_state_manager.get_state_machine(task_cancel.task_id).current_state() == TaskState.Inactive:
            await self.__routing(
                task_cancel.task_id,
                TaskTransition.TaskCancel,
                task_cancel_confirm=TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.Canceled
                ),
            )
            return

        await self.__routing(task_cancel.task_id, TaskTransition.TaskCancel, client=client, task_cancel=task_cancel)

    async def on_task_balance_cancel(self, task_id: TaskID):
        await self.__routing(task_id, TaskTransition.BalanceTaskCancel)

    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        transition = self._task_cancel_confirm_transition_map.get(task_cancel_confirm.cancel_confirm_type, None)
        if transition is None:
            raise ValueError(f"unknown TaskCancelConfirmTy pe: {task_cancel_confirm.cancel_confirm_type}")

        if not self._task_state_manager.has_state_machine(task_cancel_confirm.task_id):
            logging.error(
                f"{task_cancel_confirm.task_id!r}: task not exists while received TaskCancelTaskCancelConfirm, ignore"
            )
            return

        current_state = self._task_state_manager.get_state_machine(task_cancel_confirm.task_id).current_state()
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

    def add_task_flag(self, task_id: TaskID, flag: TaskTypeFlags):
        self._task_state_manager.get_state_machine(task_id).add_flag(flag)

    async def __state_inactive(self, task_id: TaskID, state_machine: TaskStateMachine, task: Task):
        assert task_id == task.task_id
        assert state_machine.current_state() == TaskState.Inactive

        self._client_controller.on_task_begin(task.source, task.task_id)
        self._task_id_to_task[task.task_id] = task
        await self._unassigned.put(task.task_id)

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

        if task_id not in self._unassigned and task_id not in self._unassignable:
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

        if task_id in self._unassignable:
            self._unassignable.remove(task_id)

    async def __state_balance_canceling(self, task_id: TaskID, state_machine: TaskStateMachine):
        assert state_machine.current_state() == TaskState.BalanceCanceling
        await self.__send_task_cancel_to_worker(
            TaskCancel.new_msg(task_id=task_id, flags=TaskCancel.TaskCancelFlags(force=False))
        )

    async def __state_worker_disconnecting(self, task_id: TaskID, state_machine: TaskStateMachine, worker_id: bytes):
        assert isinstance(worker_id, bytes)
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
            await self.__routing(task_id, TaskTransition.SchedulerHasNoTask, task=task)

    async def __state_canceled(
        self, task_id: TaskID, state_machine: TaskStateMachine, task_cancel_confirm: TaskCancelConfirm
    ):
        assert task_id == task_cancel_confirm.task_id
        assert task_cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.Canceled
        assert state_machine.current_state() == TaskState.Canceled

        if task_cancel_confirm.task_id in self._unassigned or task_cancel_confirm.task_id in self._unassignable:
            pass  # if task is not assigned to any worker, we don't need to deal with worker manager
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
        if not worker:
            logging.error(f"{task_cancel.task_id!r}: cannot find task in worker to cancel")
            await self.__routing(
                task_cancel.task_id,
                TaskTransition.TaskCancelConfirmNotFound,
                task_cancel_confirm=TaskCancelConfirm.new_msg(
                    task_cancel.task_id, TaskCancelConfirmType.CancelNotFound
                ),
            )
            return

        await self._binder.send(worker, TaskCancel.new_msg(task_cancel.task_id))
        await self.__send_monitor(task_cancel.task_id, b"")

    async def __send_task_result_to_client(self, task_result: TaskResult):
        await self._worker_controller.on_task_done(task_result.task_id)
        client = self._client_controller.on_task_finish(task_result.task_id)
        await self._binder.send(client, task_result)
        await self.__send_monitor(task_result.task_id, b"", task_result.metadata)

        self._task_state_manager.remove_state_machine(task_result.task_id)
        self._task_id_to_task.pop(task_result.task_id)

        if self._graph_controller.is_graph_sub_task(task_result.task_id):
            await self._graph_controller.on_graph_sub_task_result(task_result)

        await self.__retry_unassignable()

    async def __send_task_cancel_confirm_to_client(self, task_cancel_confirm: TaskCancelConfirm):
        client = self._client_controller.on_task_finish(task_cancel_confirm.task_id)
        await self._binder.send(client, task_cancel_confirm)
        await self.__send_monitor(task_cancel_confirm.task_id, b"")
        self._task_state_manager.remove_state_machine(task_cancel_confirm.task_id)
        self._task_id_to_task.pop(task_cancel_confirm.task_id)

        if self._graph_controller.is_graph_sub_task(task_cancel_confirm.task_id):
            await self._graph_controller.on_graph_sub_task_cancel_confirm(task_cancel_confirm)

        await self.__retry_unassignable()

    async def __send_monitor(self, task_id: TaskID, function_name: bytes, metadata: Optional[bytes] = b""):
        worker = self._worker_controller.get_worker_by_task_id(task_id)
        task_state = self._task_state_manager.get_state_machine(task_id).current_state()
        await self._binder_monitor.send(StateTask.new_msg(task_id, function_name, task_state, worker, metadata))

    async def __retry_unassignable(self):
        if not self._unassignable:
            return

        unassignable = self._unassignable.copy()
        self._unassignable.clear()
        for item in unassignable:
            await self._unassigned.put(item)

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
