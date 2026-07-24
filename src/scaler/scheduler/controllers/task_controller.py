import asyncio
import logging
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from scaler.io.mixins import AsyncBinder, AsyncPublisher
from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError
from scaler.protocol.capnp import (
    BaseMessage,
    StateTask,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskManagerStatus,
    TaskResult,
    TaskResultType,
    TaskState,
    TaskTransition,
)
from scaler.protocol.helpers import capabilities_to_dict, dict_to_capabilities
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

logger = logging.getLogger(__name__)


class VanillaTaskController(TaskController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None

        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._worker_controller: Optional[WorkerController] = None

        self._graph_controller: Optional[GraphTaskController] = None

        self._task_id_to_task: Dict[TaskID, Task] = dict()
        self._task_state_manager: TaskStateManager = TaskStateManager(debug=True)

        self._unassigned: Deque[TaskID] = deque()  # type: ignore[misc]

        self._state_functions: Dict[TaskState, Callable[[*Tuple[Any, ...]], Awaitable[None]]] = {
            TaskState.inactive: self.__state_inactive,
            TaskState.running: self.__state_running,
            TaskState.canceling: self.__state_canceling,
            TaskState.balanceCanceling: self.__state_balance_canceling,
            TaskState.workerDisconnecting: self.__state_worker_disconnecting,
            TaskState.canceled: self.__state_canceled,
            TaskState.canceledNotFound: self.__state_canceled_not_found,
            TaskState.success: self.__state_success,
            TaskState.failed: self.__state_failed,
            TaskState.failedWorkerDied: self.__state_failed_worker_died,
        }
        self._task_result_transition_map = {
            TaskResultType.success: TaskTransition.taskResultSuccess,
            TaskResultType.failed: TaskTransition.taskResultFailed,
            TaskResultType.failedWorkerDied: TaskTransition.taskResultWorkerDied,
        }
        self._task_cancel_confirm_transition_map = {
            TaskCancelConfirmType.canceled: TaskTransition.taskCancelConfirmCanceled,
            TaskCancelConfirmType.cancelNotFound: TaskTransition.taskCancelConfirmNotFound,
            TaskCancelConfirmType.cancelFailed: TaskTransition.taskCancelConfirmFailed,
        }

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncPublisher,
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
        task.capabilities = capabilities_to_dict(task.capabilities)
        if self._task_state_manager.get_state_machine(task.taskId) is not None:
            logger.error(
                f"{task.taskId!r}: state machine already exists: "
                f"{self._task_state_manager.get_state_machine(task.taskId)}"
            )
            return

        state_machine = self._task_state_manager.add_state_machine(task.taskId)
        await self.__state_inactive(task_id=task.taskId, state_machine=state_machine, task=task)

    async def on_task_cancel(self, client_id: ClientID, task_cancel: TaskCancel):
        state_machine = self._task_state_manager.get_state_machine(task_cancel.taskId)
        if state_machine is None:
            logger.error(f"{task_cancel.taskId!r}: task not exists while received TaskCancel, send TaskCancelConfirm")

            task_cancel_confirm = TaskCancelConfirm(
                taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.cancelNotFound
            )

            if self._graph_controller.is_graph_subtask(task_cancel.taskId):
                await self._graph_controller.on_graph_sub_task_cancel_confirm(task_cancel_confirm)

            await self._binder.send(client_id, task_cancel_confirm)
            return

        if state_machine.current_state() == TaskState.inactive:
            await self.__routing(
                task_cancel.taskId,
                TaskTransition.taskCancel,
                task_cancel_confirm=TaskCancelConfirm(
                    taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.canceled
                ),
            )
            return

        await self.__routing(task_cancel.taskId, TaskTransition.taskCancel, client=client_id, task_cancel=task_cancel)

    async def on_task_balance_cancel(self, task_id: TaskID):
        await self.__routing(task_id, TaskTransition.balanceTaskCancel)

    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        cancel_confirm_type = TaskCancelConfirmType(task_cancel_confirm.cancelConfirmType.value)
        transition = self._task_cancel_confirm_transition_map.get(cancel_confirm_type, None)
        if transition is None:
            raise ValueError(f"unknown TaskCancelConfirmType: {task_cancel_confirm.cancelConfirmType}")

        state_machine = self._task_state_manager.get_state_machine(task_cancel_confirm.taskId)
        if state_machine is None:
            logger.error(
                f"{task_cancel_confirm.taskId!r}: task not exists while received TaskCancelTaskCancelConfirm, ignore"
            )
            return

        current_state = state_machine.current_state()
        if current_state == TaskState.balanceCanceling and cancel_confirm_type == TaskCancelConfirmType.canceled:
            # if balance cancel success
            task = self._task_id_to_task[task_cancel_confirm.taskId]
            await self.__routing(task_cancel_confirm.taskId, transition, task=task)
            return

        if (
            current_state in {TaskState.canceling, TaskState.balanceCanceling}
            and cancel_confirm_type == TaskCancelConfirmType.cancelFailed
        ):
            # cancel failed (task is ongoing on worker), pass worker_id so __state_running can identify the
            # previous state and wait for the real result
            worker_id = self._worker_controller.get_worker_by_task_id(task_cancel_confirm.taskId)
            await self.__routing(task_cancel_confirm.taskId, transition, worker_id=worker_id)
            return

        await self.__routing(task_cancel_confirm.taskId, transition, task_cancel_confirm=task_cancel_confirm)

    async def on_task_result(self, task_result: TaskResult):
        result_type = TaskResultType(task_result.resultType.value)
        transition = self._task_result_transition_map.get(result_type, None)
        if transition is None:
            raise ValueError(f"unknown TaskResultType: {task_result.resultType}")

        await self.__routing(task_result.taskId, transition, task_result=task_result)

    async def on_worker_connect(self, worker_id: WorkerID):
        await self.__retry_unassignable()

    async def on_worker_disconnect(self, task_id: TaskID, worker_id: WorkerID):
        state_machine = self._task_state_manager.get_state_machine(task_id)
        if state_machine is not None and state_machine.current_state() == TaskState.canceling:
            # canceling routes workerDisconnect to canceled, whose handler takes a TaskCancelConfirm, not
            # the worker_id the workerDisconnecting targets take.
            await self.__routing(
                task_id,
                TaskTransition.workerDisconnect,
                task_cancel_confirm=TaskCancelConfirm(taskId=task_id, cancelConfirmType=TaskCancelConfirmType.canceled),
            )
            return

        await self.__routing(task_id, TaskTransition.workerDisconnect, worker_id=worker_id)

    def get_status(self) -> TaskManagerStatus:
        statistics = self._task_state_manager.get_statistics()

        return TaskManagerStatus(
            stateToCount=[
                TaskManagerStatus.Pair(state=task_state.value, count=count) for task_state, count in statistics.items()
            ]
        )

    async def __state_inactive(self, task_id: TaskID, state_machine: TaskStateMachine, task: Task):
        assert task_id == task.taskId
        assert state_machine.current_state() == TaskState.inactive

        if state_machine.previous_state() == TaskState.balanceCanceling:
            # balance cancel confirmed: deregister the task from its old worker before re-acquiring
            await self._worker_controller.on_task_done(task_id)

        self._client_controller.on_task_begin(task.source, task.taskId)
        self._task_id_to_task[task.taskId] = task

        worker_id = self._worker_controller.acquire_worker(self._task_id_to_task[task_id])
        if not worker_id.is_valid():
            # put task on hold until there is worker is added or task is finished/canceled (means have capacity)
            self._unassigned.append(task_id)
            await self.__send_monitor(task.taskId, self._object_controller.get_object_name(task.funcObjectId))
            return

        await self.__routing(task_id, TaskTransition.hasCapacity, worker_id=worker_id)
        await self.__send_monitor(task.taskId, self._object_controller.get_object_name(task.funcObjectId))

    async def __state_running(self, task_id: TaskID, state_machine: TaskStateMachine, worker_id: WorkerID):
        if state_machine.previous_state() in {TaskState.canceling, TaskState.balanceCanceling}:
            # if cancel failed (task is ongoing), we should wait here for the result
            return

        assert state_machine.current_state() == TaskState.running

        task = self._task_id_to_task[task_id]
        if not await self.__send_to_worker(worker_id, task):
            return
        await self.__send_monitor(task_id, self._object_controller.get_object_name(task.funcObjectId))

    async def __state_canceling(
        self, task_id: TaskID, state_machine: TaskStateMachine, client: ClientID, task_cancel: TaskCancel
    ):
        assert task_id == task_cancel.taskId
        assert state_machine.current_state() == TaskState.canceling

        if state_machine.previous_state() == TaskState.balanceCanceling:
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
            TaskTransition.taskCancelConfirmCanceled,
            task_cancel_confirm=TaskCancelConfirm(taskId=task_id, cancelConfirmType=TaskCancelConfirmType.canceled),
        )

        if task_id in self._unassigned:
            self._unassigned.remove(task_id)

    async def __state_balance_canceling(self, task_id: TaskID, state_machine: TaskStateMachine):
        assert state_machine.current_state() == TaskState.balanceCanceling
        await self.__send_task_cancel_to_worker(
            TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=False))
        )

    async def __state_worker_disconnecting(self, task_id: TaskID, state_machine: TaskStateMachine, worker_id: WorkerID):
        worker_id = WorkerID(worker_id)
        assert state_machine.current_state() == TaskState.workerDisconnecting

        # this is where we decide to reroute or just send fail
        task = self._task_id_to_task.get(task_id)
        if task is None:
            await self.__routing(
                task_id,
                TaskTransition.schedulerHasNoTask,
                task_result=TaskResult(
                    taskId=task_id, resultType=TaskResultType.failedWorkerDied, metadata=b"", results=[]
                ),
            )
        else:
            await self.__routing(task_id, TaskTransition.schedulerHasTask, task=task)

    async def __state_canceled(
        self, task_id: TaskID, state_machine: TaskStateMachine, task_cancel_confirm: TaskCancelConfirm
    ):
        assert task_id == task_cancel_confirm.taskId
        assert task_cancel_confirm.cancelConfirmType == TaskCancelConfirmType.canceled
        assert state_machine.current_state() == TaskState.canceled

        if task_cancel_confirm.taskId in self._unassigned:
            # if task is not assigned to any worker, we don't need to deal with worker manager
            self._unassigned.remove(task_cancel_confirm.taskId)
        else:
            await self._worker_controller.on_task_done(task_cancel_confirm.taskId)

        await self.__send_task_cancel_confirm_to_client(task_cancel_confirm)

    async def __state_canceled_not_found(
        self, task_id: TaskID, state_machine: TaskStateMachine, task_cancel_confirm: TaskCancelConfirm
    ):
        assert task_id == task_cancel_confirm.taskId
        assert task_cancel_confirm.cancelConfirmType == TaskCancelConfirmType.cancelNotFound
        assert state_machine.current_state() == TaskState.canceledNotFound

        await self.__send_task_cancel_confirm_to_client(task_cancel_confirm)

    async def __state_success(self, task_id: TaskID, state_machine: TaskStateMachine, task_result: TaskResult):
        assert task_id == task_result.taskId
        assert state_machine.current_state() == TaskState.success
        await self.__send_task_result_to_client(task_result)

    async def __state_failed(self, task_id: TaskID, state_machine: TaskStateMachine, task_result: TaskResult):
        assert task_id == task_result.taskId
        assert state_machine.current_state() == TaskState.failed
        await self.__send_task_result_to_client(task_result)

    async def __state_failed_worker_died(
        self, task_id: TaskID, state_machine: TaskStateMachine, task_result: TaskResult
    ):
        assert task_id == task_result.taskId
        assert state_machine.current_state() == TaskState.failedWorkerDied
        logger.warning(f"{task_id!r}: failed because its worker died; reporting failedWorkerDied to the client")
        await self.__send_task_result_to_client(task_result)

    async def __send_task_cancel_to_worker(self, task_cancel: TaskCancel):
        worker = await self._worker_controller.on_task_cancel(task_cancel)
        assert isinstance(worker, WorkerID)
        if not worker.is_valid():
            logger.error(f"{task_cancel.taskId!r}: cannot find task in worker to cancel")
            await self.__routing(
                task_cancel.taskId,
                TaskTransition.taskCancelConfirmNotFound,
                task_cancel_confirm=TaskCancelConfirm(
                    taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.cancelNotFound
                ),
            )
            return

        if not await self.__send_to_worker(worker, task_cancel):
            return
        await self.__send_monitor(task_cancel.taskId, b"")

    async def __send_task_result_to_client(self, task_result: TaskResult):
        await self._worker_controller.on_task_done(task_result.taskId)
        client = self._client_controller.on_task_finish(task_result.taskId)
        if client is None:
            logger.warning(
                f"{task_result.taskId!r}: dropping {task_result.resultType} result, owning client is no longer "
                f"registered (likely disconnected via client_timeout_seconds while the task was running)"
            )
        else:
            await self.__send_to_client(client, task_result)

        func_name = b""
        task = self._task_id_to_task.get(task_result.taskId)
        if task:
            func_name = self._object_controller.get_object_name(task.funcObjectId)
        await self.__send_monitor(task_result.taskId, func_name, task_result.metadata)

        self._task_state_manager.remove_state_machine(task_result.taskId)
        self._task_id_to_task.pop(task_result.taskId)

        if self._graph_controller.is_graph_subtask(task_result.taskId):
            await self._graph_controller.on_graph_sub_task_result(task_result)

        await self.__retry_unassignable()

    async def __send_task_cancel_confirm_to_client(self, task_cancel_confirm: TaskCancelConfirm):
        client = self._client_controller.on_task_finish(task_cancel_confirm.taskId)
        if client is None:
            logger.warning(
                f"{task_cancel_confirm.taskId!r}: dropping task cancel confirm, owning client is no "
                f"longer registered"
            )
        else:
            await self.__send_to_client(client, task_cancel_confirm)
        await self.__send_monitor(task_cancel_confirm.taskId, b"")
        self._task_state_manager.remove_state_machine(task_cancel_confirm.taskId)
        self._task_id_to_task.pop(task_cancel_confirm.taskId)

        if self._graph_controller.is_graph_subtask(task_cancel_confirm.taskId):
            await self._graph_controller.on_graph_sub_task_cancel_confirm(task_cancel_confirm)

        await self.__retry_unassignable()

    async def __send_monitor(self, task_id: TaskID, function_name: bytes, metadata: bytes = b""):
        worker = self._worker_controller.get_worker_by_task_id(task_id)
        task_state = self._task_state_manager.get_state_machine(task_id).current_state()
        capabilities = self._task_id_to_task[task_id].capabilities if task_id in self._task_id_to_task else []
        await self._binder_monitor.send(
            StateTask(
                taskId=task_id,
                functionName=function_name,
                state=task_state,
                worker=worker,
                capabilities=dict_to_capabilities(capabilities),
                metadata=metadata,
            )
        )

    async def __send_to_worker(self, worker_id: WorkerID, message: BaseMessage) -> bool:
        """Send to a worker, returning False if its socket was already closed by the peer.

        A worker can die between its last heartbeat and this send (pod eviction, crash), and the closed
        socket only surfaces here. A dead worker must never crash the scheduler, so swallow the error --
        raised from a timer loop (not the binder receive loop) it would otherwise propagate through
        asyncio.gather and tear the scheduler down. Rerouting the worker's tasks is left to the disconnect
        path (its DisconnectRequest, or the heartbeat-timeout sweep); the False return only tells the
        caller to stop this transition.
        """
        try:
            await self._binder.send(worker_id, message)
            return True
        except ConnectorSocketClosedByRemoteEndError:
            logger.info(f"{worker_id!r}: departed before {type(message).__name__} could be sent")
            return False

    async def __send_to_client(self, client_id: ClientID, message: BaseMessage) -> None:
        """Deliver a result/cancel-confirm to a client, dropping it if the client has departed.

        Swallowing the departed-client error HERE (rather than letting it raise out to __routing) is what
        keeps the caller's cleanup running -- remove_state_machine, _task_id_to_task.pop and, critically,
        __retry_unassignable. If it escaped, a client killed while its tasks are completing would leave
        those tasks' state machines in limbo and, because the retry is skipped, freed workers would never
        be handed the queued tasks, wedging the scheduler.
        """
        try:
            await self._binder.send(client_id, message)
        except ConnectorSocketClosedByRemoteEndError:
            logger.info(f"{client_id!r}: departed, dropping undeliverable task message")

    async def __routing(self, task_id: TaskID, transition: TaskTransition, **kwargs):
        state_machine = self._task_state_manager.on_transition(task_id, transition)
        if state_machine is None:
            logger.info(f"{task_id!r}: unknown transition: {transition}")
            return

        try:
            await self._state_functions[state_machine.current_state()](task_id, state_machine, **kwargs)  # noqa
        except ConnectorSocketClosedByRemoteEndError:
            # Defensive net for any undeliverable send during a transition. __send_to_worker and
            # __send_to_client swallow their own departed-peer errors, but anything they miss must still
            # not be fatal here: raised from a timer loop (balancer/cleanup) rather than the binder receive
            # loop, re-raising would propagate through asyncio.gather and tear the whole scheduler down.
            logger.info(f"{task_id!r}: peer departed during {transition}, dropping undeliverable message")
        except Exception:
            # A bug in a state function must not crash the scheduler: __routing runs both from message
            # handlers and from the balance/cleanup timer loops, so re-raising would propagate through
            # asyncio.gather. Log the transition + state path and drop it -- the task may stall (surfaced by
            # the client/worker timeouts), but the scheduler stays alive.
            logger.exception(f"{task_id!r}: transition {transition} failed, path: {state_machine.get_path()}")

    async def __retry_unassignable(self):
        futures = [
            self.__routing(task_id, TaskTransition.hasCapacity, worker_id=worker_id)
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
