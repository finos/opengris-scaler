import logging
import threading
from concurrent.futures import Future, InvalidStateError
from typing import Dict, Optional

from scaler.client.agent.mixins import FutureManager
from scaler.client.future import ScalerFuture
from scaler.client.serializer.mixins import Serializer
from scaler.protocol.capnp import TaskCancelConfirm, TaskCancelConfirmType, TaskResult, TaskResultType, TaskState
from scaler.utility.exceptions import WorkerDiedError
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.profile_result import retrieve_profiling_result_from_task_result


class ClientFutureManager(FutureManager):
    def __init__(self, serializer: Serializer):
        self._lock = threading.RLock()
        self._serializer = serializer

        self._task_id_to_future: Dict[TaskID, ScalerFuture] = dict()

    def add_future(self, future: Future):
        assert isinstance(future, ScalerFuture)
        with self._lock:
            future.set_running_or_notify_cancel()
            self._task_id_to_future[future.task_id] = future

    def cancel_all_futures(self):
        with self._lock:
            futures_to_cancel = list(self._task_id_to_future.values())

        # Actually cancelling the futures should occur without holding the future manager's lock. That's because
        # `cancel()` is blocking, and requires the manager to process result and cancel confirm messages.

        logging.info(f"canceling {len(futures_to_cancel)} task(s)")
        for future in futures_to_cancel:
            future.cancel()

    def set_all_futures_with_exception(self, exception: Exception):
        with self._lock:
            for future in self._task_id_to_future.values():
                try:
                    future.set_exception(exception)
                except InvalidStateError:
                    continue  # Future got canceled

            self._task_id_to_future.clear()

    def on_task_result(self, result: TaskResult):
        result_type = TaskResultType(result.resultType.raw)
        with self._lock:
            task_id = result.taskId
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert result.taskId == future.task_id

            profile_result = retrieve_profiling_result_from_task_result(result)

            if result_type == TaskResultType.failedWorkerDied:
                future.set_exception(
                    WorkerDiedError(f"worker died when processing task: {task_id.hex()}"), profile_result
                )

            elif result_type == TaskResultType.success:
                result_object_id = self.__get_result_object_id(result)
                future.set_result_ready(result_object_id, TaskState.success, profile_result)

            elif result_type == TaskResultType.failed:
                result_object_id = self.__get_result_object_id(result)
                future.set_result_ready(result_object_id, TaskState.failed, profile_result)

            else:
                raise TypeError(f"{result.taskId.hex()}: Unknown task status: {result.resultType}")

    def on_task_cancel_confirm(self, cancel_confirm: TaskCancelConfirm):
        cancel_confirm_type = TaskCancelConfirmType(cancel_confirm.cancelConfirmType.raw)
        with self._lock:
            task_id = cancel_confirm.taskId
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert cancel_confirm.taskId == future.task_id

            if cancel_confirm_type == TaskCancelConfirmType.canceled:
                future.set_canceled()

            elif cancel_confirm_type == TaskCancelConfirmType.cancelNotFound:
                logging.error(f"{task_id!r}: task to cancel not found")
                future.set_canceled()

            elif cancel_confirm_type == TaskCancelConfirmType.cancelFailed:
                logging.error(f"{task_id!r}: task cancel failed")
                self._task_id_to_future[task_id] = future

            else:
                raise TypeError(f"{task_id!r}: unknown task cancel confirm type: {cancel_confirm.cancelConfirmType}")

    @staticmethod
    def __get_result_object_id(result: TaskResult) -> Optional[ObjectID]:
        if len(result.results) == 1:
            result_object_id = ObjectID(result.results[0])
        elif len(result.results) == 0:
            # this will happen only if umbrella task is done
            result_object_id = None
        else:
            raise ValueError(f"{result.taskId!r}: received multiple objects for the results: {len(result.results)=}")

        return result_object_id
