import logging
import threading
from concurrent.futures import Future, InvalidStateError
from typing import Dict, Optional

from scaler.client.agent.mixins import FutureManager
from scaler.client.future import ScalerFuture
from scaler.client.serializer.mixins import Serializer
from scaler.protocol.python.common import TaskCancelConfirmType, TaskResultType, TaskState
from scaler.protocol.python.message import TaskCancelConfirm, TaskResult
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
        with self._lock:
            task_id = result.task_id
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert result.task_id == future.task_id

            profile_result = retrieve_profiling_result_from_task_result(result)

            if result.result_type == TaskResultType.FailedWorkerDied:
                future.set_exception(
                    WorkerDiedError(f"worker died when processing task: {task_id.hex()}"), profile_result
                )

            elif result.result_type == TaskResultType.Success:
                result_object_id = self.__get_result_object_id(result)
                future.set_result_ready(result_object_id, TaskState.Success, profile_result)

            elif result.result_type == TaskResultType.Failed:
                result_object_id = self.__get_result_object_id(result)
                future.set_result_ready(result_object_id, TaskState.Failed, profile_result)

            else:
                raise TypeError(f"{result.task_id.hex()}: Unknown task status: {result.result_type}")

    def on_task_cancel_confirm(self, cancel_confirm: TaskCancelConfirm):
        with self._lock:
            task_id = cancel_confirm.task_id
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert cancel_confirm.task_id == future.task_id

            if cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.Canceled:
                future.set_canceled()

            elif cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.CancelNotFound:
                logging.error(f"{task_id!r}: task to cancel not found")
                future.set_canceled()

            elif cancel_confirm.cancel_confirm_type == TaskCancelConfirmType.CancelFailed:
                logging.error(f"{task_id!r}: task cancel failed")
                self._task_id_to_future[task_id] = future

            else:
                raise TypeError(
                    f"{task_id}: unknown task cancel confirm type:" f" {cancel_confirm.cancel_confirm_type}"
                )

    @staticmethod
    def __get_result_object_id(result: TaskResult) -> Optional[ObjectID]:
        if len(result.results) == 1:
            result_object_id = ObjectID(result.results[0])
        elif len(result.results) == 0:
            # this will happen only if umbrella task is done
            result_object_id = None
        else:
            raise ValueError(f"{result.task_id!r}: received multiple objects for the results: {len(result.results)=}")

        return result_object_id
