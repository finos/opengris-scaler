import logging
import threading
from concurrent.futures import Future, InvalidStateError
from typing import Dict

from scaler.client.agent.mixins import FutureManager
from scaler.client.future import ScalerFuture
from scaler.client.serializer.mixins import Serializer
from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import TaskCancel, TaskResult
from scaler.utility.exceptions import DisconnectedError, NoWorkerError, TaskNotFoundError, WorkerDiedError
from scaler.utility.metadata.profile_result import retrieve_profiling_result_from_task_result
from scaler.utility.identifiers import ObjectID


class ClientFutureManager(FutureManager):
    def __init__(self, serializer: Serializer):
        self._lock = threading.RLock()
        self._serializer = serializer

        self._task_id_to_future: Dict[bytes, ScalerFuture] = dict()

    def add_future(self, future: Future):
        assert isinstance(future, ScalerFuture)
        with self._lock:
            future.set_running_or_notify_cancel()
            self._task_id_to_future[future.task_id] = future

    def cancel_all_futures(self):
        with self._lock:
            logging.info(f"canceling {len(self._task_id_to_future)} task(s)")
            for task_id, future in self._task_id_to_future.items():
                future.cancel()

            self._task_id_to_future.clear()

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

            try:
                if result.status == TaskStatus.NotFound:
                    future.set_exception(TaskNotFoundError(f"task not found: {task_id.hex()}"), profile_result)
                    return

                if result.status == TaskStatus.WorkerDied:
                    future.set_exception(
                        WorkerDiedError(f"worker died when processing task: {task_id.hex()}"), profile_result
                    )
                    return

                if result.status == TaskStatus.NoWorker:
                    future.set_exception(
                        NoWorkerError(f"no available worker when processing task: {task_id.hex()}"), profile_result
                    )
                    return

                if result.status == TaskStatus.Canceled:
                    future.set_exception(DisconnectedError("client disconnected"), profile_result)
                    return

                if result.status in {TaskStatus.Success, TaskStatus.Failed}:
                    assert len(result.results) == 1
                    result_object_id = ObjectID(result.results[0])
                    future.set_result_ready(result_object_id, result.status, profile_result)
                    return

                raise TypeError(f"Unknown task status: {result.status}")
            except InvalidStateError:
                return  # Future got canceled

    def on_cancel_task(self, task_cancel: TaskCancel):
        with self._lock:
            self._task_id_to_future.pop(task_cancel.task_id, None)
