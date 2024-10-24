import logging
import threading
from concurrent.futures import Future, InvalidStateError
from typing import Dict, Tuple

from scaler.client.agent.mixins import FutureManager
from scaler.client.future import ScalerFuture
from scaler.client.serializer.mixins import Serializer
from scaler.io.utility import concat_list_of_bytes
from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import ObjectResponse, TaskResult
from scaler.utility.exceptions import DisconnectedError, NoWorkerError, TaskNotFoundError, WorkerDiedError
from scaler.utility.metadata.profile_result import retrieve_profiling_result_from_task_result
from scaler.utility.object_utility import deserialize_failure


class ClientFutureManager(FutureManager):
    def __init__(self, serializer: Serializer):
        self._lock = threading.RLock()
        self._serializer = serializer

        self._task_id_to_future: Dict[bytes, ScalerFuture] = dict()
        self._object_id_to_future: Dict[bytes, Tuple[TaskStatus, ScalerFuture]] = dict()

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

    def set_all_futures_with_exception(self, exception: Exception):
        with self._lock:
            for future in self._task_id_to_future.values():
                try:
                    future.set_exception(exception)
                except InvalidStateError:
                    continue  # Future got canceled

            self._task_id_to_future = dict()

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

                if result.status == TaskStatus.Success:
                    assert len(result.results) == 1
                    result_object_id = result.results[0]
                    future.set_result_ready(result_object_id, profile_result)
                    self._object_id_to_future[result_object_id] = result.status, future
                    return

                if result.status == TaskStatus.Failed:
                    assert len(result.results) == 1
                    result_object_id = result.results[0]
                    future.set_result_ready(result_object_id, profile_result)
                    self._object_id_to_future[result_object_id] = result.status, future
                    return

                raise TypeError(f"Unknown task status: {result.status}")
            except InvalidStateError:
                return  # Future got canceled

    def on_object_response(self, response: ObjectResponse):
        for object_id, object_name, object_bytes in zip(
            response.object_content.object_ids,
            response.object_content.object_names,
            response.object_content.object_bytes,
        ):
            if object_id not in self._object_id_to_future:
                continue

            status, future = self._object_id_to_future.pop(object_id)

            try:
                if status == TaskStatus.Success:
                    future.set_result(self._serializer.deserialize(concat_list_of_bytes(object_bytes)))

                elif status == TaskStatus.Failed:
                    future.set_exception(deserialize_failure(concat_list_of_bytes(object_bytes)))
            except InvalidStateError:
                continue  # future got canceled
