import asyncio
import logging
from concurrent.futures import Future
from typing import Dict, Optional, Set, cast

import cloudpickle
from bidict import bidict

from scaler import Serializer
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata, ObjectStorageAddress, TaskCancelConfirmType, TaskResultType
from scaler.protocol.python.message import ObjectInstruction, Task, TaskCancel, TaskCancelConfirm, TaskResult
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.mixins import HeartbeatManager, TaskManager
from scaler.worker_adapter.drivers.symphony.callback import SessionCallback
from scaler.worker_adapter.drivers.symphony.message import SoamMessage

try:
    import soamapi
except ImportError:
    raise ImportError("IBM Spectrum Symphony API not found, please install it with 'pip install soamapi'.")


class SymphonyTaskManager(Looper, TaskManager):
    def __init__(self, base_concurrency: int, service_name: str):
        if isinstance(base_concurrency, int) and base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be a possible integer, got {base_concurrency}")

        self._base_concurrency = base_concurrency
        self._service_name = service_name

        self._executor_semaphore = asyncio.Semaphore(value=self._base_concurrency)

        self._task_id_to_task: Dict[TaskID, Task] = dict()
        self._task_id_to_future: bidict[TaskID, asyncio.Future] = bidict()

        self._serializers: Dict[bytes, Serializer] = dict()

        self._queued_task_id_queue = AsyncPriorityQueue()
        self._queued_task_ids: Set[bytes] = set()

        self._acquiring_task_ids: Set[TaskID] = set()  # tasks contesting the semaphore
        self._processing_task_ids: Set[TaskID] = set()
        self._canceled_task_ids: Set[TaskID] = set()

        self._object_storage_address: Optional[ObjectStorageAddress] = None

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        """
        SOAM specific code
        """
        soamapi.initialize()

        self._session_callback = SessionCallback()

        self._ibm_soam_connection = soamapi.connect(
            self._service_name, soamapi.DefaultSecurityCallback("Guest", "Guest")
        )
        logging.info(f"established IBM Spectrum Symphony connection {self._ibm_soam_connection.get_id()}")

        ibm_soam_session_attr = soamapi.SessionCreationAttributes()
        ibm_soam_session_attr.set_session_type("RecoverableAllHistoricalData")
        ibm_soam_session_attr.set_session_name("ScalerSession")
        ibm_soam_session_attr.set_session_flags(soamapi.SessionFlags.PARTIAL_ASYNC)
        ibm_soam_session_attr.set_session_callback(self._session_callback)
        self._ibm_soam_session = self._ibm_soam_connection.create_session(ibm_soam_session_attr)
        logging.info(f"established IBM Spectrum Symphony session {self._ibm_soam_session.get_id()}")

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        heartbeat_manager: HeartbeatManager,
    ):
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._heartbeat_manager = heartbeat_manager

    async def routine(self):  # SymphonyTaskManager has two loops
        pass

    async def on_object_instruction(self, instruction: ObjectInstruction):
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            for object_id in instruction.object_metadata.object_ids:
                self._serializers.pop(object_id, None)  # we only cache serializers

            return

        logging.error(f"worker received unknown object instruction type {instruction=}")

    async def on_task_new(self, task: Task):
        task_priority = self.__get_task_priority(task)

        # if semaphore is locked, check if task is higher priority than all acquired tasks
        # if so, bypass acquiring and execute the task immediately
        if self._executor_semaphore.locked():
            for acquired_task_id in self._acquiring_task_ids:
                acquired_task = self._task_id_to_task[acquired_task_id]
                acquired_task_priority = self.__get_task_priority(acquired_task)
                if task_priority <= acquired_task_priority:
                    break
            else:
                self._task_id_to_task[task.task_id] = task
                self._processing_task_ids.add(task.task_id)
                self._task_id_to_future[task.task_id] = await self.__execute_task(task)
                return

        self._task_id_to_task[task.task_id] = task
        self._queued_task_id_queue.put_nowait((-task_priority, task.task_id))
        self._queued_task_ids.add(task.task_id)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        task_queued = task_cancel.task_id in self._queued_task_ids
        task_processing = task_cancel.task_id in self._processing_task_ids

        if not task_queued and not task_processing:
            await self._connector_external.send(
                TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.CancelNotFound
                )
            )
            return

        if task_processing and not task_cancel.flags.force:
            await self._connector_external.send(
                TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.CancelFailed
                )
            )
            return

        if task_queued:
            self._queued_task_ids.remove(task_cancel.task_id)
            self._queued_task_id_queue.remove(task_cancel.task_id)

            # task can be discarded because task was never submitted
            self._task_id_to_task.pop(task_cancel.task_id)

        if task_processing:
            future = self._task_id_to_future[task_cancel.task_id]
            future.cancel()

            # regardless of the future being canceled, the task is considered canceled and cleanup will occur later
            self._processing_task_ids.remove(task_cancel.task_id)
            self._canceled_task_ids.add(task_cancel.task_id)

        result = TaskCancelConfirm.new_msg(
            task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.Canceled
        )
        await self._connector_external.send(result)

    async def on_task_result(self, result: TaskResult):
        if result.task_id in self._queued_task_ids:
            self._queued_task_ids.remove(result.task_id)
            self._queued_task_id_queue.remove(result.task_id)

        self._processing_task_ids.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)

        await self._connector_external.send(result)

    def get_queued_size(self):
        return self._queued_task_id_queue.qsize()

    def can_accept_task(self):
        return not self._executor_semaphore.locked()

    async def resolve_tasks(self):
        if not self._task_id_to_future:
            await asyncio.sleep(0)
            return

        done, _ = await asyncio.wait(self._task_id_to_future.values(), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = self._task_id_to_future.inv.pop(future)
            task = self._task_id_to_task[task_id]

            if task_id in self._processing_task_ids:
                self._processing_task_ids.remove(task_id)

                if future.exception() is None:
                    serializer_id = ObjectID.generate_serializer_object_id(task.source)
                    serializer = self._serializers[serializer_id]
                    result_bytes = serializer.serialize(future.result())
                    result_type = TaskResultType.Success
                else:
                    result_bytes = serialize_failure(cast(Exception, future.exception()))
                    result_type = TaskResultType.Failed

                result_object_id = ObjectID.generate_object_id(task.source)

                await self._connector_storage.set_object(result_object_id, result_bytes)
                await self._connector_external.send(
                    ObjectInstruction.new_msg(
                        ObjectInstruction.ObjectInstructionType.Create,
                        task.source,
                        ObjectMetadata.new_msg(
                            object_ids=(result_object_id,),
                            object_types=(ObjectMetadata.ObjectContentType.Object,),
                            object_names=(f"<res {result_object_id.hex()[:6]}>".encode(),),
                        ),
                    )
                )

                await self._connector_external.send(
                    TaskResult.new_msg(task_id, result_type, metadata=b"", results=[bytes(result_object_id)])
                )

            elif task_id in self._canceled_task_ids:
                self._canceled_task_ids.remove(task_id)

            else:
                raise ValueError(f"task_id {task_id.hex()} not found in processing or canceled tasks")

            if task_id in self._acquiring_task_ids:
                self._acquiring_task_ids.remove(task_id)
                self._executor_semaphore.release()

            self._task_id_to_task.pop(task_id)

    async def process_task(self):
        await self._executor_semaphore.acquire()

        _, task_id = await self._queued_task_id_queue.get()
        task = self._task_id_to_task[task_id]

        self._acquiring_task_ids.add(task_id)
        self._processing_task_ids.add(task_id)
        self._task_id_to_future[task.task_id] = await self.__execute_task(task)

    async def __execute_task(self, task: Task) -> asyncio.Future:
        """
        This method is not very efficient because it does let objects linger in the cache. Each time inputs are
        requested, all object data are requested.
        """
        serializer_id = ObjectID.generate_serializer_object_id(task.source)

        if serializer_id not in self._serializers:
            serializer_bytes = await self._connector_storage.get_object(serializer_id)
            serializer = cloudpickle.loads(serializer_bytes)
            self._serializers[serializer_id] = serializer
        else:
            serializer = self._serializers[serializer_id]

        # Fetches the function object and the argument objects concurrently

        get_tasks = [
            self._connector_storage.get_object(object_id)
            for object_id in [task.func_object_id, *(cast(ObjectID, arg) for arg in task.function_args)]
        ]

        function_bytes, *arg_bytes = await asyncio.gather(*get_tasks)

        function = serializer.deserialize(function_bytes)
        arg_objects = [serializer.deserialize(object_bytes) for object_bytes in arg_bytes]

        """
        SOAM specific code
        """
        input_message = SoamMessage()
        input_message.set_payload(cloudpickle.dumps((function, *arg_objects)))

        task_attr = soamapi.TaskSubmissionAttributes()
        task_attr.set_task_input(input_message)

        with self._session_callback.get_callback_lock():
            symphony_task = self._ibm_soam_session.send_task_input(task_attr)

            future: Future = Future()
            future.set_running_or_notify_cancel()

            self._session_callback.submit_task(symphony_task.get_id(), future)

        return asyncio.wrap_future(future)

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
