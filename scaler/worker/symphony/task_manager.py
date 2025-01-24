import asyncio
import logging
import uuid
from concurrent.futures import Future
from typing import Dict, Optional, Set, cast

import cloudpickle
from bidict import bidict

from scaler import Serializer
from scaler.io.async_connector import AsyncConnector
from scaler.io.utility import chunk_to_list_of_bytes, concat_list_of_bytes
from scaler.protocol.python.common import ObjectContent, TaskStatus
from scaler.protocol.python.message import (
    ObjectInstruction,
    ObjectRequest,
    ObjectResponse,
    Task,
    TaskCancel,
    TaskResult,
)
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.object_utility import generate_object_id, generate_serializer_object_id, serialize_failure
from scaler.utility.queues.async_sorted_priority_queue import AsyncSortedPriorityQueue
from scaler.worker.agent.mixins import TaskManager
from scaler.worker.symphony.session_callback import SessionCallback, SoamMessage

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

        self._task_id_to_task: Dict[bytes, Task] = dict()
        self._task_id_to_future: bidict[bytes, asyncio.Future] = bidict()

        self._process_task_lock = asyncio.Lock()  # to ensure the object cache is not accessed by multiple tasks
        self._serializers: Dict[bytes, Serializer] = dict()
        self._object_cache: Dict[bytes, bytes] = dict()  # this cache is ephemeral and is wiped after task processing

        # locks are used to wait for object responses to be received
        self._serializers_lock: Dict[bytes, asyncio.Lock] = dict()
        self._object_cache_lock: Dict[bytes, asyncio.Lock] = dict()

        self._queued_task_id_queue = AsyncSortedPriorityQueue()
        self._queued_task_ids: Set[bytes] = set()

        self._acquiring_task_ids: Set[bytes] = set()  # tasks contesting the semaphore
        self._processing_task_ids: Set[bytes] = set()
        self._canceled_task_ids: Set[bytes] = set()

        self._connector_external: Optional[AsyncConnector] = None

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

    def register(self, connector: AsyncConnector):
        self._connector_external = connector

    async def routine(self):  # SymphonyTaskManager has two loops
        pass

    async def on_object_instruction(self, instruction: ObjectInstruction):
        pass

    async def on_object_response(self, response: ObjectResponse):
        if response.response_type != ObjectResponse.ObjectResponseType.Content:
            raise TypeError(f"invalid object response type received: {response.response_type}.")

        object_content = response.object_content

        for object_type, object_id, object_bytes in zip(
            object_content.object_types, object_content.object_ids, object_content.object_bytes
        ):
            if object_type == ObjectContent.ObjectContentType.Serializer:
                self._serializers[object_id] = cloudpickle.loads(concat_list_of_bytes(object_bytes))
                self._serializers_lock[object_id].release()
            elif object_type == ObjectContent.ObjectContentType.Object:
                self._object_cache[object_id] = concat_list_of_bytes(object_bytes)
                self._object_cache_lock[object_id].release()
            else:
                raise ValueError(f"invalid object type received: {object_type}")

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

        if (not task_queued and not task_processing) or (task_processing and not task_cancel.flags.force):
            result = TaskResult.new_msg(task_cancel.task_id, TaskStatus.NotFound)
            await self._connector_external.send(result)
            return

        canceled_task = self._task_id_to_task[task_cancel.task_id]

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

        payload = [canceled_task.get_message().to_bytes()] if task_cancel.flags.retrieve_task_object else []
        result = TaskResult.new_msg(
            task_id=task_cancel.task_id, status=TaskStatus.Canceled, metadata=b"", results=payload
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
            return

        done, _ = await asyncio.wait(self._task_id_to_future.values(), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = self._task_id_to_future.inv.pop(future)
            task = self._task_id_to_task[task_id]

            if task_id in self._processing_task_ids:
                self._processing_task_ids.remove(task_id)

                if future.exception() is None:
                    serializer_id = generate_serializer_object_id(task.source)
                    serializer = self._serializers[serializer_id]

                    result_bytes = serializer.serialize(future.result())
                    status = TaskStatus.Success
                else:
                    result_bytes = serialize_failure(cast(Exception, future.exception()))
                    status = TaskStatus.Failed

                result_object_id = generate_object_id(task.source, uuid.uuid4().bytes)
                await self._connector_external.send(
                    ObjectInstruction.new_msg(
                        ObjectInstruction.ObjectInstructionType.Create,
                        task.source,
                        ObjectContent.new_msg(
                            object_ids=(result_object_id,),
                            object_types=(ObjectContent.ObjectContentType.Object,),
                            object_names=(f"<res {result_object_id.hex()[:6]}>".encode(),),
                            object_bytes=(chunk_to_list_of_bytes(result_bytes),),
                        ),
                    )
                )

                await self._connector_external.send(
                    TaskResult.new_msg(task_id, status, metadata=b"", results=[result_object_id])
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
        requested, locks are acquired to wait for object responses to be received.
        """
        async with self._process_task_lock:

            awaited_locks = []
            request_ids = []

            serializer_id = generate_serializer_object_id(task.source)
            if serializer_id not in self._serializers:
                serializer_lock = asyncio.Lock()
                await serializer_lock.acquire()
                self._serializers_lock[serializer_id] = serializer_lock
                awaited_locks.append(serializer_lock)
                request_ids.append(serializer_id)

            object_ids = (task.func_object_id, *(arg.data for arg in task.function_args))
            for object_id in object_ids:
                object_lock = asyncio.Lock()
                await object_lock.acquire()
                self._object_cache_lock[object_id] = object_lock
                awaited_locks.append(object_lock)
                request_ids.append(object_id)

            await self._connector_external.send(
                ObjectRequest.new_msg(ObjectRequest.ObjectRequestType.Get, tuple(request_ids))
            )
            await asyncio.gather(*[awaited_lock.acquire() for awaited_lock in awaited_locks])

            serializer = self._serializers[serializer_id]

            function = serializer.deserialize(self._object_cache[task.func_object_id])
            args = [serializer.deserialize(self._object_cache[arg.data]) for arg in task.function_args]

            self._object_cache.clear()
            self._object_cache_lock.clear()
            self._serializers_lock.clear()

        """
        SOAM specific code
        """
        input_message = SoamMessage()
        input_message.set_payload(cloudpickle.dumps((function, *args)))

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
