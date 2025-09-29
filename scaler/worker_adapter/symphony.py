import array
import asyncio
import concurrent.futures
import logging
import multiprocessing
import os
import signal
import threading
import time
import uuid
from collections import deque
from concurrent.futures import Future
from typing import Dict, Optional, Set, Tuple, cast

import cloudpickle
import psutil
import zmq
import zmq.asyncio
from aiohttp import web
from aiohttp.web_request import Request
from bidict import bidict

from scaler import Serializer
from scaler.io.async_connector import ZMQAsyncConnector
from scaler.io.async_object_storage_connector import PyAsyncObjectStorageConnector
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata, ObjectStorageAddress, TaskCancelConfirmType, TaskResultType
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskResult,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import Resource
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.queues.async_sorted_priority_queue import AsyncSortedPriorityQueue
from scaler.utility.serialization import serialize_failure
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.agent.mixins import HeartbeatManager, TaskManager, TimeoutManager
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager

try:
    import soamapi
except ImportError:
    raise ImportError("IBM Spectrum Symphony API not found, please install it with 'pip install soamapi'.")

WorkerGroupID = bytes


class CapacityExceededError(Exception):
    pass


class WorkerGroupNotFoundError(Exception):
    pass


class SymphonyWorkerAdapter:
    def __init__(
            self,
            address: ZMQConfig,
            storage_address: Optional[ObjectStorageConfig],
            service_name: str,
            base_concurrency: int,
            capabilities: Dict[str, int],
            io_threads: int,
            task_queue_size: int,
            heartbeat_interval_seconds: int,
            death_timeout_seconds: int,
            event_loop: str,
            logging_paths: Tuple[str, ...],
            logging_level: str,
            logging_config_file: Optional[str],
    ):
        self._address = address
        self._storage_address = storage_address
        self._service_name = service_name
        self._base_concurrency = base_concurrency
        self._capabilities = capabilities
        self._io_threads = io_threads
        self._task_queue_size = task_queue_size
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._event_loop = event_loop
        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        """
        Although a worker group can contain multiple workers, in this Symphony adapter implementation,
        there will be only one worker group which contains one Symphony worker.
        """
        self._worker_groups: Dict[WorkerGroupID, Dict[WorkerID, SymphonyWorker]] = {}

    async def start_worker_group(self) -> WorkerGroupID:
        if self._worker_groups:
            raise CapacityExceededError("Symphony worker already started")

        worker = SymphonyWorker(
            name=uuid.uuid4().hex,
            address=self._address,
            storage_address=self._storage_address,
            service_name=self._service_name,
            base_concurrency=self._base_concurrency,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            event_loop=self._event_loop,
        )

        worker.start()
        worker_group_id = f"symphony-{uuid.uuid4().hex}".encode()
        self._worker_groups[worker_group_id] = {worker.identity: worker}
        return worker_group_id

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID):
        if worker_group_id not in self._worker_groups:
            raise WorkerGroupNotFoundError(f"Worker group with ID {worker_group_id.decode()} does not exist.")

        for worker in self._worker_groups[worker_group_id].values():
            os.kill(worker.pid, signal.SIGINT)
            worker.join()

        self._worker_groups.pop(worker_group_id)

    async def webhook_handler(self, request: Request):
        request_json = await request.json()

        if "action" not in request_json:
            return web.json_response({"error": "No action specified"}, status=web.HTTPBadRequest.status_code)

        action = request_json["action"]

        if action == "start_worker_group":
            try:
                worker_group_id = await self.start_worker_group()
            except CapacityExceededError as e:
                return web.json_response({"error": str(e)}, status=web.HTTPTooManyRequests.status_code)
            except Exception as e:
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response(
                {
                    "status": "Worker group started",
                    "worker_group_id": worker_group_id.decode(),
                    "worker_ids": [worker_id.decode() for worker_id in self._worker_groups[worker_group_id].keys()],
                },
                status=web.HTTPOk.status_code,
            )

        elif action == "shutdown_worker_group":
            if "worker_group_id" not in request_json:
                return web.json_response(
                    {"error": "No worker_group_id specified"}, status=web.HTTPBadRequest.status_code
                )

            worker_group_id = request_json["worker_group_id"].encode()
            try:
                await self.shutdown_worker_group(worker_group_id)
            except WorkerGroupNotFoundError as e:
                return web.json_response({"error": str(e)}, status=web.HTTPNotFound.status_code)
            except Exception as e:
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response({"status": "Worker group shutdown"}, status=web.HTTPOk.status_code)

        else:
            return web.json_response({"error": "Unknown action"}, status=web.HTTPBadRequest.status_code)

    def create_app(self):
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        return app


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

        self._queued_task_id_queue = AsyncSortedPriorityQueue()
        self._queued_task_ids: Set[bytes] = set()

        self._acquiring_task_ids: Set[TaskID] = set()  # tasks contesting the semaphore
        self._processing_task_ids: Set[TaskID] = set()
        self._canceled_task_ids: Set[TaskID] = set()

        self._storage_address: Optional[ObjectStorageAddress] = None

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


class SymphonyHeartbeatManager(Looper, HeartbeatManager):
    def __init__(
            self, storage_address: Optional[ObjectStorageConfig], capabilities: Dict[str, int], task_queue_size: int
    ):
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size

        self._agent_process = psutil.Process()

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._worker_task_manager: Optional[SymphonyTaskManager] = None
        self._timeout_manager: Optional[TimeoutManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0

        self._storage_address: Optional[ObjectStorageConfig] = storage_address

    def register(
            self,
            connector_external: AsyncConnector,
            connector_storage: AsyncObjectStorageConnector,
            worker_task_manager: SymphonyTaskManager,
            timeout_manager: TimeoutManager,
    ):
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._worker_task_manager = worker_task_manager
        self._timeout_manager = timeout_manager

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        if self._start_timestamp_ns == 0:
            # not handling echo if we didn't send out heartbeat
            return

        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

        if self._storage_address is None:
            address_message = heartbeat.object_storage_address()
            self._storage_address = ObjectStorageConfig(address_message.host, address_message.port)
            await self._connector_storage.connect(self._storage_address.host, self._storage_address.port)

    def get_storage_address(self) -> Optional[ObjectStorageConfig]:
        return self._storage_address

    async def routine(self):
        if self._start_timestamp_ns != 0:
            return

        await self._connector_external.send(
            WorkerHeartbeat.new_msg(
                Resource.new_msg(int(self._agent_process.cpu_percent() * 10), self._agent_process.memory_info().rss),
                psutil.virtual_memory().available,
                self._task_queue_size,
                self._worker_task_manager.get_queued_size(),
                self._latency_us,
                self._worker_task_manager.can_accept_task(),
                [],
                self._capabilities,
            )
        )
        self._start_timestamp_ns = time.time_ns()


class SymphonyWorker(multiprocessing.get_context("spawn").Process):  # type: ignore
    """
    SymphonyWorker is an implementation of a worker that can handle multiple tasks concurrently.
    Most of the task execution logic is handled by SymphonyTaskManager.
    """

    def __init__(
            self,
            name: str,
            address: ZMQConfig,
            storage_address: Optional[ObjectStorageConfig],
            service_name: str,
            capabilities: Dict[str, int],
            base_concurrency: int,
            heartbeat_interval_seconds: int,
            death_timeout_seconds: int,
            task_queue_size: int,
            io_threads: int,
            event_loop: str,
    ):
        multiprocessing.Process.__init__(self, name="Agent")

        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._storage_address = storage_address
        self._capabilities = capabilities
        self._io_threads = io_threads

        self._ident = WorkerID.generate_worker_id(name)  # _identity is internal to multiprocessing.Process

        self._service_name = service_name
        self._base_concurrency = base_concurrency

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._task_queue_size = task_queue_size

        self._context: Optional[zmq.asyncio.Context] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._task_manager: Optional[SymphonyTaskManager] = None
        self._heartbeat_manager: Optional[SymphonyHeartbeatManager] = None

        """
        Sometimes the first message received is not a heartbeat echo, so we need to backoff processing other tasks
        until we receive the first heartbeat echo.
        """
        self._heartbeat_received: bool = False
        self._backoff_message_queue: deque = deque()

    @property
    def identity(self) -> WorkerID:
        return self._ident

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        setup_logger()
        register_event_loop(self._event_loop)

        self._context = zmq.asyncio.Context()
        self._connector_external = ZMQAsyncConnector(
            context=self._context,
            name=self.name,
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
            identity=self._ident,
        )

        self._connector_storage = PyAsyncObjectStorageConnector()

        self._heartbeat_manager = SymphonyHeartbeatManager(
            storage_address=self._storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._task_queue_size,
        )
        self._task_manager = SymphonyTaskManager(
            base_concurrency=self._base_concurrency, service_name=self._service_name
        )
        self._timeout_manager = VanillaTimeoutManager(death_timeout_seconds=self._death_timeout_seconds)

        # register
        self._heartbeat_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            worker_task_manager=self._task_manager,
            timeout_manager=self._timeout_manager,
        )
        self._task_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            heartbeat_manager=self._heartbeat_manager,
        )

        self._loop = asyncio.get_event_loop()
        self.__register_signal()
        self._task = self._loop.create_task(self.__get_loops())

    async def __on_receive_external(self, message: Message):
        if not self._heartbeat_received and not isinstance(message, WorkerHeartbeatEcho):
            self._backoff_message_queue.append(message)
            return

        if isinstance(message, WorkerHeartbeatEcho):
            await self._heartbeat_manager.on_heartbeat_echo(message)
            self._heartbeat_received = True

            while self._backoff_message_queue:
                backoff_message = self._backoff_message_queue.popleft()
                await self.__on_receive_external(backoff_message)

            return

        if isinstance(message, Task):
            await self._task_manager.on_task_new(message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_cancel_task(message)
            return

        if isinstance(message, ObjectInstruction):
            await self._task_manager.on_object_instruction(message)
            return

        if isinstance(message, ClientDisconnect):
            if message.disconnect_type == ClientDisconnect.DisconnectType.Shutdown:
                raise ClientShutdownException("received client shutdown, quitting")
            logging.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self):
        if self._storage_address is not None:
            # With a manually set storage address, immediately connect to the object storage server.
            await self._connector_storage.connect(self._storage_address.host, self._storage_address.port)

        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._task_manager.process_task, 0),
                create_async_loop_routine(self._task_manager.resolve_tasks, 0),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as e:
            logging.info(f"{self.identity!r}: {str(e)}")
        except Exception as e:
            logging.exception(f"{self.identity!r}: failed with unhandled exception:\n{e}")

        await self._connector_external.send(DisconnectRequest.new_msg(self.identity))

        self._connector_external.destroy()
        logging.info(f"{self.identity!r}: quit")

    def __run_forever(self):
        self._loop.run_until_complete(self._task)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)

    def __destroy(self):
        self._task.cancel()


class SoamMessage(soamapi.Message):
    def __init__(self, payload: bytes = b""):
        self.__payload = payload

    def set_payload(self, payload: bytes):
        self.__payload = payload

    def get_payload(self) -> bytes:
        return self.__payload

    def on_serialize(self, stream):
        payload_array = array.array("b", self.get_payload())
        stream.write_byte_array(payload_array, 0, len(payload_array))

    def on_deserialize(self, stream):
        self.set_payload(stream.read_byte_array("b"))


class SessionCallback(soamapi.SessionCallback):
    def __init__(self):
        self._callback_lock = threading.Lock()
        self._task_id_to_future: Dict[str, concurrent.futures.Future] = {}

    def on_response(self, task_output_handle):
        with self._callback_lock:
            task_id = task_output_handle.get_id()

            future = self._task_id_to_future.pop(task_id)

            if task_output_handle.is_successful():
                output_message = SoamMessage()
                task_output_handle.populate_task_output(output_message)
                result = cloudpickle.loads(output_message.get_payload())
                future.set_result(result)
            else:
                future.set_exception(task_output_handle.get_exception().get_embedded_exception())

    def on_exception(self, exception):
        with self._callback_lock:
            for future in self._task_id_to_future.values():
                future.set_exception(exception)

            self._task_id_to_future.clear()

    def submit_task(self, task_id: str, future: concurrent.futures.Future):
        self._task_id_to_future[task_id] = future

    def get_callback_lock(self) -> threading.Lock:
        return self._callback_lock
