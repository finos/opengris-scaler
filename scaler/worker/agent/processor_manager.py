import asyncio
import logging
import os
import tempfile
import uuid
from typing import Dict, List, Optional, Tuple

import tblib.pickling_support
import zmq.asyncio

# from scaler.utility.logging.utility import setup_logger
from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.io.utility import chunk_to_list_of_bytes
from scaler.protocol.python.common import ObjectContent, TaskStatus
from scaler.protocol.python.message import (
    ObjectInstruction,
    ObjectRequest,
    ObjectResponse,
    ProcessorInitialized,
    Task,
    TaskResult,
)
from scaler.protocol.python.mixins import Message
from scaler.utility.exceptions import ProcessorDiedError
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.mixins import Looper
from scaler.utility.object_utility import generate_object_id, serialize_failure
from scaler.utility.zmq_config import ZMQConfig, ZMQType
from scaler.worker.agent.mixins import HeartbeatManager, ObjectTracker, ProcessorManager, ProfilingManager, TaskManager
from scaler.worker.agent.processor_holder import ProcessorHolder


class VanillaProcessorManager(Looper, ProcessorManager):
    def __init__(
        self,
        context: zmq.asyncio.Context,
        event_loop: str,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        hard_processor_suspend: bool,
        logging_paths: Tuple[str, ...],
        logging_level: str,
    ):
        tblib.pickling_support.install()

        self._event_loop = event_loop

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._hard_processor_suspend = hard_processor_suspend
        self._logging_paths = logging_paths
        self._logging_level = logging_level

        self._address_path = os.path.join(tempfile.gettempdir(), f"scaler_worker_{uuid.uuid4().hex}")
        self._address = ZMQConfig(ZMQType.ipc, host=self._address_path)

        self._heartbeat: Optional[HeartbeatManager] = None
        self._task_manager: Optional[TaskManager] = None
        self._profiling_manager: Optional[ProfilingManager] = None
        self._object_tracker: Optional[ObjectTracker] = None
        self._connector_external: Optional[AsyncConnector] = None

        self._current_holder: Optional[ProcessorHolder] = None
        self._suspended_holders_by_task_id: Dict[bytes, ProcessorHolder] = {}
        self._holders_by_processor_id: Dict[bytes, ProcessorHolder] = {}

        self._can_accept_task_lock: asyncio.Lock = asyncio.Lock()

        self._binder_internal: AsyncBinder = AsyncBinder(
            context=context, name="processor_manager", address=self._address, identity=None
        )
        self._binder_internal.register(self.__on_receive_internal)

    def register(
        self,
        heartbeat: HeartbeatManager,
        task_manager: TaskManager,
        profiling_manager: ProfilingManager,
        object_tracker: ObjectTracker,
        connector_external: AsyncConnector,
    ):
        self._heartbeat = heartbeat
        self._task_manager = task_manager
        self._profiling_manager = profiling_manager
        self._object_tracker = object_tracker
        self._connector_external = connector_external

    async def initialize(self):
        # setup_logger()
        await self._can_accept_task_lock.acquire()  # prevents processor to accept task until initialized
        self.__start_new_processor()

    async def routine(self):
        await self._binder_internal.routine()

    async def on_object_instruction(self, instruction: ObjectInstruction):
        processor_instructions = self._object_tracker.on_object_instruction(instruction)

        for processor_id, instruction in processor_instructions.items():
            await self._binder_internal.send(processor_id, instruction)

    async def on_object_response(self, response: ObjectResponse):
        processors_ids = self._object_tracker.on_object_response(response)

        for process_id in processors_ids:
            await self._binder_internal.send(process_id, response)

    def can_accept_task(self) -> bool:
        return self._can_accept_task_lock.locked()

    async def wait_until_can_accept_task(self):
        """
        Makes sure a processor is ready to start processing a new or suspended task.

        Must be called before any call to `on_task()` or `on_task_resume()`.
        """

        await self._can_accept_task_lock.acquire()

    async def on_task(self, task: Task) -> bool:
        assert self._can_accept_task_lock.locked()
        assert self.initialized()

        holder = self._current_holder

        assert holder.task() is None
        holder.set_task(task)

        self._profiling_manager.on_task_start(holder.pid(), task.task_id)

        await self._binder_internal.send(holder.processor_id(), task)

        return True

    async def on_cancel_task(self, task_id: bytes) -> Optional[Task]:
        assert self._current_holder is not None

        if self.current_task_id() == task_id:
            current_task = self.current_task()
            self.__restart_current_processor(f"cancel task_id={task_id.hex()}")
            return current_task

        if task_id in self._suspended_holders_by_task_id:
            suspended_holder = self._suspended_holders_by_task_id.pop(task_id)
            task = suspended_holder.task()
            self.__kill_processor(f"cancel suspended task_id={task_id.hex()}", suspended_holder)
            return task

        return None

    async def on_failing_processor(self, processor_id: bytes, process_status: str):
        assert self._current_holder is not None

        holder = self._holders_by_processor_id.get(processor_id)

        if holder is None:
            return

        task = holder.task()
        if task is not None:
            profile_result = self.__end_task(holder)  # profiling the task should happen before killing the processor
        else:
            profile_result = None

        reason = f"process died {process_status=}"
        if holder == self._current_holder:
            self.__restart_current_processor(reason)
        else:
            self.__kill_processor(reason, holder)

        if task is not None:
            source = task.source
            task_id = task.task_id

            result_object_bytes = chunk_to_list_of_bytes(serialize_failure(ProcessorDiedError(f"{process_status=}")))

            result_object_id = generate_object_id(source, uuid.uuid4().bytes)
            await self._connector_external.send(
                ObjectInstruction.new_msg(
                    ObjectInstruction.ObjectInstructionType.Create,
                    source,
                    ObjectContent.new_msg((result_object_id,), (b"",), (result_object_bytes,)),
                )
            )

            await self._task_manager.on_task_result(
                TaskResult.new_msg(task_id, TaskStatus.Failed, profile_result.serialize(), [result_object_id])
            )

    async def on_suspend_task(self, task_id: bytes) -> bool:
        assert self._current_holder is not None
        holder = self._current_holder

        current_task = holder.task()

        if current_task is None or current_task.task_id != task_id:
            return False

        holder.suspend()
        self._suspended_holders_by_task_id[task_id] = holder

        logging.info(f"Worker[{os.getpid()}]: suspend Processor[{holder.pid()}]")

        self.__start_new_processor()

        return True

    def on_resume_task(self, task_id: bytes) -> bool:
        assert self._can_accept_task_lock.locked()
        assert self.initialized()

        if self.current_task() is not None:
            return False

        suspended_holder = self._suspended_holders_by_task_id.pop(task_id, None)

        if suspended_holder is None:
            return False

        self.__kill_processor("replaced by suspended processor", self._current_holder)

        self._current_holder = suspended_holder
        suspended_holder.resume()

        logging.info(f"Worker[{os.getpid()}]: resume Processor[{self._current_holder.pid()}]")

        return True

    def destroy(self, reason: str):
        self.__kill_all_processors(reason)
        self._binder_internal.destroy()
        os.remove(self._address_path)

    def initialized(self) -> bool:
        return self._current_holder is not None and self._current_holder.initialized()

    def current_task(self) -> Optional[Task]:
        assert self._current_holder is not None
        return self._current_holder.task()

    def current_task_id(self) -> bytes:
        task = self.current_task()

        if task is None:
            return b""
        else:
            return task.task_id

    def processors(self) -> List[ProcessorHolder]:
        return list(self._holders_by_processor_id.values())

    def num_suspended_processors(self) -> int:
        return len(self._suspended_holders_by_task_id)

    def __start_new_processor(self):
        self._current_holder = ProcessorHolder(
            self._event_loop,
            self._address,
            self._garbage_collect_interval_seconds,
            self._trim_memory_threshold_bytes,
            self._hard_processor_suspend,
            self._logging_paths,
            self._logging_level,
        )

        processor_pid = self._current_holder.pid()

        self._profiling_manager.on_process_start(processor_pid)

        logging.info(f"Worker[{os.getpid()}]: start Processor[{processor_pid}]")

    def __kill_processor(self, reason: str, holder: ProcessorHolder):
        processor_pid = holder.pid()

        self._profiling_manager.on_process_end(processor_pid)

        if holder.initialized():
            self._holders_by_processor_id.pop(holder.processor_id(), None)
            self._object_tracker.on_processor_end(holder.processor_id())

        holder.kill()

        logging.info(f"Worker[{os.getpid()}]: stop Processor[{processor_pid}], reason: {reason}")

    def __restart_current_processor(self, reason: str):
        assert self._current_holder is not None

        self.__kill_processor(reason, self._current_holder)
        self.__start_new_processor()

    def __kill_all_processors(self, reason: str):
        if self._current_holder is not None:
            self.__kill_processor(reason, self._current_holder)
            self._current_holder = None

        for processor_holder in self._suspended_holders_by_task_id.values():
            self.__kill_processor(reason, processor_holder)

        self._suspended_holders_by_task_id = {}
        self._holders_by_processor_id = {}

    def __end_task(self, processor_holder: ProcessorHolder) -> ProfileResult:
        profile_result = self._profiling_manager.on_task_end(processor_holder.pid(), processor_holder.task().task_id)
        processor_holder.set_task(None)

        return profile_result

    async def __on_receive_internal(self, processor_id: bytes, message: Message):
        if isinstance(message, ProcessorInitialized):
            await self.__on_internal_processor_initialized(processor_id)
            return

        if isinstance(message, ObjectRequest):
            await self.__on_internal_object_request(processor_id, message)
            return

        if isinstance(message, ObjectInstruction):
            await self.__on_internal_object_instruction(processor_id, message)
            return

        if isinstance(message, TaskResult):
            await self.__on_internal_task_result(processor_id, message)
            return

        raise TypeError(f"Unknown message from {processor_id=}: {message}")

    async def __on_internal_processor_initialized(self, processor_id: bytes):
        assert self._current_holder is not None

        if self._current_holder.initialized():
            return

        self._holders_by_processor_id[processor_id] = self._current_holder
        self._current_holder.initialize(processor_id)

        self._can_accept_task_lock.release()

    async def __on_internal_object_request(self, processor_id: bytes, request: ObjectRequest):
        if not self.__processor_ready_to_process_object(processor_id):
            return

        self._object_tracker.on_object_request(processor_id, request)
        await self._connector_external.send(request)

    async def __on_internal_object_instruction(self, processor_id: bytes, instruction: ObjectInstruction):
        if not self.__processor_ready_to_process_object(processor_id):
            return

        await self._connector_external.send(instruction)

    async def __on_internal_task_result(self, processor_id: bytes, task_result: TaskResult):
        assert self._current_holder is not None
        task_id = task_result.task_id

        if task_id == self.current_task_id():
            assert self._current_holder.processor_id() == processor_id

            profile_result = self.__end_task(self._current_holder)

            release_task_lock = True
        elif task_id in self._suspended_holders_by_task_id:
            # Receiving a task result from a suspended processor is possible as the message might have been queued while
            # we were suspending the process.

            holder = self._suspended_holders_by_task_id.pop(task_id)
            assert holder.processor_id() == processor_id

            profile_result = self.__end_task(holder)

            self.__kill_processor("task finished in suspended processor", holder)

            release_task_lock = False
        else:
            return

        await self._task_manager.on_task_result(
            TaskResult.new_msg(
                task_id=task_id,
                status=task_result.status,
                metadata=profile_result.serialize(),
                results=task_result.results,
            )
        )

        # task lock must be released after calling `TaskManager.on_task_result()`
        if release_task_lock:
            self._can_accept_task_lock.release()

    def __processor_ready_to_process_object(self, processor_id: bytes) -> bool:
        holder = self._holders_by_processor_id.get(processor_id)

        if holder is None:
            return False

        assert holder.initialized()

        if holder.task() is None:
            return False

        # TODO: check if the objects belong to the task

        return True
