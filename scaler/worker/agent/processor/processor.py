import contextlib
import logging
import multiprocessing
import os
import signal
from contextvars import ContextVar, Token
from multiprocessing.synchronize import Event as EventType
from typing import Callable, List, Optional, Tuple, cast

import tblib.pickling_support

from scaler.io.config import DUMMY_CLIENT
from scaler.io.sync_connector import SyncConnector
from scaler.io.sync_object_storage_connector import SyncObjectStorageConnector
from scaler.io.ymq.ymq import IOContext, IOSocketType
from scaler.protocol.python.common import ObjectMetadata, TaskStatus
from scaler.protocol.python.message import ObjectInstruction, ProcessorInitialized, Task, TaskResult
from scaler.protocol.python.mixins import Message
from scaler.utility.logging.utility import setup_logger
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.serialization import serialize_failure
from scaler.utility.ymq_config import YMQConfig
from scaler.worker.agent.processor.object_cache import ObjectCache

SUSPEND_SIGNAL = "SIGUSR1"  # use str instead of a signal.Signal to not trigger an import error on unsupported systems.

_current_processor: ContextVar[Optional["Processor"]] = ContextVar("_current_processor", default=None)


class Processor(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        event_loop: str,
        agent_address: YMQConfig,
        storage_address: ObjectStorageConfig,
        resume_event: Optional[EventType],
        resumed_event: Optional[EventType],
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        logging_paths: Tuple[str, ...],
        logging_level: str,
    ):
        multiprocessing.Process.__init__(self, name="Processor")

        self._event_loop = event_loop
        self._agent_address = agent_address
        self._storage_address = storage_address

        self._resume_event = resume_event
        self._resumed_event = resumed_event

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._logging_paths = logging_paths
        self._logging_level = logging_level

        self._object_cache: Optional[ObjectCache] = None

        self._current_task: Optional[Task] = None

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    @staticmethod
    def get_current_processor() -> Optional["Processor"]:
        """Returns the current Processor instance controlling the current process, if any."""
        return _current_processor.get()

    def current_task(self) -> Optional[Task]:
        return self._current_task

    def __initialize(self):
        # modify the logging path and add process id to the path
        logging_paths = [f"{path}-{os.getpid()}" for path in self._logging_paths if path != "/dev/stdout"]
        if "/dev/stdout" in self._logging_paths:
            logging_paths.append("/dev/stdout")

        setup_logger(log_paths=tuple(logging_paths), logging_level=self._logging_level)
        tblib.pickling_support.install()

        self._connector_agent = SyncConnector(
            context=IOContext(), socket_type=IOSocketType.Connector, address=self._agent_address, identity=None
        )
        self._connector_storage = SyncObjectStorageConnector(self._storage_address.host, self._storage_address.port)

        self._object_cache = ObjectCache(
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
        )
        self._object_cache.start()

        self.__register_signals()

    def __register_signals(self):
        self.__register_signal("SIGTERM", self.__interrupt)

        if self._resume_event is not None:
            self.__register_signal(SUSPEND_SIGNAL, self.__suspend)

    def __interrupt(self, *args):
        self._connector_agent.close()  # interrupts any blocking socket.

    def __suspend(self, *args):
        assert self._resume_event is not None
        assert self._resumed_event is not None

        self._resume_event.wait()  # stops any computation in the main thread until the event is triggered

        # Ensures the processor agent knows we stopped waiting on `_resume_event`, as to avoid re-entrant wait on the
        # event.
        self._resumed_event.set()

    def __run_forever(self):
        try:
            self._connector_agent.send(ProcessorInitialized.new_msg())
            while True:
                message = self._connector_agent.receive()
                if message is None:
                    continue

                self.__on_connector_receive(message)

        except (KeyboardInterrupt, InterruptedError):
            pass

        except Exception as e:
            logging.exception(f"Processor[{self.pid}]: failed with unhandled exception:\n{(e)}")

        finally:
            self._object_cache.destroy()
            self._connector_agent.close()

            self._object_cache.join()

    def __on_connector_receive(self, message: Message):
        if isinstance(message, ObjectInstruction):
            self.__on_receive_object_instruction(message)
            return

        if isinstance(message, Task):
            self.__on_received_task(message)
            return

        logging.error(f"unknown {message=}")

    def __on_receive_object_instruction(self, instruction: ObjectInstruction):
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            for object_id in instruction.object_metadata.object_ids:
                self._object_cache.del_object(object_id)
            return

        logging.error(f"worker received unknown object instruction type {instruction=}")

    def __on_received_task(self, task: Task):
        self._current_task = task

        self.__cache_required_object_ids(task)

        self.__process_task(task)

    def __cache_required_object_ids(self, task: Task) -> None:
        required_object_ids = self.__get_required_object_ids_for_task(task)

        for object_id in required_object_ids:
            if self._object_cache.has_object(object_id):
                continue

            object_content = self._connector_storage.get_object(object_id)
            self._object_cache.add_object(task.source, object_id, object_content)

    @staticmethod
    def __get_required_object_ids_for_task(task: Task) -> List[ObjectID]:
        serializer_id = ObjectID.generate_serializer_object_id(task.source)
        object_ids = [
            serializer_id,
            task.func_object_id,
            *(cast(ObjectID, argument) for argument in task.function_args),
        ]
        return object_ids

    def __process_task(self, task: Task):
        try:
            function = self._object_cache.get_object(task.func_object_id)
            function_with_logger = self.__get_object_with_client_logger(DUMMY_CLIENT, function)

            args = [self._object_cache.get_object(cast(ObjectID, arg)) for arg in task.function_args]

            with self.__processor_context():
                result = function_with_logger(*args)
            result_bytes = self._object_cache.serialize(task.source, result)
            status = TaskStatus.Success

        except Exception as e:
            logging.exception(f"exception when processing task_id={task.task_id.hex()}:")
            status = TaskStatus.Failed
            result_bytes = serialize_failure(e)

        self.__send_result(task.source, task.task_id, status, result_bytes)

    def __get_object_with_client_logger(self, client: ClientID, fn: Callable) -> Callable:
        assert self is not None
        return fn

        # if client in self._client_to_decorator:
        #     wrap = self._client_to_decorator[client]
        #     return wrap(fn)
        #
        # def _generate_wrapper(handler: WorkerLogPublisher):
        #     def decorator(func: Callable):
        #         @functools.wraps(func)
        #         def wrapper(*args, **kwargs):
        #             logger = logging.getLogger()
        #             logger.addHandler(handler)
        #             result = func(*args, **kwargs)
        #             logger.removeHandler(handler)
        #             return result
        #
        #         return wrapper
        #
        #     return decorator
        #
        # wrap = _generate_wrapper(WorkerLogPublisher(client, self._log_hub_address, log_level=logging.DEBUG))
        # self._client_to_decorator[client] = wrap
        # return wrap(fn)

    def __send_result(self, source: ClientID, task_id: TaskID, status: TaskStatus, result_bytes: bytes):
        self._current_task = None

        result_object_id = ObjectID.generate_object_id(source)

        self._connector_storage.set_object(result_object_id, result_bytes)
        self._connector_agent.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Create,
                source,
                ObjectMetadata.new_msg(
                    (result_object_id,),
                    (ObjectMetadata.ObjectContentType.Object,),
                    (f"<res {repr(result_object_id)}>".encode(),),
                ),
            )
        )
        self._connector_agent.send(TaskResult.new_msg(task_id, status, metadata=b"", results=[bytes(result_object_id)]))

    @staticmethod
    def __set_current_processor(context: Optional["Processor"]) -> Token:
        if context is not None and _current_processor.get() is not None:
            raise ValueError("cannot override a previously set processor context.")

        return _current_processor.set(context)

    @contextlib.contextmanager
    def __processor_context(self):
        self.__set_current_processor(self)
        try:
            yield
        finally:
            self.__set_current_processor(None)

    @staticmethod
    def __register_signal(signal_name: str, handler: Callable) -> None:
        signal_instance = getattr(signal, signal_name, None)
        if signal_instance is None:
            raise RuntimeError(f"unsupported platform, signal not available: {signal_name}.")

        signal.signal(signal_instance, handler)
