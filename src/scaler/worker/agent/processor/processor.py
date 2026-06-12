import contextlib
import logging
import multiprocessing
import os
import signal
import sys
import threading
from contextlib import redirect_stderr, redirect_stdout
from contextvars import ContextVar, Token
from multiprocessing.synchronize import Event as EventType
from typing import IO, Callable, List, Optional, Tuple, cast

import tblib.pickling_support

from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.mixins import ConnectorRemoteType, NetworkBackend, SyncConnector, SyncObjectStorageConnector
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    ActorCreate,
    ActorDestroy,
    ActorError,
    ActorMessage,
    ActorPayload,
    ActorState,
    ActorStateUpdate,
    BaseMessage,
    ObjectInstruction,
    ObjectMetadata,
    ProcessorInitialized,
    Task,
    TaskLog,
    TaskResult,
    TaskResultType,
)
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.processor.object_cache import ObjectCache
from scaler.worker.agent.processor.streaming_buffer import StreamingBuffer
from scaler.worker.preload import execute_preload

logger = logging.getLogger(__name__)

SUSPEND_SIGNAL = "SIGUSR1"  # use str instead of a signal.Signal to not trigger an import error on unsupported systems.

_current_processor: ContextVar[Optional["Processor"]] = ContextVar("_current_processor", default=None)


class Processor(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        event_loop: str,
        agent_address: AddressConfig,
        scheduler_address: AddressConfig,
        object_storage_address: AddressConfig,
        preload: Optional[str],
        resume_event: Optional[EventType],
        resumed_event: Optional[EventType],
        suspend_trigger: Optional[EventType],
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        logging_paths: Tuple[str, ...],
        logging_level: str,
    ):
        super().__init__(name="Processor")

        self._event_loop = event_loop
        self._agent_address = agent_address
        self._scheduler_address = scheduler_address
        self._object_storage_address = object_storage_address
        self._preload = preload

        self._identity = generate_identity_from_name(f"processor|{self.pid}")
        self._backend: Optional[NetworkBackend] = None

        self._resume_event = resume_event
        self._resumed_event = resumed_event
        self._suspend_trigger = suspend_trigger

        # _listener_shutdown is created in __initialize (the child process) because threading.Event
        # cannot be pickled across the spawn boundary on Windows.
        self._suspend_listener: Optional[threading.Thread] = None
        self._listener_shutdown: Optional[threading.Event] = None

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._logging_paths = logging_paths
        self._logging_level = logging_level

        self._object_cache: Optional[ObjectCache] = None

        self._current_task: Optional[Task] = None

        # actor mode: set when the worker is designated as an actor and the constructor
        # succeeded; the processor then responds to ActorMessage instead of Task
        self._actor_create: Optional[ActorCreate] = None
        self._actor_instance: Optional[object] = None
        self._stop_requested = False

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    @staticmethod
    def get_current_processor() -> Optional["Processor"]:
        """Returns the current Processor instance controlling the current process, if any."""
        return _current_processor.get()

    def scheduler_address(self) -> AddressConfig:
        """Returns the scheduler address this processor's worker is connected to."""
        return self._scheduler_address

    def current_task(self) -> Optional[Task]:
        return self._current_task

    def __initialize(self):
        self._listener_shutdown = threading.Event()

        # modify the logging path and add process id to the path
        logging_paths = [f"{path}-{os.getpid()}" for path in self._logging_paths if path != "/dev/stdout"]
        if "/dev/stdout" in self._logging_paths:
            logging_paths.append("/dev/stdout")

        setup_logger(log_paths=tuple(logging_paths), logging_level=self._logging_level)
        tblib.pickling_support.install()

        self._backend = get_network_backend_from_env()
        assert self._backend is not None

        self._connector_agent: SyncConnector = self._backend.create_sync_connector(
            identity=self._identity, connector_remote_type=ConnectorRemoteType.Binder, address=self._agent_address
        )

        logger.info(f"Processor[{self.pid}] connecting to object storage at {self._object_storage_address}...")
        self._connector_storage: SyncObjectStorageConnector = self._backend.create_sync_object_storage_connector(
            identity=self._identity, address=self._object_storage_address
        )

        self._object_cache = ObjectCache(
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
        )
        self._object_cache.start()

        self.__register_signals()

        # Execute optional preload hook if provided
        if self._preload is not None:
            try:
                execute_preload(self._preload)
            except Exception as e:
                raise RuntimeError(
                    f"Processor[{self.pid}] initialization failed due to preload error: {self._preload}"
                ) from e

    def __register_signals(self):
        if sys.platform != "win32":
            # Windows signal.signal() accepts SIGTERM but the OS does not deliver it from external commands,
            # so the handler would never run. Skip registration to keep the Windows code path honest.
            self.__register_signal("SIGTERM", self.__interrupt)

        if self._resume_event is None:
            return

        if sys.platform == "win32":
            assert self._suspend_trigger is not None
            self._suspend_listener = threading.Thread(
                target=self.__suspend_listener_main, name="ProcessorSuspendListener", daemon=True
            )
            self._suspend_listener.start()
        else:
            self.__register_signal(SUSPEND_SIGNAL, self.__suspend)

    def __interrupt(self, *args):
        self._connector_agent.destroy()  # interrupts any blocking socket.
        self._connector_storage.destroy()

    def __suspend(self, *args):
        assert self._resume_event is not None
        assert self._resumed_event is not None

        self._resume_event.wait()  # stops any computation in the main thread until the event is triggered

        # Ensures the processor agent knows we stopped waiting on `_resume_event`, as to avoid re-entrant wait on the
        # event.
        self._resumed_event.set()

    def __suspend_listener_main(self):
        # Bridges suspend requests from the parent (worker agent) into a Py_AddPendingCall scheduled on the
        # main interpreter thread. This is the Windows substitute for SIGUSR1: it dispatches `__suspend` at
        # the next CPython eval-breaker check, with the same safe-point semantics as a Python signal handler.
        from scaler.utility import pending_call

        assert self._suspend_trigger is not None
        assert self._listener_shutdown is not None
        while not self._listener_shutdown.is_set():
            if not self._suspend_trigger.wait(timeout=0.1):
                continue
            self._suspend_trigger.clear()
            if self._listener_shutdown.is_set():
                break
            try:
                pending_call.schedule(self.__suspend)
            except RuntimeError:
                logger.exception(f"Processor[{self.pid}]: failed to schedule suspend pending call")

    def __run_forever(self):
        try:
            self._connector_agent.send(ProcessorInitialized())
            while True:
                message = self._connector_agent.receive()
                if message is None:
                    continue

                self.__on_connector_receive(message)

                if self._stop_requested:
                    break

        except ymq.SocketStopRequestedError:
            pass

        except ObjectStorageException:
            pass

        except (KeyboardInterrupt, InterruptedError):
            pass

        except Exception as e:
            if self.__is_closed_zmq_socket_exception(e):
                return

            logger.exception(f"Processor[{self.pid}]: failed with unhandled exception:\n{e}")

        finally:
            # Wake the suspend listener (if running on Windows) and let it exit before the connectors go away,
            # so it never tries to schedule into a torn-down processor.
            if self._listener_shutdown is not None:
                self._listener_shutdown.set()
            if self._suspend_trigger is not None:
                self._suspend_trigger.set()
            if self._suspend_listener is not None:
                self._suspend_listener.join(timeout=1.0)

            self._object_cache.destroy()
            self._connector_agent.destroy()

            self._object_cache.join()
            self._connector_storage.destroy()

    def __on_connector_receive(self, message: BaseMessage):
        if isinstance(message, ObjectInstruction):
            self.__on_receive_object_instruction(message)
            return

        if isinstance(message, Task):
            self.__on_received_task(message)
            return

        if isinstance(message, ActorCreate):
            self.__on_received_actor_create(message)
            return

        if isinstance(message, ActorMessage):
            self.__on_received_actor_message(message)
            return

        if isinstance(message, ActorDestroy):
            self.__on_received_actor_destroy(message)
            return

        logger.error(f"unknown {message=}")

    def __on_receive_object_instruction(self, instruction: ObjectInstruction):
        if instruction.instructionType == ObjectInstruction.ObjectInstructionType.delete:
            for object_id in instruction.objectMetadata.objectIds:
                self._object_cache.del_object(object_id)
            return

        logger.error(f"worker received unknown object instruction type {instruction=}")

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
            self._object_cache.add_object(task.source, object_id, bytes(object_content))

    @staticmethod
    def __get_required_object_ids_for_task(task: Task) -> List[ObjectID]:
        serializer_id = ObjectID.generate_serializer_object_id(task.source)
        object_ids = [
            serializer_id,
            ObjectID(task.funcObjectId),
            *(ObjectID(argument.data) for argument in task.functionArgs),
        ]
        return object_ids

    def __on_received_actor_create(self, actor_create: ActorCreate):
        if self._actor_instance is not None:
            # should not happen (the worker agent designates one actor per processor), but if it
            # does, tell the scheduler this create failed instead of silently dropping it
            logging.error(f"Processor[{self.pid}]: already hosts an actor, ignoring ActorCreate")
            self.__send_actor_state_update(
                actor_create,
                ActorState.dead,
                death_info=ActorStateUpdate.DeathInfo(
                    reason=ActorStateUpdate.DeathInfo.Reason.unknownActor,
                    error=ActorError(
                        errorType="scaler.utility.exceptions.ActorNotFoundError",
                        message="processor already hosts another actor",
                    ),
                ),
            )
            return

        source = ClientID(bytes(actor_create.source))

        try:
            self.__cache_required_object_ids_for_actor(actor_create)

            actor_class = self._object_cache.get_object(ObjectID(bytes(actor_create.classObjectId)))
            arguments = actor_create.constructorArguments
            args = [self.__resolve_actor_payload(source, payload) for payload in arguments.positional]
            kwargs = {
                keyword_argument.name: self.__resolve_actor_payload(source, keyword_argument.value)
                for keyword_argument in arguments.keyword
            }

            with self.__processor_context():
                actor_instance = actor_class(*args, **kwargs)

        except Exception as e:
            logging.exception(f"Processor[{self.pid}]: failed to construct actor:")
            self.__send_actor_state_update(
                actor_create,
                ActorState.dead,
                death_info=ActorStateUpdate.DeathInfo(
                    reason=ActorStateUpdate.DeathInfo.Reason.constructorFailed,
                    error=ActorError(errorType=f"{type(e).__module__}.{type(e).__qualname__}", message=str(e)),
                ),
            )
            return

        self._actor_create = actor_create
        self._actor_instance = actor_instance
        self.__send_actor_state_update(actor_create, ActorState.alive)

    def __cache_required_object_ids_for_actor(self, actor_create: ActorCreate) -> None:
        source = ClientID(bytes(actor_create.source))
        arguments = actor_create.constructorArguments
        payloads = list(arguments.positional) + [keyword_argument.value for keyword_argument in arguments.keyword]

        object_ids = [ObjectID.generate_serializer_object_id(source), ObjectID(bytes(actor_create.classObjectId))]
        object_ids.extend(
            ObjectID(bytes(payload.data))
            for payload in payloads
            if payload.type == ActorPayload.ActorPayloadType.objectID
        )

        for object_id in object_ids:
            if self._object_cache.has_object(object_id):
                continue

            object_content = self._connector_storage.get_object(object_id)
            self._object_cache.add_object(source, object_id, bytes(object_content))

    def __resolve_actor_payload(self, source: ClientID, payload: ActorPayload):
        if payload.type == ActorPayload.ActorPayloadType.objectID:
            return self._object_cache.get_object(ObjectID(bytes(payload.data)))

        return self._object_cache.deserialize(source, bytes(payload.data))

    def __on_received_actor_message(self, actor_message: ActorMessage):
        if self._actor_instance is None:
            logging.error(f"Processor[{self.pid}]: received an ActorMessage but hosts no actor, dropping")
            return

        payload = bytes(actor_message.payload)

        receive_method = getattr(self._actor_instance, "__receive__", None)
        if receive_method is None:
            logging.error(
                f"Processor[{self.pid}]: actor {type(self._actor_instance).__name__} has no __receive__ "
                "method, dropping message"
            )
            return

        try:
            with self.__processor_context():
                reply = receive_method(payload)
        except Exception:
            # a failed delivery does not kill a stateful actor; the message plane is
            # fire-and-forget, so log and keep serving
            logging.exception(f"Processor[{self.pid}]: actor raised while handling a message:")
            return

        if reply is None:
            return

        # accept anything that exposes the buffer protocol (bytes, bytearray, memoryview, numpy
        # arrays, ...), not just bytes. Going through memoryview() is what rejects non-buffer
        # objects: bytes(5) returns b"\x00\x00\x00\x00\x00", but bytes(memoryview(5)) raises.
        try:
            payload = bytes(memoryview(reply))
        except TypeError:
            logging.error(
                f"Processor[{self.pid}]: __receive__ returned {type(reply).__name__}, expected bytes-like or "
                "None, dropping reply"
            )
            return

        self._connector_agent.send(
            ActorMessage(
                actorId=bytes(self._actor_create.actorId), source=bytes(self._actor_create.source), payload=payload
            )
        )

    def __on_received_actor_destroy(self, actor_destroy: ActorDestroy):
        if self._actor_create is None or bytes(actor_destroy.actorId) != bytes(self._actor_create.actorId):
            logging.error(f"Processor[{self.pid}]: received ActorDestroy for an actor it does not host, ignoring")
            return

        self.__send_actor_state_update(
            self._actor_create,
            ActorState.dead,
            death_info=ActorStateUpdate.DeathInfo(reason=ActorStateUpdate.DeathInfo.Reason.destroyed),
        )

        # exiting the process is what actually releases the actor's resources; the agent
        # detects the exit and starts a fresh processor
        self._stop_requested = True

    def __send_actor_state_update(
        self, actor_create: ActorCreate, state: ActorState, death_info: Optional[ActorStateUpdate.DeathInfo] = None
    ):
        fields = dict(actorId=bytes(actor_create.actorId), source=bytes(actor_create.source), workerId=b"", state=state)
        if death_info is not None:
            fields["deathInfo"] = death_info

        self._connector_agent.send(ActorStateUpdate(**fields))

    def __process_task(self, task: Task):
        task_flags = retrieve_task_flags_from_task(task)

        try:
            function = self._object_cache.get_object(ObjectID(task.funcObjectId))

            args = [self._object_cache.get_object(ObjectID(argument.data)) for argument in task.functionArgs]

            if task_flags.stream_output:
                with (
                    StreamingBuffer(task.taskId, TaskLog.LogType.stdout, self._connector_agent) as stdout_buf,
                    StreamingBuffer(task.taskId, TaskLog.LogType.stderr, self._connector_agent) as stderr_buf,
                    self.__processor_context(),
                    redirect_stdout(cast(IO[str], stdout_buf)),
                    redirect_stderr(cast(IO[str], stderr_buf)),
                ):
                    result = function(*args)
            else:
                with self.__processor_context():
                    result = function(*args)

            result_bytes = self._object_cache.serialize(task.source, result)
            task_result_type = TaskResultType.success

        except Exception as e:
            logger.exception(f"exception when processing task_id={task.taskId.hex()}:")
            task_result_type = TaskResultType.failed
            result_bytes = serialize_failure(e)

        self.__send_result(task.source, task.taskId, task_result_type, result_bytes)

    def __send_result(self, source: ClientID, task_id: TaskID, task_result_type: TaskResultType, result_bytes: bytes):
        self._current_task = None

        result_object_id = ObjectID.generate_object_id(source)

        self._connector_storage.set_object(result_object_id, result_bytes)
        self._connector_agent.send(
            ObjectInstruction(
                instructionType=ObjectInstruction.ObjectInstructionType.create,
                objectUser=source,
                objectMetadata=ObjectMetadata(
                    objectIds=(result_object_id,),
                    objectTypes=(ObjectMetadata.ObjectContentType.object,),
                    objectNames=(f"<res {repr(result_object_id)}>".encode(),),
                ),
            )
        )
        self._connector_agent.send(
            TaskResult(taskId=task_id, resultType=task_result_type, metadata=b"", results=[bytes(result_object_id)])
        )

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

    @staticmethod
    def __is_closed_zmq_socket_exception(exception: Exception) -> bool:
        """Validates whether exception represents a closed ZMQ socket error, lazily importing pyzmq."""

        if exception.__class__.__name__ != "ZMQError":
            return False

        import zmq

        assert isinstance(exception, zmq.error.ZMQError)

        return exception.errno == zmq.ENOTSOCK
