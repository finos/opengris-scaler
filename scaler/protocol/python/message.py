import dataclasses
import enum
import os
import pickle
import struct
from typing import List, Set, Tuple, TypeVar

import bidict

from scaler.protocol.python.mixins import _Message
from scaler.protocol.python.status import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    Resource,
    TaskManagerStatus,
    WorkerManagerStatus,
)


class MessageType(enum.Enum):
    Task = b"TK"
    TaskCancel = b"TC"
    TaskResult = b"TR"

    GraphTask = b"GT"
    GraphTaskCancel = b"GC"

    ObjectInstruction = b"OI"
    ObjectRequest = b"OR"
    ObjectResponse = b"OA"

    ClientHeartbeat = b"CB"
    ClientHeartbeatEcho = b"CE"

    WorkerHeartbeat = b"HB"
    WorkerHeartbeatEcho = b"HE"

    DisconnectRequest = b"DR"
    DisconnectResponse = b"DP"

    StateClient = b"SC"
    StateObject = b"SF"
    StateBalanceAdvice = b"SA"
    StateScheduler = b"SS"
    StateWorker = b"SW"
    StateTask = b"ST"
    StateGraphTask = b"SG"

    ClientDisconnect = b"CS"
    ClientShutdownResponse = b"CR"

    ProcessorInitialized = b"PI"

    @staticmethod
    def allowed_values():
        return {member.value for member in MessageType}


class TaskEchoStatus(enum.Enum):
    # task echo is the response of task submit to scheduler
    SubmitOK = b"O"  # if submit ok and task get accepted by scheduler
    SubmitDuplicated = b"D"  # if submit and find task in scheduler
    CancelOK = b"C"  # if cancel and success
    CancelTaskNotFound = b"N"  # if cancel and cannot find task in scheduler


class TaskStatus(enum.Enum):
    # task is accepted by scheduler, but will have below status
    Success = b"S"  # if submit and task is done and get result
    Failed = b"F"  # if submit and task is failed on worker
    Canceled = b"C"  # if submit and task is canceled
    NotFound = b"N"  # if submit and task is not found in scheduler
    WorkerDied = b"K"  # if submit and worker died (only happened when scheduler keep_task=False)
    NoWorker = b"W"  # if submit and scheduler is full (not implemented yet)

    # below are only used for monitoring channel, not sent to client
    Inactive = b"I"  # task is scheduled but not allocate to worker
    Running = b"R"  # task is running in worker
    Canceling = b"X"  # task is canceling (can be in Inactive or Running state)


class NodeTaskType(enum.Enum):
    Normal = b"N"
    Target = b"T"


class ObjectInstructionType(enum.Enum):
    Create = b"C"
    Delete = b"D"


class ObjectRequestType(enum.Enum):
    Get = b"A"


class ObjectResponseType(enum.Enum):
    Content = b"C"
    ObjectNotExist = b"N"


class ArgumentType(enum.Enum):
    Task = b"T"
    ObjectID = b"R"


class DisconnectType(enum.Enum):
    Disconnect = b"D"
    Shutdown = b"S"


@dataclasses.dataclass
class Argument:
    type: ArgumentType
    data: bytes

    def __repr__(self):
        return f"Argument(type={self.type}, data={self.data.hex()})"

    def serialize(self) -> Tuple[bytes, ...]:
        return self.type.value, self.data

    @staticmethod
    def deserialize(data: List[bytes]):
        return Argument(ArgumentType(data[0]), data[1])


@dataclasses.dataclass
class ObjectContent:
    object_ids: Tuple[bytes, ...]
    object_names: Tuple[bytes, ...] = dataclasses.field(default_factory=tuple)
    object_bytes: Tuple[bytes, ...] = dataclasses.field(default_factory=tuple)

    def serialize(self) -> Tuple[bytes, ...]:
        payload = (
            struct.pack("III", len(self.object_ids), len(self.object_names), len(self.object_bytes)),
            *self.object_ids,
            *self.object_names,
            *self.object_bytes,
        )
        return payload

    @staticmethod
    def deserialize(data: List[bytes]) -> "ObjectContent":
        num_of_object_ids, num_of_object_names, num_of_object_bytes = struct.unpack("III", data[0])

        data = data[1:]
        object_ids = data[:num_of_object_ids]

        data = data[num_of_object_ids:]
        object_names = data[:num_of_object_names]

        data = data[num_of_object_names:]
        object_bytes = data[:num_of_object_bytes]

        return ObjectContent(tuple(object_ids), tuple(object_names), tuple(object_bytes))


MessageVariant = TypeVar("MessageVariant", bound=_Message)


@dataclasses.dataclass
class Task(_Message):
    task_id: bytes
    source: bytes
    metadata: bytes
    func_object_id: bytes
    function_args: List[Argument]

    def __repr__(self):
        return (
            f"Task(task_id={self.task_id.hex()}, source={self.source.hex()}, metadata={self.metadata.hex()},"
            f"func_object_id={self.func_object_id.hex()}, function_args={self.function_args})"
        )

    def get_required_object_ids(self) -> Set[bytes]:
        return {self.func_object_id} | {arg.data for arg in self.function_args if arg.type == ArgumentType.ObjectID}

    def serialize(self) -> Tuple[bytes, ...]:
        return (
            self.task_id,
            self.source,
            self.metadata,
            self.func_object_id,
            *[d for arg in self.function_args for d in arg.serialize()],
        )

    @staticmethod
    def deserialize(data: List[bytes]):
        return Task(
            data[0], data[1], data[2], data[3], [Argument.deserialize(data[i : i + 2]) for i in range(4, len(data), 2)]
        )


@dataclasses.dataclass
class TaskCancelFlags:
    force: bool = dataclasses.field(default=False)
    retrieve_task_object: bool = dataclasses.field(default=False)

    FORMAT = "??"

    def serialize(self) -> bytes:
        return struct.pack(TaskCancelFlags.FORMAT, self.force, self.retrieve_task_object)

    @staticmethod
    def deserialize(data: bytes) -> "TaskCancelFlags":
        return TaskCancelFlags(*struct.unpack(TaskCancelFlags.FORMAT, data))


@dataclasses.dataclass
class TaskCancel(_Message):
    task_id: bytes
    flags: TaskCancelFlags = dataclasses.field(default_factory=TaskCancelFlags)

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.task_id, self.flags.serialize())

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskCancel(data[0], TaskCancelFlags.deserialize(data[1]))  # type: ignore


@dataclasses.dataclass
class TaskResult(_Message):
    task_id: bytes
    status: TaskStatus
    metadata: bytes = dataclasses.field(default=b"")
    results: List[bytes] = dataclasses.field(default_factory=list)

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value, self.metadata, *self.results

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskResult(data[0], TaskStatus(data[1]), data[2], data[3:])


@dataclasses.dataclass
class GraphTask(_Message):
    task_id: bytes
    source: bytes
    targets: List[bytes]
    graph: List[Task]

    def __repr__(self):
        return (
            f"GraphTask({os.linesep}"
            f"    task_id={self.task_id.hex()},{os.linesep}"
            f"    targets=[{os.linesep}"
            f"        {[target.hex() + ',' + os.linesep for target in self.targets]}"
            f"    ]\n"
            f"    graph={self.graph}\n"
            f")"
        )

    def serialize(self) -> Tuple[bytes, ...]:
        graph_bytes = []
        for task in self.graph:
            frames = task.serialize()
            graph_bytes.append(struct.pack("I", len(frames)))
            graph_bytes.extend(frames)

        return self.task_id, self.source, struct.pack("I", len(self.targets)), *self.targets, *graph_bytes

    @staticmethod
    def deserialize(data: List[bytes]):
        index = 0
        task_id = data[index]

        index += 1
        client_id = data[index]

        index += 1
        number_of_targets = struct.unpack("I", data[index])[0]

        index += 1
        targets = data[index : index + number_of_targets]

        index += number_of_targets
        graph = []
        while index < len(data):
            number_of_frames = struct.unpack("I", data[index])[0]
            index += 1
            graph.append(Task.deserialize(data[index : index + number_of_frames]))
            index += number_of_frames

        return GraphTask(task_id, client_id, targets, graph)


@dataclasses.dataclass
class GraphTaskCancel(_Message):
    task_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.task_id,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return GraphTaskCancel(data[0])


@dataclasses.dataclass
class ClientHeartbeat(_Message):
    client_cpu: float
    client_rss: int
    latency_us: int

    FORMAT = "HQI"

    def serialize(self) -> Tuple[bytes, ...]:
        return (struct.pack(ClientHeartbeat.FORMAT, int(self.client_cpu * 1000), self.client_rss, self.latency_us),)

    @staticmethod
    def deserialize(data: List[bytes]):
        client_cpu, client_rss, latency_us = struct.unpack(ClientHeartbeat.FORMAT, data[0])
        return ClientHeartbeat(float(client_cpu / 1000), client_rss, latency_us)


@dataclasses.dataclass
class ClientHeartbeatEcho(_Message):
    def serialize(self) -> Tuple[bytes, ...]:
        return (b"",)

    @staticmethod
    def deserialize(data: List[bytes]):
        return ClientHeartbeatEcho()


@dataclasses.dataclass
class ProcessorHeartbeat:
    pid: int
    initialized: bool
    has_task: bool
    suspended: bool
    cpu: float
    rss: int

    FORMAT = "I???HQ"

    def serialize(self) -> bytes:
        return struct.pack(
            ProcessorHeartbeat.FORMAT,
            self.pid,
            self.initialized,
            self.has_task,
            self.suspended,
            int(self.cpu * 1000),
            self.rss,
        )

    @staticmethod
    def deserialize(data: bytes) -> "ProcessorHeartbeat":
        pid, initialized, has_task, suspended, cpu, rss = struct.unpack(ProcessorHeartbeat.FORMAT, data)
        return ProcessorHeartbeat(pid, initialized, has_task, suspended, float(cpu / 1000), rss)


@dataclasses.dataclass
class WorkerHeartbeat(_Message):
    agent_cpu: float
    agent_rss: int
    rss_free: int
    queued_tasks: int
    latency_us: int
    task_lock: bool

    processors: List[ProcessorHeartbeat]

    FORMAT = "HQQHI?"  # processor heartbeats come right after the main fields

    def serialize(self) -> Tuple[bytes, ...]:
        return (
            struct.pack(
                WorkerHeartbeat.FORMAT,
                int(self.agent_cpu * 1000),
                self.agent_rss,
                self.rss_free,
                self.queued_tasks,
                self.latency_us,
                self.task_lock,
            ),
            *(p.serialize() for p in self.processors)
        )

    @staticmethod
    def deserialize(data: List[bytes]):
        (
            agent_cpu,
            agent_rss,
            rss_free,
            queued_tasks,
            latency_us,
            task_lock,
        ) = struct.unpack(WorkerHeartbeat.FORMAT, data[0])
        processors = [ProcessorHeartbeat.deserialize(d) for d in data[1:]]

        return WorkerHeartbeat(
            float(agent_cpu / 1000),
            agent_rss,
            rss_free,
            queued_tasks,
            latency_us,
            task_lock,
            processors,
        )


@dataclasses.dataclass
class WorkerHeartbeatEcho(_Message):
    def serialize(self) -> Tuple[bytes, ...]:
        return (b"",)

    @staticmethod
    def deserialize(data: List[bytes]):
        return WorkerHeartbeatEcho()


@dataclasses.dataclass
class ObjectInstruction(_Message):
    type: ObjectInstructionType
    object_user: bytes
    object_content: ObjectContent

    def serialize(self) -> Tuple[bytes, ...]:
        return self.type.value, self.object_user, *self.object_content.serialize()

    @staticmethod
    def deserialize(data: List[bytes]) -> "ObjectInstruction":
        return ObjectInstruction(ObjectInstructionType(data[0]), data[1], ObjectContent.deserialize(data[2:]))


@dataclasses.dataclass
class ObjectRequest(_Message):
    type: ObjectRequestType
    object_ids: Tuple[bytes, ...]

    def __repr__(self):
        return f"ObjectRequest(type={self.type}, object_ids={tuple(object_id.hex() for object_id in self.object_ids)})"

    def serialize(self) -> Tuple[bytes, ...]:
        return self.type.value, *self.object_ids

    @staticmethod
    def deserialize(data: List[bytes]):
        return ObjectRequest(ObjectRequestType(data[0]), tuple(data[1:]))


@dataclasses.dataclass
class ObjectResponse(_Message):
    type: ObjectResponseType
    object_content: ObjectContent

    def serialize(self) -> Tuple[bytes, ...]:
        return self.type.value, *self.object_content.serialize()

    @staticmethod
    def deserialize(data: List[bytes]):
        request_type = ObjectResponseType(data[0])
        return ObjectResponse(request_type, ObjectContent.deserialize(data[1:]))


@dataclasses.dataclass
class DisconnectRequest(_Message):
    worker: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.worker,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return DisconnectRequest(data[0])


@dataclasses.dataclass
class DisconnectResponse(_Message):
    worker: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.worker,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return DisconnectResponse(data[0])


@dataclasses.dataclass
class ClientDisconnect(_Message):
    type: DisconnectType

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.type.value,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return ClientDisconnect(DisconnectType(data[0]))


@dataclasses.dataclass
class ClientShutdownResponse(_Message):
    accepted: bool

    def serialize(self) -> Tuple[bytes, ...]:
        return (struct.pack("?", self.accepted),)

    @staticmethod
    def deserialize(data: List[bytes]):
        return ClientShutdownResponse(struct.unpack("?", data[0])[0])


@dataclasses.dataclass
class StateClient(_Message):
    # TODO: implement this
    def serialize(self) -> Tuple[bytes, ...]:
        return (b"",)

    @staticmethod
    def deserialize(data: List[bytes]):
        return StateClient()


@dataclasses.dataclass
class StateObject(_Message):
    # TODO: implement this
    def serialize(self) -> Tuple[bytes, ...]:
        return (b"",)

    @staticmethod
    def deserialize(data: List[bytes]):
        return StateObject()


@dataclasses.dataclass
class StateBalanceAdvice(_Message):
    worker_id: bytes
    task_ids: List[bytes]

    def serialize(self) -> Tuple[bytes, ...]:
        return self.worker_id, *self.task_ids

    @staticmethod
    def deserialize(data: List[bytes]):
        return StateBalanceAdvice(data[0], data[1:])


@dataclasses.dataclass
class StateScheduler(_Message):
    binder: BinderStatus
    scheduler: Resource
    client_manager: ClientManagerStatus
    object_manager: ObjectManagerStatus
    task_manager: TaskManagerStatus
    worker_manager: WorkerManagerStatus

    def serialize(self) -> Tuple[bytes, ...]:
        return (
            pickle.dumps(
                (
                    self.binder,
                    self.scheduler,
                    self.client_manager,
                    self.object_manager,
                    self.task_manager,
                    self.worker_manager,
                )
            ),
        )

    @staticmethod
    def deserialize(data: List[bytes]):
        return StateScheduler(*pickle.loads(data[0]))


@dataclasses.dataclass
class StateWorker(_Message):
    worker_id: bytes
    message: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.worker_id, self.message

    @staticmethod
    def deserialize(data: List[bytes]):
        return StateWorker(data[0], data[1])


@dataclasses.dataclass
class StateTask(_Message):
    task_id: bytes
    function_name: bytes
    status: TaskStatus
    worker: bytes
    metadata: bytes = dataclasses.field(default=b"")

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.function_name, self.status.value, self.worker, self.metadata

    @staticmethod
    def deserialize(data: List[bytes]):
        return StateTask(data[0], data[1], TaskStatus(data[2]), data[3], data[4])


@dataclasses.dataclass
class StateGraphTask(_Message):
    graph_task_id: bytes
    task_id: bytes
    node_task_type: NodeTaskType
    parent_task_ids: Set[bytes]

    def serialize(self) -> Tuple[bytes, ...]:
        return self.graph_task_id, self.task_id, self.node_task_type.value, *self.parent_task_ids

    @staticmethod
    def deserialize(data: List[bytes]):
        return StateGraphTask(data[0], data[1], NodeTaskType(data[2]), set(data[3:]))


@dataclasses.dataclass
class ProcessorInitialized(_Message):
    def serialize(self) -> Tuple[bytes, ...]:
        return (b"",)

    @staticmethod
    def deserialize(data: List[bytes]):
        return ProcessorInitialized()


PROTOCOL = bidict.bidict(
    {
        MessageType.ClientHeartbeat: ClientHeartbeat,
        MessageType.ClientHeartbeatEcho: ClientHeartbeatEcho,
        MessageType.WorkerHeartbeat: WorkerHeartbeat,
        MessageType.WorkerHeartbeatEcho: WorkerHeartbeatEcho,
        MessageType.Task: Task,
        MessageType.TaskCancel: TaskCancel,
        MessageType.TaskResult: TaskResult,
        MessageType.GraphTask: GraphTask,
        MessageType.GraphTaskCancel: GraphTaskCancel,
        MessageType.ObjectInstruction: ObjectInstruction,
        MessageType.ObjectRequest: ObjectRequest,
        MessageType.ObjectResponse: ObjectResponse,
        MessageType.DisconnectRequest: DisconnectRequest,
        MessageType.DisconnectResponse: DisconnectResponse,
        MessageType.StateClient: StateClient,
        MessageType.StateObject: StateObject,
        MessageType.StateBalanceAdvice: StateBalanceAdvice,
        MessageType.StateScheduler: StateScheduler,
        MessageType.StateWorker: StateWorker,
        MessageType.StateTask: StateTask,
        MessageType.StateGraphTask: StateGraphTask,
        MessageType.ClientDisconnect: ClientDisconnect,
        MessageType.ClientShutdownResponse: ClientShutdownResponse,
        MessageType.ProcessorInitialized: ProcessorInitialized,
    }
)
