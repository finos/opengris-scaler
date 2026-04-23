from typing import Any, ClassVar

from scaler.utility.identifiers import ClientID
from scaler.utility.identifiers import ObjectID as ScalerObjectID
from scaler.utility.identifiers import TaskID, WorkerID

class EnumFieldValue:
    raw: int
    def _as_str(self) -> str: ...

class CapnpStruct:
    def __init__(self, **kwargs: Any) -> None: ...
    def to_bytes(self) -> bytes: ...
    def get_message(self) -> "CapnpStruct": ...
    @classmethod
    def from_bytes(cls, data: bytes, traversal_limit_in_words: int = ...) -> Any: ...

class BaseMessage(CapnpStruct): ...

class CapnpUnionStruct(CapnpStruct):
    def which(self) -> str: ...

class TaskResultType(EnumFieldValue):
    def __new__(cls, value: int) -> "TaskResultType": ...
    success: ClassVar["TaskResultType"]
    failed: ClassVar["TaskResultType"]
    failedWorkerDied: ClassVar["TaskResultType"]

class TaskCancelConfirmType(EnumFieldValue):
    def __new__(cls, value: int) -> "TaskCancelConfirmType": ...
    canceled: ClassVar["TaskCancelConfirmType"]
    cancelFailed: ClassVar["TaskCancelConfirmType"]
    cancelNotFound: ClassVar["TaskCancelConfirmType"]

class TaskTransition(EnumFieldValue):
    def __new__(cls, value: int) -> "TaskTransition": ...
    hasCapacity: ClassVar["TaskTransition"]
    taskResultSuccess: ClassVar["TaskTransition"]
    taskResultFailed: ClassVar["TaskTransition"]
    taskResultWorkerDied: ClassVar["TaskTransition"]
    taskCancel: ClassVar["TaskTransition"]
    taskCancelConfirmCanceled: ClassVar["TaskTransition"]
    taskCancelConfirmFailed: ClassVar["TaskTransition"]
    taskCancelConfirmNotFound: ClassVar["TaskTransition"]
    balanceTaskCancel: ClassVar["TaskTransition"]
    workerDisconnect: ClassVar["TaskTransition"]
    schedulerHasTask: ClassVar["TaskTransition"]
    schedulerHasNoTask: ClassVar["TaskTransition"]

class TaskState(EnumFieldValue):
    def __new__(cls, value: int) -> "TaskState": ...
    inactive: ClassVar["TaskState"]
    running: ClassVar["TaskState"]
    canceling: ClassVar["TaskState"]
    balanceCanceling: ClassVar["TaskState"]
    success: ClassVar["TaskState"]
    failed: ClassVar["TaskState"]
    failedWorkerDied: ClassVar["TaskState"]
    canceled: ClassVar["TaskState"]
    canceledNotFound: ClassVar["TaskState"]
    balanceCanceled: ClassVar["TaskState"]
    workerDisconnecting: ClassVar["TaskState"]

class WorkerState(EnumFieldValue):
    def __new__(cls, value: int) -> "WorkerState": ...
    connected: ClassVar["WorkerState"]
    disconnected: ClassVar["WorkerState"]

class TaskCapability(CapnpStruct):
    name: str
    value: int
    @staticmethod
    def new_msg(name: str, value: int) -> "TaskCapability": ...

class ObjectMetadata(CapnpStruct):
    class ObjectContentType(EnumFieldValue):
        def __new__(cls, value: int) -> "ObjectMetadata.ObjectContentType": ...
        serializer: ClassVar["ObjectMetadata.ObjectContentType"]
        object: ClassVar["ObjectMetadata.ObjectContentType"]

    objectIds: Any
    objectTypes: Any
    objectNames: Any

    @staticmethod
    def new_msg(object_ids: Any, object_types: Any = ..., object_names: Any = ...) -> "ObjectMetadata": ...

class ObjectStorageAddress(CapnpStruct):
    host: str
    port: int
    @staticmethod
    def new_msg(host: str, port: int) -> "ObjectStorageAddress": ...

class Resource(CapnpStruct):
    cpu: int
    rss: int

class ObjectManagerStatus(CapnpStruct):
    numberOfObjects: int

class ClientManagerStatus(CapnpStruct):
    class Pair(CapnpStruct):
        client: ClientID
        numTask: int

    clientToNumOfTask: Any

class TaskManagerStatus(CapnpStruct):
    class Pair(CapnpStruct):
        state: int
        count: int

    stateToCount: Any

class ProcessorStatus(CapnpStruct):
    pid: int
    initialized: bool
    hasTask: bool
    suspended: bool
    resource: Resource

class WorkerStatus(CapnpStruct):
    workerId: WorkerID
    agent: Resource
    rssFree: int
    free: int
    sent: int
    queued: int
    suspended: int
    lagUS: int
    lastS: int
    itl: str
    processorStatuses: Any

class WorkerManagerStatus(CapnpStruct):
    workers: Any

class ScalingManagerStatus(CapnpStruct):
    class Pair(CapnpStruct):
        workerManagerID: bytes
        workerIDs: Any

    class WorkerManagerDetail(CapnpStruct):
        workerManagerID: bytes
        identity: str
        lastSeenS: int
        maxTaskConcurrency: int
        capabilities: str
        pendingWorkers: int

    managedWorkers: Any
    workerManagerDetails: Any

class BinderStatus(CapnpStruct):
    class Pair(CapnpStruct):
        client: str
        number: int

    received: Any
    sent: Any

class Task(BaseMessage):
    taskId: TaskID
    source: ClientID
    metadata: bytes
    funcObjectId: ScalerObjectID
    functionArgs: Any
    capabilities: Any

    class Argument(CapnpStruct):
        type: Any
        data: bytes

        class ArgumentType(EnumFieldValue):
            def __new__(cls, value: int) -> "Task.Argument.ArgumentType": ...
            task: ClassVar["Task.Argument.ArgumentType"]
            objectID: ClassVar["Task.Argument.ArgumentType"]

    @staticmethod
    def new_msg(*args: Any, **kwargs: Any) -> "Task": ...

class TaskCancel(BaseMessage):
    taskId: TaskID
    flags: Any

    class TaskCancelFlags(CapnpStruct):
        force: bool

    @staticmethod
    def new_msg(*args: Any, **kwargs: Any) -> "TaskCancel": ...

class TaskLog(BaseMessage):
    taskId: TaskID
    logType: Any
    content: str

    class LogType(EnumFieldValue):
        def __new__(cls, value: int) -> "TaskLog.LogType": ...
        stdout: ClassVar["TaskLog.LogType"]
        stderr: ClassVar["TaskLog.LogType"]

    @staticmethod
    def new_msg(*args: Any, **kwargs: Any) -> "TaskLog": ...

class TaskResult(BaseMessage):
    taskId: TaskID
    resultType: TaskResultType
    metadata: bytes
    results: Any

class TaskCancelConfirm(BaseMessage):
    taskId: TaskID
    cancelConfirmType: TaskCancelConfirmType

class GraphTask(BaseMessage):
    taskId: TaskID
    source: ClientID
    targets: Any
    graph: Any

class ClientHeartbeat(BaseMessage):
    resource: Resource
    latencyUS: int

class ClientHeartbeatEcho(BaseMessage):
    objectStorageAddress: ObjectStorageAddress

class WorkerHeartbeat(BaseMessage):
    agent: Resource
    rssFree: int
    queueSize: int
    queuedTasks: int
    latencyUS: int
    taskLock: bool
    processors: Any
    capabilities: Any
    workerManagerID: bytes

class WorkerHeartbeatEcho(BaseMessage):
    objectStorageAddress: ObjectStorageAddress

class WorkerManagerHeartbeat(BaseMessage):
    maxTaskConcurrency: int
    capabilities: Any
    workerManagerID: bytes

class WorkerManagerHeartbeatEcho(BaseMessage): ...

class WorkerManagerCommandType(EnumFieldValue):
    def __new__(cls, value: int) -> "WorkerManagerCommandType": ...
    startWorkers: ClassVar["WorkerManagerCommandType"]
    shutdownWorkers: ClassVar["WorkerManagerCommandType"]
    setDesiredTaskConcurrency: ClassVar["WorkerManagerCommandType"]

class WorkerManagerCommand(BaseMessage):
    workerIDs: Any
    command: WorkerManagerCommandType
    capabilities: Any
    setDesiredTaskConcurrencyRequests: Any

    class DesiredTaskConcurrencyRequest(CapnpStruct):
        taskConcurrency: int
        capabilities: Any

class WorkerManagerCommandResponse(BaseMessage):
    command: WorkerManagerCommandType
    status: "WorkerManagerCommandResponse.Status"
    workerIDs: Any
    capabilities: Any

    class Status(EnumFieldValue):
        def __new__(cls, value: int) -> "WorkerManagerCommandResponse.Status": ...
        tooManyWorkers: ClassVar["WorkerManagerCommandResponse.Status"]
        unknownAction: ClassVar["WorkerManagerCommandResponse.Status"]
        workerNotFound: ClassVar["WorkerManagerCommandResponse.Status"]
        success: ClassVar["WorkerManagerCommandResponse.Status"]

class ObjectInstruction(BaseMessage):
    instructionType: "ObjectInstruction.ObjectInstructionType"
    objectUser: ClientID
    objectMetadata: ObjectMetadata

    class ObjectInstructionType(EnumFieldValue):
        def __new__(cls, value: int) -> "ObjectInstruction.ObjectInstructionType": ...
        create: ClassVar["ObjectInstruction.ObjectInstructionType"]
        delete: ClassVar["ObjectInstruction.ObjectInstructionType"]
        clear: ClassVar["ObjectInstruction.ObjectInstructionType"]

class DisconnectRequest(BaseMessage):
    worker: WorkerID

class DisconnectResponse(BaseMessage):
    worker: WorkerID

class ClientDisconnect(BaseMessage):
    disconnectType: "ClientDisconnect.DisconnectType"

    class DisconnectType(EnumFieldValue):
        def __new__(cls, value: int) -> "ClientDisconnect.DisconnectType": ...
        disconnect: ClassVar["ClientDisconnect.DisconnectType"]
        shutdown: ClassVar["ClientDisconnect.DisconnectType"]

class ClientShutdownResponse(BaseMessage):
    accepted: bool

class StateClient(BaseMessage): ...
class StateObject(BaseMessage): ...

class StateBalanceAdvice(BaseMessage):
    workerId: WorkerID
    taskIds: Any

class StateScheduler(BaseMessage):
    binder: BinderStatus
    scheduler: Resource
    rssFree: int
    clientManager: ClientManagerStatus
    objectManager: ObjectManagerStatus
    taskManager: TaskManagerStatus
    workerManager: WorkerManagerStatus
    scalingManager: ScalingManagerStatus

class StateWorker(BaseMessage):
    workerId: WorkerID
    state: WorkerState
    capabilities: Any

class StateTask(BaseMessage):
    taskId: TaskID
    functionName: bytes
    state: TaskState
    worker: WorkerID
    capabilities: Any
    metadata: bytes

class StateGraphTask(BaseMessage):
    graphTaskId: TaskID
    taskId: TaskID
    nodeTaskType: "StateGraphTask.NodeTaskType"
    parentTaskIds: Any

    class NodeTaskType(EnumFieldValue):
        def __new__(cls, value: int) -> "StateGraphTask.NodeTaskType": ...
        normal: ClassVar["StateGraphTask.NodeTaskType"]
        target: ClassVar["StateGraphTask.NodeTaskType"]

class ProcessorInitialized(BaseMessage): ...

class InformationRequest(BaseMessage):
    request: bytes

class InformationResponse(BaseMessage):
    response: bytes

class Message(CapnpUnionStruct):
    task: Task
    taskCancel: TaskCancel
    taskCancelConfirm: TaskCancelConfirm
    taskResult: TaskResult
    taskLog: TaskLog
    graphTask: GraphTask
    objectInstruction: ObjectInstruction
    clientHeartbeat: ClientHeartbeat
    clientHeartbeatEcho: ClientHeartbeatEcho
    workerHeartbeat: WorkerHeartbeat
    workerHeartbeatEcho: WorkerHeartbeatEcho
    disconnectRequest: DisconnectRequest
    disconnectResponse: DisconnectResponse
    stateClient: StateClient
    stateObject: StateObject
    stateBalanceAdvice: StateBalanceAdvice
    stateScheduler: StateScheduler
    stateWorker: StateWorker
    stateTask: StateTask
    stateGraphTask: StateGraphTask
    clientDisconnect: ClientDisconnect
    clientShutdownResponse: ClientShutdownResponse
    processorInitialized: ProcessorInitialized
    informationRequest: InformationRequest
    informationResponse: InformationResponse
    workerManagerHeartbeat: WorkerManagerHeartbeat
    workerManagerHeartbeatEcho: WorkerManagerHeartbeatEcho
    workerManagerCommand: WorkerManagerCommand
    workerManagerCommandResponse: WorkerManagerCommandResponse

class ObjectRequestHeader(CapnpStruct):
    MESSAGE_LENGTH: ClassVar[int]
    objectID: Any
    payloadLength: int
    requestID: int
    requestType: "ObjectRequestHeader.ObjectRequestType"

    class ObjectRequestType(EnumFieldValue):
        def __new__(cls, value: int) -> "ObjectRequestHeader.ObjectRequestType": ...
        setObject: ClassVar["ObjectRequestHeader.ObjectRequestType"]
        getObject: ClassVar["ObjectRequestHeader.ObjectRequestType"]
        deleteObject: ClassVar["ObjectRequestHeader.ObjectRequestType"]
        duplicateObjectID: ClassVar["ObjectRequestHeader.ObjectRequestType"]
        infoGetTotal: ClassVar["ObjectRequestHeader.ObjectRequestType"]

class ObjectID(CapnpStruct):
    field0: int
    field1: int
    field2: int
    field3: int

class ObjectResponseHeader(CapnpStruct):
    MESSAGE_LENGTH: ClassVar[int]
    objectID: Any
    payloadLength: int
    responseID: int
    responseType: "ObjectResponseHeader.ObjectResponseType"

    class ObjectResponseType(EnumFieldValue):
        def __new__(cls, value: int) -> "ObjectResponseHeader.ObjectResponseType": ...
        setOK: ClassVar["ObjectResponseHeader.ObjectResponseType"]
        getOK: ClassVar["ObjectResponseHeader.ObjectResponseType"]
        delOK: ClassVar["ObjectResponseHeader.ObjectResponseType"]
        delNotExists: ClassVar["ObjectResponseHeader.ObjectResponseType"]
        duplicateOK: ClassVar["ObjectResponseHeader.ObjectResponseType"]
        infoGetTotalOK: ClassVar["ObjectResponseHeader.ObjectResponseType"]

def get_module_descriptor(module_name: str) -> Any: ...
def message_to_bytes(variant_name: str, inner: Any) -> bytes: ...
def message_from_bytes(data: bytes, traversal_limit: int = ...) -> Any: ...
def struct_to_bytes(type_name: str, obj: Any) -> bytes: ...
def struct_from_bytes(type_name: str, data: bytes, traversal_limit: int = ...) -> Any: ...

PROTOCOL: Any
