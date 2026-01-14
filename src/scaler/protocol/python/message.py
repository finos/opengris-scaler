import dataclasses
import enum
from typing import Dict, List, Optional, Set, Type, Union

import bidict

from scaler.protocol.capnp._python import _message  # noqa
from scaler.protocol.python.common import (
    ObjectMetadata,
    ObjectStorageAddress,
    TaskCancelConfirmType,
    TaskCapability,
    TaskResultType,
    TaskState,
    WorkerState,
)
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    ProcessorStatus,
    Resource,
    ScalingManagerStatus,
    TaskManagerStatus,
    WorkerManagerStatus,
)
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID


class Task(Message):
    def __init__(self, msg):
        super().__init__(msg)

    def __repr__(self):
        return (
            f"Task(task_id={self.task_id!r}, source={self.source!r}, metadata={self.metadata.hex()},"
            f"func_object_id={self.func_object_id!r}, function_args={self.function_args})"
        )

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def source(self) -> ClientID:
        return ClientID(self._msg.source)

    @property
    def metadata(self) -> bytes:
        return bytes(self._msg.metadata)

    @property
    def func_object_id(self) -> Optional[ObjectID]:
        if len(self._msg.funcObjectId) > 0:
            return ObjectID(self._msg.funcObjectId)
        else:
            return None

    @property
    def function_args(self) -> List[Union[ObjectID, TaskID]]:
        return [self._from_capnp_task_argument(arg) for arg in self._msg.functionArgs]

    @property
    def capabilities(self) -> Dict[str, int]:
        return {capability.name: capability.value for capability in self._msg.capabilities}

    @staticmethod
    def new_msg(
        task_id: TaskID,
        source: ClientID,
        metadata: bytes,
        func_object_id: Optional[ObjectID],
        function_args: List[Union[ObjectID, TaskID]],
        capabilities: Dict[str, int],
    ) -> "Task":
        return Task(
            _message.Task(
                taskId=bytes(task_id),
                source=bytes(source),
                metadata=metadata,
                funcObjectId=bytes(func_object_id) if func_object_id is not None else b"",
                functionArgs=[Task._to_capnp_task_argument(arg) for arg in function_args],
                capabilities=[
                    TaskCapability.new_msg(name, value).get_message() for name, value in capabilities.items()
                ],
            )
        )

    @staticmethod
    def _from_capnp_task_argument(value: _message.Task.Argument) -> Union[ObjectID, TaskID]:
        if value.type.raw == _message.Task.Argument.ArgumentType.task:
            return TaskID(value.data)
        else:
            assert value.type.raw == _message.Task.Argument.ArgumentType.objectID
            return ObjectID(value.data)

    @staticmethod
    def _to_capnp_task_argument(value: Union[ObjectID, TaskID]) -> _message.Task.Argument:
        if isinstance(value, TaskID):
            return _message.Task.Argument(type=_message.Task.Argument.ArgumentType.task, data=bytes(value))
        else:
            assert isinstance(value, ObjectID)
            return _message.Task.Argument(type=_message.Task.Argument.ArgumentType.objectID, data=bytes(value))


class TaskCancel(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @dataclasses.dataclass
    class TaskCancelFlags:
        force: bool

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def flags(self) -> TaskCancelFlags:
        return TaskCancel.TaskCancelFlags(force=self._msg.flags.force)

    @staticmethod
    def new_msg(task_id: TaskID, flags: Optional[TaskCancelFlags] = None) -> "TaskCancel":
        if flags is None:
            flags = TaskCancel.TaskCancelFlags(force=False)

        return TaskCancel(
            _message.TaskCancel(taskId=bytes(task_id), flags=_message.TaskCancel.TaskCancelFlags(force=flags.force))
        )


class TaskLog(Message):
    class LogType(enum.Enum):
        Stdout = _message.TaskLog.LogType.stdout
        Stderr = _message.TaskLog.LogType.stderr

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def log_type(self) -> "TaskLog.LogType":
        return TaskLog.LogType(self._msg.logType.raw)

    @property
    def content(self) -> str:
        return self._msg.content

    @staticmethod
    def new_msg(task_id: TaskID, log_type: "TaskLog.LogType", content: str) -> "TaskLog":
        return TaskLog(_message.TaskLog(taskId=bytes(task_id), logType=log_type.value, content=content))


class TaskResult(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def result_type(self) -> TaskResultType:
        return TaskResultType(self._msg.resultType.raw)

    @property
    def metadata(self) -> bytes:
        return bytes(self._msg.metadata)

    @property
    def results(self) -> List[bytes]:
        return [bytes(r) for r in self._msg.results]

    @staticmethod
    def new_msg(
        task_id: TaskID,
        result_type: TaskResultType,
        metadata: Optional[bytes] = None,
        results: Optional[List[bytes]] = None,
    ) -> "TaskResult":
        if metadata is None:
            metadata = bytes()

        if results is None:
            results = list()

        return TaskResult(
            _message.TaskResult(taskId=bytes(task_id), resultType=result_type.value, metadata=metadata, results=results)
        )


class TaskCancelConfirm(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def cancel_confirm_type(self) -> TaskCancelConfirmType:
        return TaskCancelConfirmType(self._msg.cancelConfirmType.raw)

    @staticmethod
    def new_msg(task_id: TaskID, cancel_confirm_type: TaskCancelConfirmType) -> "TaskCancelConfirm":
        return TaskCancelConfirm(
            _message.TaskCancelConfirm(taskId=bytes(task_id), cancelConfirmType=cancel_confirm_type.value)
        )


class GraphTask(Message):
    def __init__(self, msg):
        super().__init__(msg)

    def __repr__(self):
        return (
            f"GraphTask(\n"
            f"    task_id={self.task_id!r},\n"
            f"    targets=[\n"
            f"        {', '.join(repr(target) for target in self.targets)}\n"
            f"    ]\n"
            f"    graph={self.graph!r}\n"
            f")"
        )

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def source(self) -> ClientID:
        return ClientID(self._msg.source)

    @property
    def targets(self) -> List[TaskID]:
        return [TaskID(target) for target in self._msg.targets]

    @property
    def graph(self) -> List[Task]:
        return [Task(task) for task in self._msg.graph]

    @staticmethod
    def new_msg(task_id: TaskID, source: ClientID, targets: List[TaskID], graph: List[Task]) -> "GraphTask":
        return GraphTask(
            _message.GraphTask(
                taskId=bytes(task_id),
                source=bytes(source),
                targets=[bytes(target) for target in targets],
                graph=[task.get_message() for task in graph],
            )
        )


class ClientHeartbeat(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def resource(self) -> Resource:
        return Resource(self._msg.resource)

    @property
    def latency_us(self) -> int:
        return self._msg.latencyUS

    @staticmethod
    def new_msg(resource: Resource, latency_us: int) -> "ClientHeartbeat":
        return ClientHeartbeat(_message.ClientHeartbeat(resource=resource.get_message(), latencyUS=latency_us))


class ClientHeartbeatEcho(Message):
    def __init__(self, msg):
        super().__init__(msg)

    def object_storage_address(self) -> ObjectStorageAddress:
        return ObjectStorageAddress(self._msg.objectStorageAddress)

    @staticmethod
    def new_msg(object_storage_address: ObjectStorageAddress) -> "ClientHeartbeatEcho":
        return ClientHeartbeatEcho(
            _message.ClientHeartbeatEcho(objectStorageAddress=object_storage_address.get_message())
        )


class WorkerHeartbeat(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def agent(self) -> Resource:
        return Resource(self._msg.agent)

    @property
    def rss_free(self) -> int:
        return self._msg.rssFree

    @property
    def queue_size(self) -> int:
        return self._msg.queueSize

    @property
    def queued_tasks(self) -> int:
        return self._msg.queuedTasks

    @property
    def latency_us(self) -> int:
        return self._msg.latencyUS

    @property
    def task_lock(self) -> bool:
        return self._msg.taskLock

    @property
    def processors(self) -> List[ProcessorStatus]:
        return [ProcessorStatus(p) for p in self._msg.processors]

    @property
    def capabilities(self) -> Dict[str, int]:
        return {capability.name: capability.value for capability in self._msg.capabilities}

    @staticmethod
    def new_msg(
        agent: Resource,
        rss_free: int,
        queue_size: int,
        queued_tasks: int,
        latency_us: int,
        task_lock: bool,
        processors: List[ProcessorStatus],
        capabilities: Dict[str, int],
    ) -> "WorkerHeartbeat":
        return WorkerHeartbeat(
            _message.WorkerHeartbeat(
                agent=agent.get_message(),
                rssFree=rss_free,
                queueSize=queue_size,
                queuedTasks=queued_tasks,
                latencyUS=latency_us,
                taskLock=task_lock,
                processors=[p.get_message() for p in processors],
                capabilities=[
                    TaskCapability.new_msg(name, value).get_message() for name, value in capabilities.items()
                ],
            )
        )


class WorkerHeartbeatEcho(Message):
    def __init__(self, msg):
        super().__init__(msg)

    def object_storage_address(self) -> ObjectStorageAddress:
        return ObjectStorageAddress(self._msg.objectStorageAddress)

    @staticmethod
    def new_msg(object_storage_address: ObjectStorageAddress) -> "WorkerHeartbeatEcho":
        return WorkerHeartbeatEcho(
            _message.WorkerHeartbeatEcho(objectStorageAddress=object_storage_address.get_message())
        )


class ObjectInstruction(Message):
    class ObjectInstructionType(enum.Enum):
        Create = _message.ObjectInstruction.ObjectInstructionType.create
        Delete = _message.ObjectInstruction.ObjectInstructionType.delete
        Clear = _message.ObjectInstruction.ObjectInstructionType.clear

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def instruction_type(self) -> ObjectInstructionType:
        return ObjectInstruction.ObjectInstructionType(self._msg.instructionType.raw)

    @property
    def object_user(self) -> Optional[ClientID]:
        return ClientID(self._msg.objectUser) if len(self._msg.objectUser) > 0 else None

    @property
    def object_metadata(self) -> ObjectMetadata:
        return ObjectMetadata(self._msg.objectMetadata)

    @staticmethod
    def new_msg(
        instruction_type: ObjectInstructionType, object_user: Optional[ClientID], object_metadata: ObjectMetadata
    ) -> "ObjectInstruction":
        return ObjectInstruction(
            _message.ObjectInstruction(
                instructionType=instruction_type.value,
                objectUser=bytes(object_user) if object_user is not None else b"",
                objectMetadata=object_metadata.get_message(),
            )
        )


class DisconnectRequest(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker(self) -> WorkerID:
        return WorkerID(self._msg.worker)

    @staticmethod
    def new_msg(worker: WorkerID) -> "DisconnectRequest":
        return DisconnectRequest(_message.DisconnectRequest(worker=bytes(worker)))


@dataclasses.dataclass
class DisconnectResponse(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker(self) -> WorkerID:
        return WorkerID(self._msg.worker)

    @staticmethod
    def new_msg(worker: WorkerID) -> "DisconnectResponse":
        return DisconnectResponse(_message.DisconnectResponse(worker=bytes(worker)))


class WorkerDisconnectNotification(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker(self) -> WorkerID:
        return WorkerID(self._msg.worker)

    @staticmethod
    def new_msg(worker: WorkerID) -> "WorkerDisconnectNotification":
        return WorkerDisconnectNotification(_message.WorkerDisconnectNotification(worker=bytes(worker)))


class ClientDisconnect(Message):
    class DisconnectType(enum.Enum):
        Disconnect = _message.ClientDisconnect.DisconnectType.disconnect
        Shutdown = _message.ClientDisconnect.DisconnectType.shutdown

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def disconnect_type(self) -> DisconnectType:
        return ClientDisconnect.DisconnectType(self._msg.disconnectType.raw)

    @staticmethod
    def new_msg(disconnect_type: DisconnectType) -> "ClientDisconnect":
        return ClientDisconnect(_message.ClientDisconnect(disconnectType=disconnect_type.value))


class ClientShutdownResponse(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def accepted(self) -> bool:
        return self._msg.accepted

    @staticmethod
    def new_msg(accepted: bool) -> "ClientShutdownResponse":
        return ClientShutdownResponse(_message.ClientShutdownResponse(accepted=accepted))


class StateClient(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @staticmethod
    def new_msg() -> "StateClient":
        return StateClient(_message.StateClient())


class StateObject(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @staticmethod
    def new_msg() -> "StateObject":
        return StateObject(_message.StateObject())


class StateBalanceAdvice(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker_id(self) -> WorkerID:
        return WorkerID(self._msg.workerId)

    @property
    def task_ids(self) -> List[TaskID]:
        return [TaskID(task_id) for task_id in self._msg.taskIds]

    @staticmethod
    def new_msg(worker_id: WorkerID, task_ids: List[TaskID]) -> "StateBalanceAdvice":
        return StateBalanceAdvice(
            _message.StateBalanceAdvice(workerId=bytes(worker_id), taskIds=[bytes(task_id) for task_id in task_ids])
        )


class StateScheduler(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def binder(self) -> BinderStatus:
        return BinderStatus(self._msg.binder)

    @property
    def scheduler(self) -> Resource:
        return Resource(self._msg.scheduler)

    @property
    def rss_free(self) -> int:
        return self._msg.rssFree

    @property
    def client_manager(self) -> ClientManagerStatus:
        return ClientManagerStatus(self._msg.clientManager)

    @property
    def object_manager(self) -> ObjectManagerStatus:
        return ObjectManagerStatus(self._msg.objectManager)

    @property
    def task_manager(self) -> TaskManagerStatus:
        return TaskManagerStatus(self._msg.taskManager)

    @property
    def worker_manager(self) -> WorkerManagerStatus:
        return WorkerManagerStatus(self._msg.workerManager)

    @property
    def scaling_manager(self) -> ScalingManagerStatus:
        return ScalingManagerStatus(self._msg.scalingManager)

    @staticmethod
    def new_msg(
        binder: BinderStatus,
        scheduler: Resource,
        rss_free: int,
        client_manager: ClientManagerStatus,
        object_manager: ObjectManagerStatus,
        task_manager: TaskManagerStatus,
        worker_manager: WorkerManagerStatus,
        scaling_manager: ScalingManagerStatus,
    ) -> "StateScheduler":
        return StateScheduler(
            _message.StateScheduler(
                binder=binder.get_message(),
                scheduler=scheduler.get_message(),
                rssFree=rss_free,
                clientManager=client_manager.get_message(),
                objectManager=object_manager.get_message(),
                taskManager=task_manager.get_message(),
                workerManager=worker_manager.get_message(),
                scalingManager=scaling_manager.get_message(),
            )
        )


class StateWorker(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker_id(self) -> WorkerID:
        return WorkerID(self._msg.workerId)

    @property
    def state(self) -> WorkerState:
        return WorkerState(self._msg.state.raw)

    @property
    def capabilities(self) -> Dict[str, int]:
        return {capability.name: capability.value for capability in self._msg.capabilities}

    @staticmethod
    def new_msg(worker_id: WorkerID, state: WorkerState, capabilities: Dict[str, int]) -> "StateWorker":
        return StateWorker(
            _message.StateWorker(
                workerId=bytes(worker_id),
                state=state.value,
                capabilities=[
                    TaskCapability.new_msg(name, value).get_message() for name, value in capabilities.items()
                ],
            )
        )


class StateTask(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def function_name(self) -> bytes:
        return bytes(self._msg.functionName)

    @property
    def state(self) -> TaskState:
        return TaskState(self._msg.state.raw)

    @property
    def worker(self) -> WorkerID:
        return WorkerID(self._msg.worker)

    @property
    def capabilities(self) -> Dict[str, int]:
        return {capability.name: capability.value for capability in self._msg.capabilities}

    @property
    def metadata(self) -> bytes:
        return bytes(self._msg.metadata)

    @staticmethod
    def new_msg(
        task_id: TaskID,
        function_name: bytes,
        task_state: TaskState,
        worker: WorkerID,
        capabilities: Dict[str, int],
        metadata: bytes = b"",
    ) -> "StateTask":
        return StateTask(
            _message.StateTask(
                taskId=bytes(task_id),
                functionName=function_name,
                state=task_state.value,
                worker=bytes(worker),
                capabilities=[
                    TaskCapability.new_msg(name, value).get_message() for name, value in capabilities.items()
                ],
                metadata=metadata,
            )
        )


class StateGraphTask(Message):
    class NodeTaskType(enum.Enum):
        Normal = _message.StateGraphTask.NodeTaskType.normal
        Target = _message.StateGraphTask.NodeTaskType.target

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def graph_task_id(self) -> TaskID:
        return TaskID(self._msg.graphTaskId)

    @property
    def task_id(self) -> TaskID:
        return TaskID(self._msg.taskId)

    @property
    def node_task_type(self) -> NodeTaskType:
        return StateGraphTask.NodeTaskType(self._msg.nodeTaskType.raw)

    @property
    def parent_task_ids(self) -> Set[TaskID]:
        return {TaskID(parent_task_id) for parent_task_id in self._msg.parentTaskIds}

    @staticmethod
    def new_msg(
        graph_task_id: TaskID, task_id: TaskID, node_task_type: NodeTaskType, parent_task_ids: Set[TaskID]
    ) -> "StateGraphTask":
        return StateGraphTask(
            _message.StateGraphTask(
                graphTaskId=bytes(graph_task_id),
                taskId=bytes(task_id),
                nodeTaskType=node_task_type.value,
                parentTaskIds=[bytes(parent_task_id) for parent_task_id in parent_task_ids],
            )
        )


class ProcessorInitialized(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @staticmethod
    def new_msg() -> "ProcessorInitialized":
        return ProcessorInitialized(_message.ProcessorInitialized())


class InformationRequest(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def request(self) -> bytes:
        return bytes(self._msg.request)

    @staticmethod
    def new_msg(request: bytes) -> "InformationRequest":
        return InformationRequest(_message.InformationRequest(request=request))


class InformationResponse(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def response(self) -> bytes:
        return bytes(self._msg.response)

    @staticmethod
    def new_msg(response: bytes) -> "InformationResponse":
        return InformationResponse(_message.InformationResponse(response=response))


# NOTE: This class is not a Message and does not have serialization methods. This is intentional.
class InformationSnapshot:
    def __init__(self, tasks: Dict[TaskID, Task], workers: Dict[WorkerID, WorkerHeartbeat]):
        self._tasks = tasks
        self._workers = workers

    @property
    def tasks(self) -> Dict[TaskID, Task]:
        return self._tasks

    @property
    def workers(self) -> Dict[WorkerID, WorkerHeartbeat]:
        return self._workers


PROTOCOL: bidict.bidict[str, Type[Message]] = bidict.bidict(
    {
        "task": Task,
        "taskCancel": TaskCancel,
        "taskCancelConfirm": TaskCancelConfirm,
        "taskResult": TaskResult,
        "taskLog": TaskLog,
        "graphTask": GraphTask,
        "objectInstruction": ObjectInstruction,
        "clientHeartbeat": ClientHeartbeat,
        "clientHeartbeatEcho": ClientHeartbeatEcho,
        "workerHeartbeat": WorkerHeartbeat,
        "workerHeartbeatEcho": WorkerHeartbeatEcho,
        "disconnectRequest": DisconnectRequest,
        "disconnectResponse": DisconnectResponse,
        "workerDisconnectNotification": WorkerDisconnectNotification,
        "stateClient": StateClient,
        "stateObject": StateObject,
        "stateBalanceAdvice": StateBalanceAdvice,
        "stateScheduler": StateScheduler,
        "stateWorker": StateWorker,
        "stateTask": StateTask,
        "stateGraphTask": StateGraphTask,
        "clientDisconnect": ClientDisconnect,
        "clientShutdownResponse": ClientShutdownResponse,
        "processorInitialized": ProcessorInitialized,
        "informationRequest": InformationRequest,
        "informationResponse": InformationResponse,
    }
)
