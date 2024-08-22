from typing import Dict, List

from scaler.protocol.capnp._python import _status  # noqa
from scaler.protocol.python.mixins import Message


class Resource(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def cpu(self) -> int:
        return self._msg.cpu

    @property
    def rss(self) -> int:
        return self._msg.rss

    @staticmethod
    def new_msg(cpu: int, rss: int) -> "Resource":  # type: ignore[override]
        return Resource(_status.Resource(cpu=cpu, rss=rss))

    def get_message(self):
        return self._msg


class ObjectManagerStatus(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def number_of_objects(self) -> int:
        return self._msg.numberOfObjects

    @property
    def object_memory(self) -> int:
        return self._msg.objectMemory

    @staticmethod
    def new_msg(number_of_objects: int, object_memory: int) -> "ObjectManagerStatus":  # type: ignore[override]
        return ObjectManagerStatus(
            _status.ObjectManagerStatus(numberOfObjects=number_of_objects, objectMemory=object_memory)
        )

    def get_message(self):
        return self._msg


class ClientManagerStatus(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def client_to_num_of_tasks(self) -> Dict[bytes, int]:
        return {p.client: p.numTask for p in self._msg.clientToNumOfTask}

    @staticmethod
    def new_msg(client_to_num_of_tasks: Dict[bytes, int]) -> "ClientManagerStatus":  # type: ignore[override]
        return ClientManagerStatus(
            _status.ClientManagerStatus(
                clientToNumOfTask=[
                    _status.ClientManagerStatus.Pair(client=p[0], numTask=p[1]) for p in client_to_num_of_tasks.items()
                ]
            )
        )

    def get_message(self):
        return self._msg


class TaskManagerStatus(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def unassigned(self) -> int:
        return self._msg.unassigned

    @property
    def running(self) -> int:
        return self._msg.running

    @property
    def success(self) -> int:
        return self._msg.success

    @property
    def failed(self) -> int:
        return self._msg.failed

    @property
    def canceled(self) -> int:
        return self._msg.canceled

    @property
    def not_found(self) -> int:
        return self._msg.notFound

    @staticmethod
    def new_msg(  # type: ignore[override]
        unassigned: int, running: int, success: int, failed: int, canceled: int, not_found: int
    ) -> "TaskManagerStatus":
        return TaskManagerStatus(
            _status.TaskManagerStatus(
                unassigned=unassigned,
                running=running,
                success=success,
                failed=failed,
                canceled=canceled,
                notFound=not_found,
            )
        )

    def get_message(self):
        return self._msg


class ProcessorStatus(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def pid(self) -> int:
        return self._msg.pid

    @property
    def initialized(self) -> int:
        return self._msg.initialized

    @property
    def has_task(self) -> bool:
        return self._msg.hasTask

    @property
    def suspended(self) -> bool:
        return self._msg.suspended

    @property
    def resource(self) -> Resource:
        return Resource(self._msg.resource)

    @staticmethod
    def new_msg(
        pid: int, initialized: int, has_task: bool, suspended: bool, resource: Resource  # type: ignore[override]
    ) -> "ProcessorStatus":
        return ProcessorStatus(
            _status.ProcessorStatus(
                pid=pid, initialized=initialized, hasTask=has_task, suspended=suspended, resource=resource.get_message()
            )
        )

    def get_message(self):
        return self._msg


class WorkerStatus(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def worker_id(self) -> bytes:
        return self._msg.workerId

    @property
    def agent(self) -> Resource:
        return Resource(self._msg.agent)

    @property
    def rss_free(self) -> int:
        return self._msg.rssFree

    @property
    def free(self) -> int:
        return self._msg.free

    @property
    def sent(self) -> int:
        return self._msg.sent

    @property
    def queued(self) -> int:
        return self._msg.queued

    @property
    def suspended(self) -> bool:
        return self._msg.suspended

    @property
    def lag_us(self) -> int:
        return self._msg.lagUS

    @property
    def last_s(self) -> int:
        return self._msg.lastS

    @property
    def itl(self) -> str:
        return self._msg.itl

    @property
    def processor_statuses(self) -> List[ProcessorStatus]:
        return [ProcessorStatus(ps) for ps in self._msg.processorStatuses]

    @staticmethod
    def new_msg(  # type: ignore[override]
        worker_id: bytes,
        agent: Resource,
        rss_free: int,
        free: int,
        sent: int,
        queued: int,
        suspended: int,
        lag_us: int,
        last_s: int,
        itl: str,
        processor_statuses: List[ProcessorStatus],
    ) -> "WorkerStatus":
        return WorkerStatus(
            _status.WorkerStatus(
                workerId=worker_id,
                agent=agent.get_message(),
                rssFree=rss_free,
                free=free,
                sent=sent,
                queued=queued,
                suspended=suspended,
                lagUS=lag_us,
                lastS=last_s,
                itl=itl,
                processorStatuses=[ps.get_message() for ps in processor_statuses],
            )
        )

    def get_message(self):
        return self._msg


class WorkerManagerStatus(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def workers(self) -> List[WorkerStatus]:
        return [WorkerStatus(ws) for ws in self._msg.workers]

    @staticmethod
    def new_msg(workers: List[WorkerStatus]) -> "WorkerManagerStatus":  # type: ignore[override]
        return WorkerManagerStatus(_status.WorkerManagerStatus(workers=[ws.get_message() for ws in workers]))

    def get_message(self):
        return self._msg


class BinderStatus(Message):
    def __init__(self, msg):
        self._msg = msg

    @property
    def received(self) -> Dict[str, int]:
        return {p.client: p.number for p in self._msg.received}

    @property
    def sent(self) -> Dict[str, int]:
        return {p.client: p.number for p in self._msg.sent}

    @staticmethod
    def new_msg(received: Dict[str, int], sent: Dict[str, int]) -> "BinderStatus":  # type: ignore[override]
        return BinderStatus(
            _status.BinderStatus(
                received=[_status.BinderStatus.Pair(client=p[0], number=p[1]) for p in received.items()],
                sent=[_status.BinderStatus.Pair(client=p[0], number=p[1]) for p in sent.items()],
            )
        )

    def get_message(self):
        return self._msg
