import dataclasses
from typing import Dict, List


@dataclasses.dataclass
class Resource:
    cpu: float
    rss: int
    rss_free: int


@dataclasses.dataclass
class ObjectManagerStatus:
    number_of_objects: int
    object_memory: int


@dataclasses.dataclass
class ClientManagerStatus:
    client_to_num_of_tasks: Dict[bytes, int]


@dataclasses.dataclass
class TaskManagerStatus:
    unassigned: int
    running: int
    success: int
    failed: int
    canceled: int
    not_found: int


@dataclasses.dataclass
class ProcessorStatus:
    pid: int
    initialized: bool
    has_task: bool
    suspended: bool
    resource: Resource


@dataclasses.dataclass
class WorkerStatus:
    worker_id: bytes
    agent: Resource
    total_processors: Resource
    free: int
    sent: int
    queued: int
    suspended: int
    lag_us: int
    last_s: int
    ITL: str
    processor_statuses: List[ProcessorStatus]


@dataclasses.dataclass
class WorkerManagerStatus:
    workers: List[WorkerStatus]


@dataclasses.dataclass
class BinderStatus:
    received: Dict[str, int]
    sent: Dict[str, int]
