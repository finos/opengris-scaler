import dataclasses
from typing import Optional

from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.ymq_config import YMQConfig
from scaler.utility.zmq_config import ZMQConfig


@dataclasses.dataclass
class SchedulerConfig:
    event_loop: str
    address: YMQConfig
    storage_address: Optional[ObjectStorageConfig]
    monitor_address: Optional[ZMQConfig]
    io_threads: int
    max_number_of_tasks_waiting: int
    client_timeout_seconds: int
    worker_timeout_seconds: int
    object_retention_seconds: int
    load_balance_seconds: int
    load_balance_trigger_times: int
    protected: bool
    store_tasks: bool
    allocate_policy: AllocatePolicy
