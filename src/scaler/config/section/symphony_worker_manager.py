import dataclasses
from typing import ClassVar

from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class SymphonyWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "symphony"

    service_name: str = dataclasses.field(metadata=dict(short="-sn", help="symphony service name"))

    worker_manager_config: WorkerManagerConfig
    worker_manager_id: str = dataclasses.field(
        metadata=dict(short="-wmi", help="worker manager ID to identify this manager")
    )

    def __post_init__(self):
        if not self.worker_manager_id:
            raise ValueError("worker_manager_id cannot be an empty string.")
        if not self.service_name:
            raise ValueError("service_name cannot be an empty string.")
