import dataclasses
from typing import ClassVar

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class SymphonyWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "symphony"

    service_name: str = dataclasses.field(metadata=dict(short="-sn", help="symphony service name"))

    worker_manager_config: WorkerManagerConfig

    worker_manager_id: str = dataclasses.field(
        metadata=dict(short="-wmi", required=True, help="worker manager ID to identify this manager")
    )

    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    def __post_init__(self):
        if not self.worker_manager_id:
            raise ValueError("worker_manager_id cannot be an empty string.")
        if not self.service_name:
            raise ValueError("service_name cannot be an empty string.")
