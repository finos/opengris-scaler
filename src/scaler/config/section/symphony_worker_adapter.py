import dataclasses

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class SymphonyWorkerConfig(ConfigClass):
    service_name: str = dataclasses.field(metadata=dict(short="-sn", help="symphony service name"))

    web_config: WebConfig
    worker_adapter_config: WorkerAdapterConfig
    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )

    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS,
        metadata=dict(short="-wit", help="set the number of io threads for io backend per worker"),
    )

    def __post_init__(self):
        """Validates configuration values after initialization."""
        if not self.service_name:
            raise ValueError("service_name cannot be an empty string.")
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
