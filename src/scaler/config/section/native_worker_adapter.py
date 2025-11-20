import dataclasses

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class NativeWorkerAdapterConfig(ConfigClass):
    web_config: WebConfig
    worker_adapter_config: WorkerAdapterConfig
    worker_config: WorkerConfig = WorkerConfig()
    logging_config: LoggingConfig = LoggingConfig()
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )

    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS,
        metadata=dict(short="-wit", help="set the number of io threads for io backend per worker"),
    )

    def __post_init__(self) -> None:
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
