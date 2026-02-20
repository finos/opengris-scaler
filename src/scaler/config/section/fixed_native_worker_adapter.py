import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class FixedNativeWorkerAdapterConfig(ConfigClass):
    worker_adapter_config: WorkerAdapterConfig
    preload: Optional[str] = None
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

    def __post_init__(self) -> None:
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
        if self.worker_adapter_config.max_workers < 0:
            raise ValueError("max_workers must be >=0 for fixed native worker adapter")
