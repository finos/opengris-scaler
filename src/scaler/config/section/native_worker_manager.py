import argparse
import dataclasses
import enum
from typing import Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType


class NativeWorkerManagerMode(enum.Enum):
    DYNAMIC = "dynamic"
    FIXED = "fixed"


@dataclasses.dataclass
class NativeWorkerManagerConfig(ConfigClass):
    worker_manager_config: WorkerManagerConfig
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

    mode: NativeWorkerManagerMode = dataclasses.field(
        default=NativeWorkerManagerMode.DYNAMIC,
        metadata=dict(
            type=NativeWorkerManagerMode,
            help="operating mode: 'dynamic' for auto-scaling driven by scheduler, 'fixed' for pre-spawned workers",
        ),
    )

    worker_type: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(help="worker type prefix used in worker IDs; defaults to 'FIX' or 'NAT' based on mode"),
    )

    @classmethod
    def configure_parser(cls, parser: argparse.ArgumentParser) -> None:
        super().configure_parser(parser)
        parser.add_argument("-n", "--num-of-workers", dest="max_workers", type=int, help=argparse.SUPPRESS)

    def __post_init__(self) -> None:
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
        if self.mode == NativeWorkerManagerMode.FIXED and self.worker_manager_config.max_workers < 0:
            raise ValueError("max_workers must be >= 0 for fixed mode")
