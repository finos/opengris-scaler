import argparse
import dataclasses
import enum
from typing import ClassVar, Optional

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


class NativeWorkerManagerMode(enum.Enum):
    DYNAMIC = "dynamic"
    FIXED = "fixed"


@dataclasses.dataclass
class NativeWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "baremetal_native"

    worker_manager_config: WorkerManagerConfig

    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

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
        parser.add_argument("-n", "--num-of-workers", dest="max_task_concurrency", type=int, help=argparse.SUPPRESS)

    def __post_init__(self) -> None:
        if self.mode == NativeWorkerManagerMode.FIXED and self.worker_manager_config.max_task_concurrency < 0:
            raise ValueError("max_task_concurrency must be >= 0 for fixed mode")
