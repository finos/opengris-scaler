import argparse
import dataclasses
from typing import ClassVar, Optional

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.security import SecurityConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class NativeWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "baremetal_native"

    worker_manager_config: WorkerManagerConfig

    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    security: SecurityConfig = dataclasses.field(default_factory=SecurityConfig)

    worker_type: Optional[str] = dataclasses.field(
        default=None, metadata=dict(help="worker type prefix used in worker IDs; defaults to 'NAT'")
    )

    @classmethod
    def configure_parser(cls, parser: argparse.ArgumentParser) -> None:
        super().configure_parser(parser)
        parser.add_argument("-n", "--num-of-workers", dest="max_task_concurrency", type=int, help=argparse.SUPPRESS)
