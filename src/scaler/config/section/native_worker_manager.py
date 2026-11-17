import argparse
import dataclasses
from typing import ClassVar, Optional

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.security import SecurityConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig


@dataclasses.dataclass
class NativeWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "baremetal_native"

    worker_manager_config: WorkerManagerConfig

    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    security: SecurityConfig = dataclasses.field(default_factory=SecurityConfig)

    worker_type: str = dataclasses.field(default="NAT", metadata=dict(help="worker type prefix used in worker IDs"))

    parent_address: Optional[AddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-pa",
            help=(
                "address of the parent worker manager to take the desired task concurrency from, instead of the "
                "scheduler. Set by a cloud worker manager in the command that starts this one"
            ),
        ),
    )

    unit_id: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(help="identity of this worker manager on the parent link, required with parent_address"),
    )

    def __post_init__(self) -> None:
        if (self.parent_address is None) != (self.unit_id is None):
            raise ValueError("parent_address and unit_id must be set together.")

    @classmethod
    def configure_parser(cls, parser: argparse.ArgumentParser) -> None:
        super().configure_parser(parser)
        parser.add_argument("-n", "--num-of-workers", dest="max_task_concurrency", type=int, help=argparse.SUPPRESS)
