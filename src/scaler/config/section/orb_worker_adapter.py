import dataclasses
from typing import List, Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class ORBWorkerAdapterConfig(ConfigClass):
    """Configuration for the ORB worker adapter."""

    web_config: WebConfig
    worker_adapter_config: WorkerAdapterConfig

    # ORB Template configuration
    image_id: str = dataclasses.field(metadata=dict(help="AMI ID for the worker instances", required=True))
    key_name: Optional[str] = dataclasses.field(
        default=None, metadata=dict(help="AWS key pair name for the instances (optional)")
    )
    subnet_id: Optional[str] = dataclasses.field(
        default=None, metadata=dict(help="AWS subnet ID where the instances will be launched (optional)")
    )

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

    orb_config_path: str = dataclasses.field(
        default="src/scaler/drivers/orb", metadata=dict(help="Path to the ORB root directory")
    )

    instance_type: str = dataclasses.field(default="t2.micro", metadata=dict(help="EC2 instance type"))
    aws_region: Optional[str] = dataclasses.field(default="us-east-1", metadata=dict(help="AWS region"))
    security_group_ids: List[str] = dataclasses.field(
        default_factory=list,
        metadata=dict(
            type=lambda s: [x for x in s.split(",") if x], help="Comma-separated list of AWS security group IDs"
        ),
    )
    allowed_ip: str = dataclasses.field(
        default="", metadata=dict(help="IP address to allow in the security group (if created automatically)")
    )

    def __post_init__(self) -> None:
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
