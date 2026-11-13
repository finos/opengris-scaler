import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig


@dataclasses.dataclass
class CloudChildrenConfig(ConfigClass):
    advertised_children_address: Optional[AddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-aca",
            help=(
                "address written into the units' start command for them to dial; defaults to children_address. "
                "Use this when the units reach this worker manager through NAT, e.g. an EC2 public IP."
            ),
        ),
    )

    unit_startup_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_MANAGER_UNIT_STARTUP_TIMEOUT_SECONDS,
        metadata=dict(
            short="-usts",
            help=(
                "seconds a cloud unit (EC2 instance, ECS task, OCI container instance) may take to send its first "
                "heartbeat before the worker manager counts it as lost. Raise it when units install scaler from source"
            ),
        ),
    )

    def __post_init__(self) -> None:
        if self.unit_startup_timeout_seconds <= 0:
            raise ValueError("unit_startup_timeout_seconds must be a positive number.")
