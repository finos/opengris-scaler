import dataclasses
from datetime import timedelta

from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig


def _parse_timeout_seconds(value: str) -> timedelta:
    return timedelta(seconds=float(value))


@dataclasses.dataclass
class TopConfig(ConfigClass):
    monitor_address: AddressConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler monitor address to connect to")
    )
    timeout: timedelta = dataclasses.field(
        default=timedelta(seconds=5),
        metadata={"short": "-t", "type": _parse_timeout_seconds, "help": "timeout seconds"},
    )

    def __post_init__(self):
        if self.timeout <= timedelta(seconds=0):
            raise ValueError("timeout must be a positive duration.")
