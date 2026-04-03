import dataclasses
import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from scaler.config.mixins import ConfigType


@dataclasses.dataclass
class HTTPConfig(ConfigType):
    host: str
    port: int

    def __post_init__(self):
        if not isinstance(self.host, str) or not self.host:
            raise TypeError(f"Host should be a non-empty string, given {self.host!r}")

        if not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            raise ValueError(f"Port should be an integer between 1 and 65535, given {self.port!r}")

    @classmethod
    def from_string(cls, value: str) -> Self:
        if ":" not in value:
            raise ValueError(f"Invalid HTTP address {value!r}, expected format: host:port (e.g. 0.0.0.0:50001)")

        host, _, port_str = value.rpartition(":")
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Cannot convert {port_str!r} to port number")

        return cls(host, port)

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"

    def __repr__(self) -> str:
        return f"{self.host}:{self.port}"
