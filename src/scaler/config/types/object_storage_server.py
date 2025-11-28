import dataclasses
import ipaddress

from scaler.config.mixins import ConfigType


@dataclasses.dataclass
class ObjectStorageAddressConfig(ConfigType):
    host: str
    port: int
    identity: str = "ObjectStorageServer"

    def __post_init__(self):
        try:
            ipaddress.ip_address(self.host)
        except ValueError:
            raise TypeError(f"Host must be a valid IP address, but got '{self.host}'")

        if not isinstance(self.identity, str):
            raise TypeError(f"Identity should be a string, given {self.identity}")

        if not isinstance(self.port, int):
            raise TypeError(f"Port should be an integer, given {self.port}")

    def __str__(self) -> str:
        return self.to_string()

    def to_string(self) -> str:
        return f"tcp://{self.host}:{self.port}"

    @classmethod
    def from_string(cls, value: str) -> "ObjectStorageAddressConfig":
        if not value.startswith("tcp://"):
            raise ValueError("Address must start with 'tcp://'")

        try:
            host, port_str = value[6:].rsplit(":", 1)
            port = int(port_str)

            ipaddress.ip_address(host)

        except (ValueError, IndexError):
            raise ValueError(f"Invalid address format '{value}'. Expected format is tcp://<ip_address>:<port>")

        return cls(host=host, port=port)
