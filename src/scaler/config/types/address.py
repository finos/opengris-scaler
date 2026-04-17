import dataclasses
import enum
import sys
from typing import Optional

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
from scaler.config.mixins import ConfigType


class SocketType(enum.Enum):
    inproc = "inproc"
    ipc = "ipc"
    tcp = "tcp"

    @staticmethod
    def allowed_types():
        return {t.value for t in SocketType}


@dataclasses.dataclass
class AddressConfig(ConfigType):
    type: SocketType
    host: str
    port: Optional[int] = None

    def __post_init__(self):
        if not isinstance(self.type, SocketType):
            raise TypeError(f"Invalid socket type {self.type}, available types are: {SocketType.allowed_types()}")

        if not isinstance(self.host, str):
            raise TypeError(f"Host should be string, given {self.host}")

        if self.port is None:
            if self.type == SocketType.tcp:
                raise ValueError(f"type {self.type.value} should have `port`")
        else:
            if self.type in {SocketType.inproc, SocketType.ipc}:
                raise ValueError(f"type {self.type.value} should not have `port`")

            if not isinstance(self.port, int):
                raise TypeError(f"Port should be integer, given {self.port}")

    @classmethod
    def from_string(cls, value: str) -> Self:
        if "://" not in value:
            raise ValueError("valid address config should be like tcp://127.0.0.1:12345")

        socket_type, host_port = value.split("://", 1)
        if socket_type not in SocketType.allowed_types():
            raise ValueError(f"supported socket types are: {SocketType.allowed_types()}")

        socket_type_enum = SocketType(socket_type)
        if socket_type_enum in {SocketType.inproc, SocketType.ipc}:
            host = host_port
            port_int = None
        elif socket_type_enum == SocketType.tcp:
            host, port = host_port.split(":")
            try:
                port_int = int(port)
            except ValueError:
                raise ValueError(f"cannot convert '{port}' to port number")
        else:
            raise ValueError(f"Unsupported socket type: {socket_type}")

        return cls(socket_type_enum, host, port_int)

    def __repr__(self) -> str:
        if self.type == SocketType.tcp:
            return f"tcp://{self.host}:{self.port}"

        if self.type in {SocketType.inproc, SocketType.ipc}:
            return f"{self.type.value}://{self.host}"

        raise TypeError(f"Unsupported socket type: {self.type}")

    def __str__(self) -> str:
        return repr(self)
