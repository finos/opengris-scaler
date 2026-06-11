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
    ws = "ws"
    wss = "wss"

    @staticmethod
    def allowed_types():
        return {t.value for t in SocketType}


_TYPES_WITH_PORT = {SocketType.tcp, SocketType.ws, SocketType.wss}
_TYPES_WITHOUT_PORT = {SocketType.inproc, SocketType.ipc}


@dataclasses.dataclass
class AddressConfig(ConfigType):
    type: SocketType
    host: str
    port: Optional[int] = None
    path: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.type, SocketType):
            raise TypeError(f"Invalid socket type {self.type}, available types are: {SocketType.allowed_types()}")

        if not isinstance(self.host, str):
            raise TypeError(f"Host should be string, given {self.host}")

        if self.port is None:
            if self.type in _TYPES_WITH_PORT:
                raise ValueError(f"type {self.type.value} should have `port`")
        else:
            if self.type in _TYPES_WITHOUT_PORT:
                raise ValueError(f"type {self.type.value} should not have `port`")

            if not isinstance(self.port, int):
                raise TypeError(f"Port should be integer, given {self.port}")

        if self.path is not None and self.type not in {SocketType.ws, SocketType.wss}:
            raise ValueError(f"type {self.type.value} should not have `path`")

    @classmethod
    def from_string(cls, value: str) -> Self:
        if "://" not in value:
            raise ValueError("valid address config should be like tcp://127.0.0.1:12345")

        socket_type, rest = value.split("://", 1)
        if socket_type not in SocketType.allowed_types():
            raise ValueError(f"supported socket types are: {SocketType.allowed_types()}")

        socket_type_enum = SocketType(socket_type)
        if socket_type_enum in _TYPES_WITHOUT_PORT:
            return cls(socket_type_enum, host=rest)

        if socket_type_enum in _TYPES_WITH_PORT:
            authority, _, path_rest = rest.partition("/")
            host, port_str = authority.rsplit(":", 1)
            try:
                port_int = int(port_str)
            except ValueError:
                raise ValueError(f"cannot convert '{port_str}' to port number")
            path = ("/" + path_rest) if socket_type_enum in {SocketType.ws, SocketType.wss} else None
            return cls(socket_type_enum, host=host, port=port_int, path=path)

        raise ValueError(f"Unsupported socket type: {socket_type}")

    def __repr__(self) -> str:
        if self.type == SocketType.tcp:
            return f"{self.type.value}://{self.host}:{self.port}"

        if self.type in _TYPES_WITHOUT_PORT:
            return f"{self.type.value}://{self.host}"

        if self.type in {SocketType.ws, SocketType.wss}:
            path = self.path if self.path is not None else "/"
            return f"{self.type.value}://{self.host}:{self.port}{path}"

        raise TypeError(f"Unsupported socket type: {self.type}")

    def __str__(self) -> str:
        return repr(self)
