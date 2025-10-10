import enum


class TransportType(enum.Enum):
    YMQ = "YMQ"
    ZMQ = "ZMQ"

    @staticmethod
    def from_string(s: str) -> "TransportType":
        if s.lower() == "ymq":
            return TransportType.YMQ
        if s.lower() == "zmq":
            return TransportType.ZMQ
        raise ValueError(f"[{s}] is not a supported transport type")
