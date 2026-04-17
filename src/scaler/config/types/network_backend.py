import enum


class NetworkBackendType(enum.Enum):
    """
    Network backend to select when running scaler
    - tcp_zmq means client/scheduler/worker communication uses ZMQ
    - ymq means client/scheduler/worker communication uses YMQ

    Object storage always uses YMQ.
    """

    tcp_zmq = enum.auto()
    ymq = enum.auto()
