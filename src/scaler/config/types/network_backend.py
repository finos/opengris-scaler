import enum


class NetworkBackendType(enum.Enum):
    """
    Network backend to select when running scaler
    - ymq means client/scheduler/worker communication uses YMQ
    - zmq means client/scheduler/worker communication uses ZMQ

    Object storage always uses YMQ.
    """

    ymq = enum.auto()
    zmq = enum.auto()
