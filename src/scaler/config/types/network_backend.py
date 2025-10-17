import enum


class NetworkBackend(enum.Enum):
    """
    Network backend to select when running scaler
    - tcp_zmq means for oss it use raw tcp, for client/scheduler/worker communication it use zmq
    - ymq means all components will use ymq for communication
    """

    tcp_zmq = enum.auto()
    ymq = enum.auto()
