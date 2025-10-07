import dataclasses

from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class TopConfig:
    monitor_address: ZMQConfig
    timeout: int = 5

    def __post_init__(self):
        if self.timeout <= 0:
            raise ValueError("timeout must be a positive integer.")
