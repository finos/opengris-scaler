import dataclasses

from scaler.config.mixins import config_section
from scaler.config.types.zmq import ZMQConfig


@config_section
@dataclasses.dataclass
class TopConfig:
    monitor_address: ZMQConfig
    timeout: int = 5

    def __post_init__(self):
        if self.timeout <= 0:
            raise ValueError("timeout must be a positive integer.")
