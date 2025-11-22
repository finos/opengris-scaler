import dataclasses

from scaler.config.config_class import ConfigClass
from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class TopConfig(ConfigClass):
    monitor_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, nargs="?", help="scheduler monitor address to connect to")
    )
    timeout: int = dataclasses.field(default=5, metadata=dict(short="-t", help="timeout seconds"))

    def __post_init__(self):
        if self.timeout <= 0:
            raise ValueError("timeout must be a positive integer.")
