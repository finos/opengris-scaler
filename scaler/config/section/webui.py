import dataclasses

from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class WebUIConfig:
    monitor_address: ZMQConfig
    web_host: str = "0.0.0.0"
    web_port: int = 50001

    def __post_init__(self):
        if not isinstance(self.web_host, str):
            raise TypeError(f"Web host should be string, given {self.web_host}")
        if not isinstance(self.web_port, int):
            raise TypeError(f"Web port should be an integer, given {self.web_port}")
