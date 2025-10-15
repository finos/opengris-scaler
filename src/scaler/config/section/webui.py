import dataclasses
from typing import Optional, Tuple

from scaler.config import defaults
from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class WebUIConfig:
    monitor_address: ZMQConfig
    web_host: str = "0.0.0.0"
    web_port: int = 50001
    logging_paths: Tuple[str, ...] = defaults.DEFAULT_LOGGING_PATHS
    logging_config_file: Optional[str] = None
    logging_level: str = defaults.DEFAULT_LOGGING_LEVEL

    def __post_init__(self):
        if not isinstance(self.web_host, str):
            raise TypeError(f"Web host should be string, given {self.web_host}")
        if not isinstance(self.web_port, int):
            raise TypeError(f"Web port should be an integer, given {self.web_port}")
