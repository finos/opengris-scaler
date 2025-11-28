import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class WebConfig(ConfigClass):
    adapter_web_host: Optional[str] = dataclasses.field(
        default=None, metadata=dict(required=True, help="host address for the worker adapter HTTP server")
    )
    adapter_web_port: Optional[int] = dataclasses.field(
        default=None, metadata=dict(short="-p", required=True, help="port for the worker adapter HTTP server")
    )

    def __post_init__(self) -> None:
        if self.adapter_web_port and not (1 <= self.adapter_web_port <= 65535):
            raise ValueError(f"adapter_web_port must be between 1 and 65535, but got {self.adapter_web_port}")
