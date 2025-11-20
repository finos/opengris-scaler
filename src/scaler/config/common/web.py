import dataclasses

from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class WebConfig(ConfigClass):
    adapter_web_host: str = dataclasses.field(
        metadata=dict(required=True, help="host address for the worker adapter HTTP server")
    )
    adapter_web_port: int = dataclasses.field(
        metadata=dict(short="-p", required=True, help="port for the worker adapter HTTP server")
    )

    def __post_init__(self) -> None:
        if not (1 <= self.adapter_web_port <= 65535):
            raise ValueError(f"adapter_web_port must be between 1 and 65535, but got {self.adapter_web_port}")
