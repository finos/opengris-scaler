import dataclasses

from scaler.config.common.logging import LoggingConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class WebUIConfig(ConfigClass):
    monitor_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler monitor address to connect to")
    )
    web_host: str = dataclasses.field(default="0.0.0.0", metadata=dict(help="host for webserver to connect to"))
    web_port: int = dataclasses.field(default=50001, metadata=dict(help="host for webserver to connect to"))

    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
