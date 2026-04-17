import dataclasses

from scaler.config.common.logging import LoggingConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig
from scaler.config.types.http import HTTPConfig


@dataclasses.dataclass
class WebGUIConfig(ConfigClass):
    monitor_address: AddressConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler monitor address to connect to")
    )
    gui_address: HTTPConfig = dataclasses.field(
        default_factory=lambda: HTTPConfig("0.0.0.0", 50001),
        metadata=dict(help="host and port for the web server (e.g. 0.0.0.0:50001)"),
    )

    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
