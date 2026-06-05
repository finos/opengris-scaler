import dataclasses

from scaler.config.common.logging import LoggingConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig


@dataclasses.dataclass
class ObjectStorageServerConfig(ConfigClass):
    bind_address: AddressConfig = dataclasses.field(
        metadata=dict(
            positional=True, help="specify the object storage server address to listen to, e.g. tcp://localhost:2345."
        )
    )
    identity: str = dataclasses.field(default="ObjectStorageServer")
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
