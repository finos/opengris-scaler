import dataclasses

from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageConfig


@dataclasses.dataclass
class ObjectStorageServerConfig(ConfigClass):
    object_storage_address: ObjectStorageConfig = dataclasses.field(
        metadata=dict(
            positional=True,
            nargs="?",
            help="specify the object storage server address to listen to, e.g. tcp://localhost:2345.",
        )
    )
