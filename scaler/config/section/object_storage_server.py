import dataclasses

from scaler.config.mixins import config_section
from scaler.config.types.object_storage_server import ObjectStorageConfig


@config_section
@dataclasses.dataclass
class ObjectStorageServerConfig:
    object_storage_address: ObjectStorageConfig
