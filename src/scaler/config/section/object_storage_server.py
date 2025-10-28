import dataclasses

from scaler.config.types.object_storage_server import ObjectStorageConfig


@dataclasses.dataclass
class ObjectStorageServerConfig:
    object_storage_address: ObjectStorageConfig
