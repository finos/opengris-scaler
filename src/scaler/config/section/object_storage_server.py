import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig


class RedisBackendConfig:
    """Configuration for Redis object storage backend."""

    url: str = "redis://localhost:6379/0"
    max_object_size_mb: int = 100
    key_prefix: str = "scaler:obj:"
    connection_pool_size: int = 10


@dataclasses.dataclass
class ObjectStorageServerConfig(ConfigClass):
    object_storage_address: ObjectStorageAddressConfig = dataclasses.field(
        metadata=dict(
            positional=True, help="specify the object storage server address to listen to, e.g. tcp://localhost:2345."
        )
    )
    backend: str = "memory"  # "memory" or "redis"
    redis: Optional[RedisBackendConfig] = None

    def __post_init__(self):
        if self.backend not in ("memory", "redis"):
            raise ValueError(f"backend must be 'memory' or 'redis', got '{self.backend}'")

        if self.backend == "redis" and self.redis is None:
            # Use default Redis config if not specified
            self.redis = RedisBackendConfig()
