import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig


@dataclasses.dataclass
class RedisBackendConfig:
    """Configuration for Redis object storage backend."""

    url: str = dataclasses.field(
        default="redis://localhost:6379/0", metadata=dict(help="Redis connection URL (e.g., redis://localhost:6379/0)")
    )
    max_object_size_mb: int = dataclasses.field(default=100, metadata=dict(help="Maximum object size in MB"))
    key_prefix: str = dataclasses.field(default="scaler:obj:", metadata=dict(help="Key prefix for Redis keys"))
    connection_pool_size: int = dataclasses.field(default=10, metadata=dict(help="Redis connection pool size"))


@dataclasses.dataclass
class ObjectStorageServerConfig(ConfigClass):
    object_storage_address: ObjectStorageAddressConfig = dataclasses.field(
        metadata=dict(
            positional=True, help="specify the object storage server address to listen to, e.g. tcp://localhost:2345."
        )
    )
    backend: str = dataclasses.field(
        default="memory",
        metadata=dict(
            help="Storage backend type: 'memory' (default, uses C++ server) or 'redis' (uses Python server with Redis)"
        ),
    )
    redis: Optional[RedisBackendConfig] = dataclasses.field(
        default=None, metadata=dict(help="Redis backend configuration (only used when backend='redis')")
    )

    def __post_init__(self):
        if self.backend not in ("memory", "redis"):
            raise ValueError(f"backend must be 'memory' or 'redis', got '{self.backend}'")

        if self.backend == "redis" and self.redis is None:
            # Use default Redis config if not specified
            self.redis = RedisBackendConfig()
