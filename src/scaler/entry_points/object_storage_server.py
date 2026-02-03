import logging
import sys

from scaler.config.section.object_storage_server import ObjectStorageServerConfig
from scaler.utility.logging.utility import get_logger_info, setup_logger


def main():
    oss_config = ObjectStorageServerConfig.parse("Scaler Object Storage Server", "object_storage_server")

    setup_logger()

    log_format_str, log_level_str, log_paths = get_logger_info(logging.getLogger())

    try:
        # Use Python server for Redis backend, C++ server for memory backend
        if oss_config.backend == "redis":
            from scaler.object_storage.python_object_storage_server import PythonObjectStorageServer, create_backend

            # Build Redis config dict from config object
            redis_config = None
            if oss_config.redis is not None:
                redis_config = {
                    "url": oss_config.redis.url,
                    "max_object_size_mb": oss_config.redis.max_object_size_mb,
                    "key_prefix": oss_config.redis.key_prefix,
                    "connection_pool_size": oss_config.redis.connection_pool_size,
                }

            backend = create_backend("redis", redis_config)
            server = PythonObjectStorageServer(backend)

            logging.info(f"Using Python object storage server with Redis backend")
            server.run(
                oss_config.object_storage_address.host,
                oss_config.object_storage_address.port,
                oss_config.object_storage_address.identity,
                log_level_str,
                log_format_str,
                log_paths,
            )
        else:
            # Default: use the high-performance C++ server
            from scaler.object_storage.object_storage_server import ObjectStorageServer

            ObjectStorageServer().run(
                oss_config.object_storage_address.host,
                oss_config.object_storage_address.port,
                oss_config.object_storage_address.identity,
                log_level_str,
                log_format_str,
                log_paths,
            )
    except KeyboardInterrupt:
        sys.exit(0)
