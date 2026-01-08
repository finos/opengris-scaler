"""
Redis-based object storage backend.

This module provides an optional Redis backend for distributed object storage.
Redis is useful for:
- Sharing objects across multiple nodes
- Persisting objects across scheduler restarts
- Small-to-medium sized objects (< 100MB recommended)

Note: Redis must be installed separately: pip install opengris-scaler[redis]
"""

import logging
import struct
from typing import Optional, Tuple

from scaler.object_storage.backend import ObjectStorageBackend

logger = logging.getLogger(__name__)


class RedisObjectStorageBackend(ObjectStorageBackend):
    """
    Redis-based object storage backend.

    This backend stores objects in Redis, which provides:
    - Distributed access across multiple nodes
    - Optional persistence to disk
    - Automatic memory management

    Limitations:
    - Redis has a default max value size of 512MB
    - All data is stored in RAM (unless Redis persistence is configured)
    - Network latency for remote Redis servers
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        max_object_size_mb: int = 100,
        key_prefix: str = "scaler:obj:",
        connection_pool_size: int = 10,
    ):
        """
        Initialize Redis backend.

        Args:
            redis_url: Redis connection URL (e.g., "redis://localhost:6379/0")
            max_object_size_mb: Maximum object size in MB (default: 100)
            key_prefix: Prefix for all Redis keys (default: "scaler:obj:")
            connection_pool_size: Size of connection pool (default: 10)

        Raises:
            ImportError: If redis package is not installed
            ConnectionError: If unable to connect to Redis
        """
        try:
            import redis
        except ImportError:
            raise ImportError(
                "Redis backend requires the 'redis' package. " "Install it with: pip install opengris-scaler[redis]"
            )

        self.max_object_size = max_object_size_mb * 1024 * 1024  # Convert to bytes
        self.key_prefix = key_prefix

        # Create Redis client with connection pooling
        self.client = redis.Redis.from_url(
            redis_url, decode_responses=False, max_connections=connection_pool_size  # We work with bytes
        )

        # Test connection
        try:
            self.client.ping()
            logger.info(f"RedisObjectStorageBackend: Connected to Redis at {redis_url}")
        except redis.ConnectionError as e:
            raise ConnectionError(f"Failed to connect to Redis at {redis_url}: {e}")

        # Track total size (approximate, since Redis doesn't provide this directly)
        # Use _meta: prefix to distinguish from object keys
        self._size_key = f"{key_prefix}_meta:total_size"
        if not self.client.exists(self._size_key):
            self.client.set(self._size_key, 0)

    def _make_key(self, object_id: bytes) -> bytes:
        """Convert object_id to Redis key."""
        return (self.key_prefix + object_id.hex()).encode()

    def _serialize(self, data: bytes, metadata: bytes) -> bytes:
        """
        Serialize data and metadata into a single byte string.

        Format: [metadata_len (4 bytes)] [metadata] [data]
        """
        metadata_len = struct.pack("<I", len(metadata))  # Little-endian unsigned int
        return metadata_len + metadata + data

    def _deserialize(self, value: bytes) -> Tuple[bytes, bytes]:
        """
        Deserialize byte string into data and metadata.

        Returns:
            Tuple of (data, metadata)
        """
        metadata_len = struct.unpack("<I", value[:4])[0]
        metadata = value[4 : 4 + metadata_len]
        data = value[4 + metadata_len :]
        return data, metadata

    def put(self, object_id: bytes, data: bytes, metadata: bytes) -> bool:
        """
        Store an object in Redis.

        Args:
            object_id: Unique identifier for the object
            data: The object's data payload
            metadata: The object's metadata

        Returns:
            True if successful, False if object is too large
        """
        total_size = len(data) + len(metadata)

        # Check size limit
        if total_size > self.max_object_size:
            logger.warning(
                f"Object {object_id.hex()} size ({total_size} bytes) exceeds "
                f"max_object_size ({self.max_object_size} bytes)"
            )
            return False

        key = self._make_key(object_id)
        value = self._serialize(data, metadata)

        try:
            # Use pipeline for atomicity and fewer round trips
            pipe = self.client.pipeline()

            # Get old value size if exists (for accurate size tracking)
            old_value = self.client.get(key)
            old_size = len(old_value) if old_value else 0

            # Store the object and update size tracking atomically
            pipe.set(key, value)
            size_delta = len(value) - old_size
            pipe.incrby(self._size_key, size_delta)
            pipe.execute()

            logger.debug(f"Stored object {object_id.hex()} ({total_size} bytes) in Redis")
            return True

        except Exception as e:
            logger.error(f"Failed to store object {object_id.hex()} in Redis: {e}")
            return False

    def get(self, object_id: bytes) -> Optional[Tuple[bytes, bytes]]:
        """
        Retrieve an object from Redis.

        Args:
            object_id: Unique identifier for the object

        Returns:
            Tuple of (data, metadata) if found, None otherwise
        """
        key = self._make_key(object_id)

        try:
            value = self.client.get(key)
            if value is None:
                return None

            data, metadata = self._deserialize(value)
            logger.debug(f"Retrieved object {object_id.hex()} ({len(data)} bytes) from Redis")
            return data, metadata

        except Exception as e:
            logger.error(f"Failed to retrieve object {object_id.hex()} from Redis: {e}")
            return None

    def delete(self, object_id: bytes) -> bool:
        """
        Delete an object from Redis.

        Args:
            object_id: Unique identifier for the object

        Returns:
            True if the object existed and was deleted, False otherwise
        """
        key = self._make_key(object_id)

        try:
            # Get size before deletion for tracking
            value = self.client.get(key)
            if value is None:
                return False

            # Delete the object
            deleted = self.client.delete(key) > 0

            if deleted:
                # Update size tracking
                self.client.decrby(self._size_key, len(value))
                logger.debug(f"Deleted object {object_id.hex()} from Redis")

            return deleted

        except Exception as e:
            logger.error(f"Failed to delete object {object_id.hex()} from Redis: {e}")
            return False

    def exists(self, object_id: bytes) -> bool:
        """
        Check if an object exists in Redis.

        Args:
            object_id: Unique identifier for the object

        Returns:
            True if the object exists, False otherwise
        """
        key = self._make_key(object_id)
        try:
            return self.client.exists(key) > 0
        except Exception as e:
            logger.error(f"Failed to check existence of object {object_id.hex()} in Redis: {e}")
            return False

    def size(self) -> int:
        """
        Get the approximate total size of all objects in bytes.

        Note: This is an approximation based on tracking. Redis overhead
        (keys, internal structures) is not included.

        Returns:
            Total bytes stored in the backend
        """
        try:
            size_bytes = self.client.get(self._size_key)
            if size_bytes is None:
                return 0
            return int(size_bytes)
        except Exception as e:
            logger.error(f"Failed to get total size from Redis: {e}")
            return 0

    def clear(self) -> None:
        """
        Clear all objects from the backend.

        Warning: This deletes ALL keys matching the key_prefix pattern.
        Use with caution in production!
        """
        try:
            # Find all keys with our prefix
            pattern = f"{self.key_prefix}*".encode()
            keys = list(self.client.scan_iter(pattern, count=1000))

            if keys:
                # Delete all keys in batches
                self.client.delete(*keys)
                logger.info(f"Cleared {len(keys)} objects from Redis")

            # Reset size counter
            self.client.set(self._size_key, 0)

        except Exception as e:
            logger.error(f"Failed to clear objects from Redis: {e}")

    def get_info(self) -> dict:
        """
        Get information about the Redis backend.

        Returns:
            Dictionary with backend statistics
        """
        try:
            info = self.client.info("memory")
            pattern = f"{self.key_prefix}*".encode()
            # Count only object keys, excluding metadata keys
            count = sum(1 for key in self.client.scan_iter(pattern, count=1000) if b"_meta:" not in key)

            return {
                "type": "redis",
                "object_count": count,
                "total_size_bytes": self.size(),
                "max_object_size_mb": self.max_object_size // (1024 * 1024),
                "redis_used_memory": info.get("used_memory", 0),
                "redis_used_memory_human": info.get("used_memory_human", "unknown"),
            }
        except Exception as e:
            logger.error(f"Failed to get Redis info: {e}")
            return {"type": "redis", "error": str(e)}

    def close(self) -> None:
        """
        Close the Redis connection.

        Should be called when the backend is no longer needed.
        """
        try:
            self.client.close()
            logger.debug("Closed Redis connection")
        except Exception as e:
            logger.error(f"Failed to close Redis connection: {e}")
