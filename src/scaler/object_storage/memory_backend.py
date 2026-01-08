"""
Simple in-memory object storage backend.

This provides the default backend for Scaler's object storage server when
Redis is not configured.
"""

import logging
import threading
from typing import Dict, Optional, Tuple

from scaler.object_storage.backend import ObjectStorageBackend

logger = logging.getLogger(__name__)


class MemoryObjectStorageBackend(ObjectStorageBackend):
    """
    Thread-safe in-memory object storage backend.

    This is a simple Python implementation for use with the Python
    object storage server. For maximum performance, use the default
    C++ object storage server which has its own optimized in-memory storage.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._objects: Dict[bytes, Tuple[bytes, bytes]] = {}  # object_id -> (data, metadata)
        self._total_size = 0

    def put(self, object_id: bytes, data: bytes, metadata: bytes) -> bool:
        """Store an object in memory."""
        with self._lock:
            # Track size changes for updates
            old_size = 0
            if object_id in self._objects:
                old_data, old_meta = self._objects[object_id]
                old_size = len(old_data) + len(old_meta)

            self._objects[object_id] = (data, metadata)
            self._total_size += len(data) + len(metadata) - old_size

            logger.debug(f"Stored object {object_id.hex()[:16]}... ({len(data)} bytes)")
            return True

    def get(self, object_id: bytes) -> Optional[Tuple[bytes, bytes]]:
        """Retrieve an object from memory."""
        with self._lock:
            result = self._objects.get(object_id)
            if result:
                logger.debug(f"Retrieved object {object_id.hex()[:16]}... ({len(result[0])} bytes)")
            return result

    def delete(self, object_id: bytes) -> bool:
        """Delete an object from memory."""
        with self._lock:
            if object_id in self._objects:
                data, metadata = self._objects.pop(object_id)
                self._total_size -= len(data) + len(metadata)
                logger.debug(f"Deleted object {object_id.hex()[:16]}...")
                return True
            return False

    def exists(self, object_id: bytes) -> bool:
        """Check if an object exists."""
        with self._lock:
            return object_id in self._objects

    def size(self) -> int:
        """Get total size of all objects in bytes."""
        with self._lock:
            return self._total_size

    def clear(self) -> None:
        """Clear all objects."""
        with self._lock:
            count = len(self._objects)
            self._objects.clear()
            self._total_size = 0
            if count:
                logger.info(f"Cleared {count} objects from memory")

    def count(self) -> int:
        """Get number of objects stored."""
        with self._lock:
            return len(self._objects)
