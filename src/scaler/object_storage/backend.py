"""
Abstract backend interface for object storage.

This module defines the interface that all object storage backends must implement,
allowing for pluggable storage strategies (in-memory, Redis, disk-based, etc.).
"""

from abc import ABC, abstractmethod
from typing import Optional, Tuple


class ObjectStorageBackend(ABC):
    """Abstract base class for object storage backends."""

    @abstractmethod
    def put(self, object_id: bytes, data: bytes, metadata: bytes) -> bool:
        """
        Store an object in the backend.

        Args:
            object_id: Unique identifier for the object
            data: The object's data payload
            metadata: The object's metadata

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    def get(self, object_id: bytes) -> Optional[Tuple[bytes, bytes]]:
        """
        Retrieve an object from the backend.

        Args:
            object_id: Unique identifier for the object

        Returns:
            Tuple of (data, metadata) if found, None otherwise
        """
        pass

    @abstractmethod
    def delete(self, object_id: bytes) -> bool:
        """
        Delete an object from the backend.

        Args:
            object_id: Unique identifier for the object

        Returns:
            True if the object existed and was deleted, False otherwise
        """
        pass

    @abstractmethod
    def exists(self, object_id: bytes) -> bool:
        """
        Check if an object exists in the backend.

        Args:
            object_id: Unique identifier for the object

        Returns:
            True if the object exists, False otherwise
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """
        Get the total size of all objects in bytes.

        Returns:
            Total bytes stored in the backend
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """
        Clear all objects from the backend.
        Used primarily for testing and cleanup.
        """
        pass
