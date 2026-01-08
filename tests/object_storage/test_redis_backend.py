"""
Unit tests for Redis object storage backend.

These tests require a running Redis server. They can be run with:
    python -m unittest tests.object_storage.test_redis_backend

To skip these tests if Redis is not available, they will automatically skip.
"""

import unittest

from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

# Try to import redis and the backend
try:
    import redis

    from scaler.object_storage.redis_backend import RedisObjectStorageBackend

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


@unittest.skipIf(not REDIS_AVAILABLE, "Redis package not installed")
class TestRedisBackend(unittest.TestCase):
    """Tests for RedisObjectStorageBackend."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        setup_logger()
        if not REDIS_AVAILABLE:
            return

        # Test Redis connection
        cls.redis_url = "redis://localhost:6379/15"  # Use database 15 for testing
        try:
            test_client = redis.Redis.from_url(cls.redis_url)
            test_client.ping()
            cls.redis_available = True
        except (redis.ConnectionError, Exception):
            cls.redis_available = False

    def setUp(self):
        """Set up each test."""
        logging_test_name(self)

        if not self.redis_available:
            self.skipTest("Redis server not available")

        # Create backend
        self.backend = RedisObjectStorageBackend(
            redis_url=self.redis_url, max_object_size_mb=10, key_prefix="test:scaler:obj:"
        )
        # Clear any existing test data
        self.backend.clear()

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, "backend"):
            self.backend.clear()

    def test_put_and_get(self):
        """Test storing and retrieving an object."""
        object_id = b"test_object_1"
        data = b"Hello, World!"
        metadata = b"metadata_value"

        # Put object
        self.assertTrue(self.backend.put(object_id, data, metadata))

        # Get object
        result = self.backend.get(object_id)
        self.assertIsNotNone(result)
        retrieved_data, retrieved_metadata = result
        self.assertEqual(retrieved_data, data)
        self.assertEqual(retrieved_metadata, metadata)

    def test_get_nonexistent(self):
        """Test getting an object that doesn't exist."""
        result = self.backend.get(b"nonexistent")
        self.assertIsNone(result)

    def test_delete(self):
        """Test deleting an object."""
        object_id = b"test_object_2"
        data = b"data"
        metadata = b"metadata"

        # Put and verify
        self.backend.put(object_id, data, metadata)
        self.assertTrue(self.backend.exists(object_id))

        # Delete and verify
        self.assertTrue(self.backend.delete(object_id))
        self.assertFalse(self.backend.exists(object_id))

        # Delete again should return False
        self.assertFalse(self.backend.delete(object_id))

    def test_exists(self):
        """Test checking if an object exists."""
        object_id = b"test_object_3"
        data = b"data"
        metadata = b"metadata"

        # Should not exist initially
        self.assertFalse(self.backend.exists(object_id))

        # Put and check
        self.backend.put(object_id, data, metadata)
        self.assertTrue(self.backend.exists(object_id))

        # Delete and check
        self.backend.delete(object_id)
        self.assertFalse(self.backend.exists(object_id))

    def test_overwrite(self):
        """Test overwriting an existing object."""
        object_id = b"test_object_4"
        data1 = b"first_data"
        metadata1 = b"first_metadata"
        data2 = b"second_data"
        metadata2 = b"second_metadata"

        # Put first version
        self.backend.put(object_id, data1, metadata1)
        result = self.backend.get(object_id)
        self.assertEqual(result, (data1, metadata1))

        # Overwrite with second version
        self.backend.put(object_id, data2, metadata2)
        result = self.backend.get(object_id)
        self.assertEqual(result, (data2, metadata2))

    def test_size_tracking(self):
        """Test size tracking."""
        object_id1 = b"test_object_5"
        data1 = b"x" * 1000
        metadata1 = b"m" * 100

        object_id2 = b"test_object_6"
        data2 = b"y" * 2000
        metadata2 = b"n" * 200

        # Initial size should be 0
        initial_size = self.backend.size()

        # Add first object
        self.backend.put(object_id1, data1, metadata1)
        size_after_first = self.backend.size()
        self.assertGreater(size_after_first, initial_size)

        # Add second object
        self.backend.put(object_id2, data2, metadata2)
        size_after_second = self.backend.size()
        self.assertGreater(size_after_second, size_after_first)

        # Delete first object
        self.backend.delete(object_id1)
        size_after_delete = self.backend.size()
        self.assertLess(size_after_delete, size_after_second)

    def test_max_object_size(self):
        """Test maximum object size limit."""
        object_id = b"test_object_large"
        # Try to store object larger than max_object_size_mb (10MB)
        large_data = b"x" * (11 * 1024 * 1024)  # 11MB
        metadata = b"metadata"

        # Should fail due to size limit
        result = self.backend.put(object_id, large_data, metadata)
        self.assertFalse(result)

        # Object should not exist
        self.assertFalse(self.backend.exists(object_id))

    def test_empty_data(self):
        """Test storing objects with empty data."""
        object_id = b"test_object_empty"
        data = b""
        metadata = b"some_metadata"

        self.backend.put(object_id, data, metadata)
        result = self.backend.get(object_id)
        self.assertEqual(result, (data, metadata))

    def test_empty_metadata(self):
        """Test storing objects with empty metadata."""
        object_id = b"test_object_no_meta"
        data = b"some_data"
        metadata = b""

        self.backend.put(object_id, data, metadata)
        result = self.backend.get(object_id)
        self.assertEqual(result, (data, metadata))

    def test_binary_data(self):
        """Test storing binary data with various byte values."""
        object_id = b"test_object_binary"
        # Create data with all possible byte values
        data = bytes(range(256))
        metadata = bytes(range(128, 256)) + bytes(range(0, 128))

        self.backend.put(object_id, data, metadata)
        result = self.backend.get(object_id)
        self.assertEqual(result, (data, metadata))

    def test_multiple_objects(self):
        """Test storing and retrieving multiple objects."""
        objects = [(b"obj_%d" % i, b"data_%d" % i, b"meta_%d" % i) for i in range(10)]

        # Store all objects
        for object_id, data, metadata in objects:
            self.assertTrue(self.backend.put(object_id, data, metadata))

        # Retrieve and verify all objects
        for object_id, expected_data, expected_metadata in objects:
            result = self.backend.get(object_id)
            self.assertEqual(result, (expected_data, expected_metadata))

    def test_clear(self):
        """Test clearing all objects."""
        # Add multiple objects
        for i in range(5):
            object_id = b"test_object_%d" % i
            self.backend.put(object_id, b"data", b"metadata")

        # Verify they exist
        for i in range(5):
            self.assertTrue(self.backend.exists(b"test_object_%d" % i))

        # Clear all
        self.backend.clear()

        # Verify all are gone
        for i in range(5):
            self.assertFalse(self.backend.exists(b"test_object_%d" % i))

        # Size should be 0
        self.assertEqual(self.backend.size(), 0)

    def test_get_info(self):
        """Test getting backend information."""
        info = self.backend.get_info()

        self.assertEqual(info["type"], "redis")
        self.assertIn("object_count", info)
        self.assertIn("total_size_bytes", info)
        self.assertIn("max_object_size_mb", info)
        self.assertEqual(info["max_object_size_mb"], 10)

    def test_key_isolation(self):
        """Test that different key prefixes isolate objects."""
        backend1 = RedisObjectStorageBackend(redis_url=self.redis_url, key_prefix="prefix1:")
        backend2 = RedisObjectStorageBackend(redis_url=self.redis_url, key_prefix="prefix2:")

        try:
            backend1.clear()
            backend2.clear()

            # Store object in backend1
            object_id = b"shared_id"
            backend1.put(object_id, b"data1", b"meta1")

            # Should exist in backend1 but not backend2
            self.assertTrue(backend1.exists(object_id))
            self.assertFalse(backend2.exists(object_id))

            # Store different data with same ID in backend2
            backend2.put(object_id, b"data2", b"meta2")

            # Both should exist with different data
            self.assertEqual(backend1.get(object_id), (b"data1", b"meta1"))
            self.assertEqual(backend2.get(object_id), (b"data2", b"meta2"))

        finally:
            backend1.clear()
            backend2.clear()

    def test_concurrent_access(self):
        """Test that multiple operations work correctly."""
        import threading

        results = []

        def worker(worker_id):
            try:
                object_id = b"worker_%d" % worker_id
                data = b"data_%d" % worker_id
                metadata = b"meta_%d" % worker_id

                # Put
                self.backend.put(object_id, data, metadata)

                # Get
                result = self.backend.get(object_id)
                results.append((worker_id, result == (data, metadata)))

                # Delete
                self.backend.delete(object_id)
            except Exception as e:
                results.append((worker_id, False, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All workers should succeed
        self.assertEqual(len(results), 10)
        self.assertTrue(all(success for _, success in results))

    def test_large_metadata(self):
        """Test storing objects with large metadata."""
        object_id = b"test_large_meta"
        data = b"small_data"
        metadata = b"x" * (1024 * 1024)  # 1MB metadata

        result = self.backend.put(object_id, data, metadata)
        self.assertTrue(result)

        retrieved = self.backend.get(object_id)
        self.assertEqual(retrieved, (data, metadata))

    def test_special_object_ids(self):
        """Test various object ID formats."""
        special_ids = [b"\x00\x01\x02", b"\xff\xfe\xfd", b"a" * 100]  # Null bytes  # High bytes  # Long ID

        for object_id in special_ids:
            data = b"test_data"
            metadata = b"test_metadata"

            self.backend.put(object_id, data, metadata)
            result = self.backend.get(object_id)
            self.assertEqual(result, (data, metadata))
            self.backend.delete(object_id)


if __name__ == "__main__":
    unittest.main()
