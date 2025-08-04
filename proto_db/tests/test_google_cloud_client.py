import os
import shutil
import tempfile
import unittest
import uuid
from unittest.mock import patch

from ..cloud_file_storage import MockGoogleCloudClient, CloudStorageError, CloudBlockProvider


class TestGoogleCloudClient(unittest.TestCase):
    """
    Test cases for the GoogleCloudClient class.
    """

    def setUp(self):
        """
        Set up test environment before each test.
        """
        # Create a temporary directory for the cache
        self.temp_dir = tempfile.mkdtemp()

        # Create a mock GoogleCloudClient
        self.client = MockGoogleCloudClient(
            bucket="test-bucket",
            prefix="test-prefix",
            project_id="test-project"
        )

        # Add some test data
        self.test_data = b"test data for Google Cloud Storage"
        self.client.put_object("test-key", self.test_data)

    def tearDown(self):
        """
        Clean up test environment after each test.
        """
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_get_object(self):
        """
        Test getting an object from Google Cloud Storage.
        """
        # Get the test object
        data, metadata = self.client.get_object("test-key")

        # Verify the data
        self.assertEqual(data, self.test_data)

        # Verify the metadata
        self.assertIn("ETag", metadata)
        self.assertEqual(metadata["ContentLength"], len(self.test_data))
        self.assertIn("LastModified", metadata)

    def test_put_object(self):
        """
        Test putting an object to Google Cloud Storage.
        """
        # Put a new object
        new_data = b"new test data"
        metadata = self.client.put_object("new-key", new_data)

        # Verify the metadata
        self.assertIn("ETag", metadata)
        self.assertEqual(metadata["ContentLength"], len(new_data))
        self.assertIn("LastModified", metadata)

        # Verify the object was stored
        data, _ = self.client.get_object("new-key")
        self.assertEqual(data, new_data)

    def test_list_objects(self):
        """
        Test listing objects in Google Cloud Storage.
        """
        # Add more test objects
        self.client.put_object("test-key2", b"test data 2")
        self.client.put_object("test-key3", b"test data 3")

        # List all objects
        objects = self.client.list_objects()

        # Verify the objects
        self.assertEqual(len(objects), 3)

        # List objects with a prefix
        objects = self.client.list_objects(prefix="test-key")

        # Verify the objects
        self.assertEqual(len(objects), 3)

    def test_delete_object(self):
        """
        Test deleting an object from Google Cloud Storage.
        """
        # Delete the test object
        result = self.client.delete_object("test-key")

        # Verify the result
        self.assertTrue(result)

        # Verify the object was deleted
        with self.assertRaises(CloudStorageError):
            self.client.get_object("test-key")

    def test_object_not_found(self):
        """
        Test getting a non-existent object.
        """
        # Try to get a non-existent object
        with self.assertRaises(CloudStorageError):
            self.client.get_object("non-existent-key")


class TestCloudBlockProviderWithGoogleCloud(unittest.TestCase):
    """
    Test cases for the CloudBlockProvider class with GoogleCloudClient.
    """

    def setUp(self):
        """
        Set up test environment before each test.
        """
        # Create a temporary directory for the cache
        self.temp_dir = tempfile.mkdtemp()

        # Create a mock GoogleCloudClient
        self.client = MockGoogleCloudClient(
            bucket="test-bucket",
            prefix="test-prefix",
            project_id="test-project"
        )

        # Create a CloudBlockProvider with the mock client
        self.provider = CloudBlockProvider(
            cloud_client=self.client,
            cache_dir=os.path.join(self.temp_dir, "cache"),
            cache_size=1024 * 1024,  # 1MB cache
            object_size=1024  # 1KB objects
        )

        # Add some test data
        self.test_data = b"test data for Google Cloud Storage"
        self.test_key = "test-key"
        self.client.put_object(self.test_key, self.test_data)

    def tearDown(self):
        """
        Clean up test environment after each test.
        """
        # Close the provider
        self.provider.close()

        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_get_object_key(self):
        """
        Test generating an object key from WAL ID and position.
        """
        # Create a test WAL ID
        test_wal_id = uuid.uuid4()
        test_position = 1024

        # Call the method
        result = self.provider._get_object_key(test_wal_id, test_position)

        # Verify the result format
        self.assertIn(str(test_wal_id), result)
        self.assertIn("1", result)  # 1024 // 1024 = 1

    def test_get_object_offset(self):
        """
        Test calculating the offset within an object.
        """
        # Test with position at the start of an object
        result = self.provider._get_object_offset(1024)
        self.assertEqual(result, 0)

        # Test with position in the middle of an object
        result = self.provider._get_object_offset(1536)
        self.assertEqual(result, 512)

    def test_cache_object(self):
        """
        Test caching an object locally.
        """
        # Get the object from the client
        data, metadata = self.client.get_object(self.test_key)

        # Cache the object
        cache_meta = self.provider._cache_object(self.test_key, data, metadata)

        # Verify the cache metadata
        self.assertEqual(cache_meta.key, self.test_key)
        self.assertEqual(cache_meta.size, len(data))
        self.assertTrue(cache_meta.is_cached)
        self.assertIsNotNone(cache_meta.cache_path)

        # Verify the object is in the cache
        self.assertIn(self.test_key, self.provider.cache_metadata)

        # Verify the cache file exists
        self.assertTrue(os.path.exists(cache_meta.cache_path))

    def test_get_reader(self):
        """
        Test getting a reader for a specific WAL position.
        """
        # Create a test WAL ID and position
        test_wal_id = uuid.uuid4()
        test_position = 0

        # Mock the _get_object_key method to return our test key
        with patch.object(self.provider, '_get_object_key', return_value=self.test_key):
            # Call the method
            reader = self.provider.get_reader(test_wal_id, test_position)

            # Verify the reader contains the test data
            data = reader.read()
            self.assertEqual(data, self.test_data)


if __name__ == '__main__':
    unittest.main()
