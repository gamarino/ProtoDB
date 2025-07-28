import unittest
from unittest.mock import MagicMock, patch
import os
import tempfile
import shutil
import uuid
import threading
import time

from ..cloud_file_storage import (
    CloudFileStorage, CloudBlockProvider, MockGoogleCloudClient, 
    CloudStorageError, GoogleCloudClient
)
from ..cluster_file_storage import ClusterNetworkManager
from ..common import AtomPointer


class TestCloudFileStorageWithGCS(unittest.TestCase):
    """
    Test cases for the CloudFileStorage class with Google Cloud Storage.
    """

    def setUp(self):
        """
        Set up test environment before each test.
        """
        # Create a temporary directory for the cache
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a mock GoogleCloudClient
        self.gcs_client = MockGoogleCloudClient(
            bucket="test-bucket",
            prefix="test-prefix",
            project_id="test-project"
        )
        
        # Create a CloudBlockProvider with the mock client
        self.block_provider = CloudBlockProvider(
            cloud_client=self.gcs_client,
            cache_dir=os.path.join(self.temp_dir, "cache"),
            cache_size=1024 * 1024,  # 1MB cache
            object_size=1024  # 1KB objects
        )
        
        # Mock the ClusterNetworkManager
        self.mock_network_manager = MagicMock()
        self.mock_network_manager.start = MagicMock()
        self.mock_network_manager.stop = MagicMock()
        self.mock_network_manager.request_vote = MagicMock(return_value=(True, 1))
        self.mock_network_manager.request_page = MagicMock()
        self.mock_network_manager.broadcast_root_update = MagicMock()
        
        # Patch the ClusterNetworkManager constructor
        with patch('proto_db.cluster_file_storage.ClusterNetworkManager', return_value=self.mock_network_manager):
            # Create an instance of CloudFileStorage with the mocks
            self.storage = CloudFileStorage(
                block_provider=self.block_provider,
                server_id="test_server",
                host="localhost",
                port=12345,
                servers=[("localhost", 12346)],
                buffer_size=1024,  # Use a small buffer for tests
                blob_max_size=1024 * 10,  # Maximum of 10KB for test
                upload_interval_ms=100  # Short interval for tests
            )

    def tearDown(self):
        """
        Clean up test environment after each test.
        """
        # Close the storage
        if hasattr(self, 'storage'):
            self.storage.close()
        
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_initialization(self):
        """
        Test that CloudFileStorage can be initialized with GoogleCloudClient.
        """
        self.assertEqual(self.storage.state, 'Running')
        self.assertIsNotNone(self.storage.executor_pool)
        self.assertEqual(self.storage.current_wal_id, self.block_provider.get_new_wal.return_value[0])
        self.mock_network_manager.start.assert_called_once()
        self.assertIsNotNone(self.storage.uploader_thread)
        self.assertTrue(self.storage.uploader_thread.daemon)
        self.assertTrue(self.storage.uploader_thread.is_alive())

    def test_push_bytes(self):
        """
        Test pushing bytes to the storage.
        """
        # Create test data
        test_data = b"test data for Google Cloud Storage"
        
        # Push the data
        future = self.storage.push_bytes(test_data)
        transaction_id, offset = future.result()
        
        # Verify the result
        self.assertIsInstance(transaction_id, uuid.UUID)
        self.assertEqual(offset, 0)
        
        # Verify the data was stored in memory
        with self.storage._lock:
            self.assertIn((transaction_id, offset), self.storage.in_memory_segments)
            stored_data = self.storage.in_memory_segments[(transaction_id, offset)]
            # The stored data includes the length prefix (8 bytes)
            self.assertEqual(stored_data[8:], test_data)

    def test_get_bytes(self):
        """
        Test getting bytes from the storage.
        """
        # Create test data
        test_data = b"test data for Google Cloud Storage"
        
        # Push the data
        future = self.storage.push_bytes(test_data)
        transaction_id, offset = future.result()
        
        # Create a pointer
        pointer = AtomPointer(transaction_id, offset)
        
        # Get the data
        future = self.storage.get_bytes(pointer)
        retrieved_data = future.result()
        
        # Verify the data
        self.assertEqual(retrieved_data, test_data)

    def test_flush_wal(self):
        """
        Test flushing the WAL.
        """
        # Create test data
        test_data = b"test data for Google Cloud Storage"
        
        # Push the data
        future = self.storage.push_bytes(test_data)
        transaction_id, offset = future.result()
        
        # Flush the WAL
        future = self.storage.flush_wal()
        result = future.result()
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify the data was uploaded to Google Cloud Storage
        # This is hard to test directly, but we can verify that the in-memory segment was removed
        time.sleep(0.2)  # Give the uploader thread time to process
        with self.storage._lock:
            self.assertNotIn((transaction_id, offset), self.storage.in_memory_segments)

    def test_read_current_root(self):
        """
        Test reading the current root object.
        """
        # Create a mock root pointer
        mock_pointer = AtomPointer(uuid.uuid4(), 1234)
        
        # Mock the get_current_root_object method
        self.block_provider.get_current_root_object = MagicMock(return_value=mock_pointer)
        
        # Call the method
        result = self.storage.read_current_root()
        
        # Verify the result
        self.assertEqual(result, mock_pointer)
        
        # Verify the method was called
        self.block_provider.get_current_root_object.assert_called_once()

    def test_set_current_root(self):
        """
        Test setting the current root object.
        """
        # Create a mock root pointer
        mock_pointer = AtomPointer(uuid.uuid4(), 1234)
        
        # Call the method
        self.storage.set_current_root(mock_pointer)
        
        # Verify the method was called
        self.block_provider.update_root_object.assert_called_once_with(mock_pointer)
        
        # Verify the broadcast was called
        self.mock_network_manager.broadcast_root_update.assert_called_once_with(
            mock_pointer.transaction_id,
            mock_pointer.offset
        )

    def test_close(self):
        """
        Test closing the storage.
        """
        # Mock the executor pool shutdown
        with patch.object(self.storage.executor_pool, 'shutdown') as mock_shutdown:
            # Call the method
            self.storage.close()
            
            # Verify the state and that the mocks were called correctly
            self.assertEqual(self.storage.state, 'Closed')
            mock_shutdown.assert_called_once()
            self.block_provider.close.assert_called_once()
            self.mock_network_manager.stop.assert_called_once()
            
            # Verify the uploader thread was stopped
            self.assertFalse(self.storage.uploader_running)


if __name__ == '__main__':
    unittest.main()