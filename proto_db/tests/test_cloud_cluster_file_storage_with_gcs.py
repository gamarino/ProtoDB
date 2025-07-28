import unittest
from unittest.mock import MagicMock, patch
import os
import tempfile
import shutil
import uuid
import threading
import time
import base64

from ..cloud_cluster_file_storage import CloudClusterFileStorage, CloudClusterStorageError
from ..cloud_file_storage import CloudBlockProvider, MockGoogleCloudClient, CloudStorageError
from ..file_block_provider import FileBlockProvider
from ..common import AtomPointer


class TestCloudClusterFileStorageWithGCS(unittest.TestCase):
    """
    Test cases for the CloudClusterFileStorage class with Google Cloud Storage.
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
            cache_dir=os.path.join(self.temp_dir, "cache")
        )
        
        # Mock the network manager to avoid actual network operations
        self.network_manager_patcher = patch('proto_db.cluster_file_storage.ClusterNetworkManager')
        self.mock_network_manager = self.network_manager_patcher.start()
        
        # Create an instance of the network manager mock
        self.mock_network_manager_instance = MagicMock()
        self.mock_network_manager.return_value = self.mock_network_manager_instance
        
        # Set up the mock network manager to return successful votes
        self.mock_network_manager_instance.request_vote.return_value = (True, 1)
        
        # Set up the mock network manager to return successful broadcasts
        self.mock_network_manager_instance.broadcast_root_update.return_value = 1
        
        # Create a CloudClusterFileStorage instance
        self.storage = CloudClusterFileStorage(
            block_provider=self.block_provider,
            server_id="test-server",
            host="localhost",
            port=12345,
            servers=[("localhost", 12345)],
            upload_interval_ms=100,  # Use a short interval for testing
            page_cache_dir=os.path.join(self.temp_dir, "page_cache")
        )

    def tearDown(self):
        """
        Clean up test environment after each test.
        """
        # Stop the network manager patcher
        self.network_manager_patcher.stop()
        
        # Close the storage
        if hasattr(self, 'storage'):
            self.storage.close()
        
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_initialization(self):
        """
        Test that CloudClusterFileStorage can be initialized with GoogleCloudClient.
        """
        self.assertIsNotNone(self.storage)
        self.assertEqual(self.storage.server_id, "test-server")
        self.assertEqual(self.storage.host, "localhost")
        self.assertEqual(self.storage.port, 12345)
        self.assertEqual(self.storage.upload_interval_ms, 100)
        self.assertIsNotNone(self.storage.page_cache)
        self.assertIsNotNone(self.storage.page_cache_provider)
        self.assertEqual(self.storage.page_cache_dir, os.path.join(self.temp_dir, "page_cache"))

    def test_read_lock_current_root(self):
        """
        Test that read_lock_current_root works correctly.
        """
        # Mock the read_current_root method to return a known value
        with patch.object(self.storage, 'read_current_root') as mock_read_current_root:
            mock_atom_pointer = MagicMock()
            mock_read_current_root.return_value = mock_atom_pointer
            
            # Call the method
            result = self.storage.read_lock_current_root()
            
            # Verify the result
            self.assertEqual(result, mock_atom_pointer)
            
            # Verify that request_vote was called
            self.mock_network_manager_instance.request_vote.assert_called_once()

    def test_set_current_root(self):
        """
        Test that set_current_root works correctly.
        """
        # Create a mock atom pointer
        mock_atom_pointer = MagicMock()
        mock_atom_pointer.transaction_id = uuid.uuid4()
        mock_atom_pointer.offset = 1234
        
        # Call the method
        self.storage.set_current_root(mock_atom_pointer)
        
        # Verify that update_root_object was called
        self.block_provider.update_root_object.assert_called_once_with(mock_atom_pointer)
        
        # Verify that broadcast_root_update was called
        self.mock_network_manager_instance.broadcast_root_update.assert_called_once_with(
            mock_atom_pointer.transaction_id,
            mock_atom_pointer.offset
        )

    def test_cloud_page_cache(self):
        """
        Test that the cloud page cache works correctly.
        """
        # Create test data
        wal_id = uuid.uuid4()
        position = 1234
        test_data = b"test data for cloud page cache"
        
        # Mock the cloud_client's get_object method to return our test data
        with patch.object(self.gcs_client, 'get_object') as mock_get_object:
            mock_get_object.return_value = (test_data, {"ETag": "test-etag"})
            
            # Mock the block provider's _get_object_key and _get_object_offset methods
            cloud_key = "test-cloud-key"
            with patch.object(self.block_provider, '_get_object_key', return_value=cloud_key), \
                 patch.object(self.block_provider, '_get_object_offset', return_value=0):
                
                # First call should get data from cloud storage and cache it
                reader1 = self.storage.get_reader(wal_id, position)
                data1 = reader1.read()
                self.assertEqual(data1, test_data)
                
                # Verify that get_object was called once
                mock_get_object.assert_called_once()
                
                # Reset the mock to verify it's not called again
                mock_get_object.reset_mock()
                
                # Second call should get data from cache
                reader2 = self.storage.get_reader(wal_id, position)
                data2 = reader2.read()
                self.assertEqual(data2, test_data)
                
                # Verify that get_object was not called again
                mock_get_object.assert_not_called()
                
                # Verify that the data was cached
                self.assertIn(cloud_key, self.storage.cloud_key_to_pointer)
                
                # Test that the network manager's page request handler uses the cache
                # Mock the original _handle_page_request method
                original_handle = MagicMock()
                self.storage.network_manager._handle_page_request = original_handle
                
                # Re-setup the cache-aware handler
                self.storage._setup_cache_aware_network_manager()
                
                # Create a test message
                message = {
                    'request_id': 'test-request',
                    'requester_id': 'test-requester',
                    'wal_id': str(wal_id),
                    'offset': position,
                    'size': len(test_data)
                }
                addr = ('localhost', 12345)
                
                # Mock the _send_message method
                with patch.object(self.storage.network_manager, '_send_message') as mock_send:
                    # Call the handler
                    self.storage.network_manager._handle_page_request(message, addr)
                    
                    # Verify that _send_message was called (indicating cache hit)
                    mock_send.assert_called_once()
                    
                    # Verify that the original handler was not called
                    original_handle.assert_not_called()

    def test_close(self):
        """
        Test that close works correctly.
        """
        # Mock the parent's close method and the page_cache close method
        with patch.object(self.storage.__class__.__bases__[0], 'close') as mock_close, \
             patch.object(self.storage.page_cache, 'close') as mock_cache_close:
            # Call the method
            self.storage.close()
            
            # Verify that close methods were called
            mock_close.assert_called_once()
            mock_cache_close.assert_called_once()
            
            # Verify that uploader_running was set to False
            self.assertFalse(self.storage.uploader_running)


if __name__ == '__main__':
    unittest.main()