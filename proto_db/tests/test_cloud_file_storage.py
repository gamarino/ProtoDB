import unittest
from unittest.mock import Mock, MagicMock, patch, call
from uuid import uuid4
import io
import struct
import socket
import threading
import time
from typing import List, Tuple

from ..cloud_file_storage import CloudFileStorage, CloudBlockProvider, S3Client, MockS3Client, CloudStorageError
from ..cluster_file_storage import ClusterFileStorage, ClusterNetworkManager
from ..standalone_file_storage import StandaloneFileStorage, WALState, AtomPointer
from ..common import BlockProvider
from ..exceptions import ProtoValidationException, ProtoUnexpectedException


class TestCloudFileStorage(unittest.TestCase):
    """
    Test cases for the CloudFileStorage class.
    """

    def setUp(self):
        """
        Set up test environment before each test.
        """
        # Create a mock for the CloudBlockProvider
        self.mock_block_provider = Mock(spec=CloudBlockProvider)
        self.mock_block_provider.get_new_wal = MagicMock(return_value=(uuid4(), 0))
        self.mock_block_provider.write_streamer = MagicMock()
        self.mock_block_provider.get_reader = MagicMock()
        self.mock_block_provider.close_wal = MagicMock()
        self.mock_block_provider.get_current_root_object = MagicMock(return_value=AtomPointer(uuid4(), 0))
        self.mock_block_provider.update_root_object = MagicMock()

        # Mock the ClusterNetworkManager
        self.mock_network_manager = Mock()
        self.mock_network_manager.start = MagicMock()
        self.mock_network_manager.stop = MagicMock()
        self.mock_network_manager.request_vote = MagicMock(return_value=True)
        self.mock_network_manager.request_page = MagicMock()
        self.mock_network_manager.broadcast_root_update = MagicMock()

        # Patch the ClusterNetworkManager constructor
        with patch('proto_db.cluster_file_storage.ClusterNetworkManager', return_value=self.mock_network_manager):
            # Create an instance of CloudFileStorage with the mocks
            self.storage = CloudFileStorage(
                block_provider=self.mock_block_provider,
                server_id="test_server",
                host="localhost",
                port=12345,
                servers=[("localhost", 12346)],
                buffer_size=1024,  # Use a small buffer for tests
                blob_max_size=1024 * 10,  # Maximum of 10KB for test
                upload_interval_ms=100  # Short interval for tests
            )

    def test_init_storage(self):
        """
        Test correct initialization of the class.
        """
        self.assertEqual(self.storage.state, 'Running')
        self.assertIsNotNone(self.storage.executor_pool)
        self.assertEqual(self.storage.current_wal_id, self.mock_block_provider.get_new_wal.return_value[0])
        self.mock_network_manager.start.assert_called_once()
        self.assertIsNotNone(self.storage.uploader_thread)
        self.assertTrue(self.storage.uploader_thread.daemon)
        self.assertTrue(self.storage.uploader_thread.is_alive())

    def test_background_uploader(self):
        """
        Test the background uploader thread functionality.
        """
        # Mock the _process_pending_uploads method
        with patch.object(self.storage, '_process_pending_uploads') as mock_process:
            # Call the _background_uploader method directly
            # We need to patch time.sleep to avoid waiting
            with patch('time.sleep'):
                # Set a flag to exit the loop after one iteration
                self.storage.state = 'Running'

                # Call the method and then set state to Closed to exit the loop
                self.storage._background_uploader()

                # Verify the method was called
                mock_process.assert_called()

    def test_process_pending_uploads(self):
        """
        Test processing pending uploads.
        """
        # Create a test WAL ID
        test_wal_id = uuid4()
        self.storage.current_wal_id = test_wal_id

        # Add some data to the current WAL buffer
        self.storage.current_wal_buffer = [bytearray(b"test data")]
        self.storage.current_wal_offset = len(b"test data")

        # Mock the _flush_wal method
        with patch.object(self.storage, '_flush_wal') as mock_flush:
            # Call the method
            self.storage._process_pending_uploads()

            # Verify the method was called
            mock_flush.assert_called_once()

    def test_flush_wal(self):
        """
        Test flushing the WAL.
        """
        # Create a test WAL ID
        test_wal_id = uuid4()
        self.storage.current_wal_id = test_wal_id

        # Add some data to the current WAL buffer
        test_data = bytearray(b"test data")
        self.storage.current_wal_buffer = [test_data]
        self.storage.current_wal_offset = len(test_data)

        # Create a mock writer
        mock_writer = Mock()
        mock_writer.write = MagicMock()
        mock_writer.flush = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)

        # Mock the write_streamer to return our mock writer
        self.mock_block_provider.write_streamer.return_value = mock_writer

        # Call the method
        self.storage._flush_wal()

        # Verify the mocks were called correctly
        self.mock_block_provider.write_streamer.assert_called_once_with(test_wal_id)
        mock_writer.write.assert_called_once_with(test_data)
        mock_writer.flush.assert_called_once()

        # Verify the WAL buffer was cleared
        self.assertEqual(self.storage.current_wal_buffer, [bytearray()])
        self.assertEqual(self.storage.current_wal_offset, 0)

    def test_flush_wal_public_method(self):
        """
        Test the public flush_wal method.
        """
        # Mock the _flush_wal method
        with patch.object(self.storage, '_flush_wal') as mock_flush:
            # Call the method
            future = self.storage.flush_wal()
            result = future.result()  # Wait for the future to complete

            # Verify the method was called and returned True
            mock_flush.assert_called_once()
            self.assertTrue(result)

    def test_close(self):
        """
        Test closing the storage.
        """
        # Mock the executor pool shutdown and _flush_wal
        with patch.object(self.storage.executor_pool, 'shutdown') as mock_shutdown, \
             patch.object(self.storage, '_flush_wal') as mock_flush:

            # Call the method
            self.storage.close()

            # Verify the state and that the mocks were called correctly
            self.assertEqual(self.storage.state, 'Closed')
            mock_shutdown.assert_called_once()
            mock_flush.assert_called_once()
            self.mock_block_provider.close.assert_called_once()
            self.mock_network_manager.stop.assert_called_once()

            # Verify the uploader thread was stopped
            self.assertFalse(self.storage.uploader_running)


class TestCloudBlockProvider(unittest.TestCase):
    """
    Test cases for the CloudBlockProvider class.
    """

    def setUp(self):
        """
        Set up test environment before each test.
        """
        # Create a mock for the S3Client
        self.mock_s3_client = Mock(spec=S3Client)
        self.mock_s3_client.get_object = MagicMock(return_value=b"test data")
        self.mock_s3_client.put_object = MagicMock()
        self.mock_s3_client.list_objects = MagicMock(return_value=[])
        self.mock_s3_client.delete_object = MagicMock()

        # Patch the os.path.exists and os.makedirs functions
        with patch('os.path.exists', return_value=True), \
             patch('os.makedirs'):

            # Create an instance of CloudBlockProvider with the mock
            self.provider = CloudBlockProvider(
                s3_client=self.mock_s3_client,
                cache_dir="/tmp/test_cache",
                cache_size=1024 * 1024,  # 1MB cache
                object_size=1024  # 1KB objects
            )

            # Mock the _load_cache_metadata and _load_config methods
            with patch.object(self.provider, '_load_cache_metadata'), \
                 patch.object(self.provider, '_load_config'):

                # Initialize the provider
                pass

    def test_get_object_key(self):
        """
        Test generating an object key from WAL ID and position.
        """
        # Create a test WAL ID
        test_wal_id = uuid4()
        test_position = 1024

        # Call the method
        result = self.provider._get_object_key(test_wal_id, test_position)

        # Verify the result format
        self.assertIn(str(test_wal_id), result)
        self.assertIn(str(test_position), result)

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

    def test_get_reader(self):
        """
        Test getting a reader for a specific WAL position.
        """
        # Create test parameters
        test_wal_id = uuid4()
        test_position = 1024

        # Mock the S3Client.get_object to return test data
        test_data = b"test data"
        self.mock_s3_client.get_object.return_value = test_data

        # Call the method
        result = self.provider.get_reader(test_wal_id, test_position)

        # Verify the result is a BytesIO object containing the test data
        self.assertIsInstance(result, io.BytesIO)
        self.assertEqual(result.read(), test_data)

        # Verify the S3Client.get_object was called with the correct key
        expected_key = self.provider._get_object_key(test_wal_id, test_position)
        self.mock_s3_client.get_object.assert_called_once_with(expected_key)

    def test_write_streamer(self):
        """
        Test the write streamer functionality.
        """
        # Create a test WAL ID
        test_wal_id = uuid4()

        # Call the method to get a writer
        writer = self.provider.write_streamer(test_wal_id)

        # Verify the writer is an instance of the S3Writer class
        self.assertIsNotNone(writer)

        # Write some test data
        test_data = b"test data"
        writer.write(test_data)

        # Close the writer to flush data to S3
        writer.close()

        # Verify the S3Client.put_object was called
        self.mock_s3_client.put_object.assert_called()


if __name__ == '__main__':
    unittest.main()
