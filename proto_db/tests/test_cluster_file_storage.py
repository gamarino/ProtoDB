import io
import unittest
from unittest.mock import Mock, MagicMock, patch
from uuid import uuid4

from ..cluster_file_storage import ClusterFileStorage
from ..exceptions import ProtoUnexpectedException
from ..standalone_file_storage import AtomPointer


class TestClusterFileStorage(unittest.TestCase):
    """
    Test cases for the ClusterFileStorage class.
    """

    def setUp(self):
        """
        Set up test environment before each test.
        """
        # Create a mock for the BlockProvider
        self.mock_block_provider = Mock()
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
        self.mock_network_manager.request_vote = MagicMock(return_value=(True, 1))  # Return (success, votes)
        self.mock_network_manager.request_page = MagicMock(return_value=b"network data")
        self.mock_network_manager.broadcast_root_update = MagicMock(return_value=1)  # Return number of servers updated

        # Patch the ClusterNetworkManager constructor
        with patch('proto_db.cluster_file_storage.ClusterNetworkManager', return_value=self.mock_network_manager):
            # Create an instance of ClusterFileStorage with the mocks
            self.storage = ClusterFileStorage(
                block_provider=self.mock_block_provider,
                server_id="test_server",
                host="localhost",
                port=12345,
                servers=[("localhost", 12346)],
                buffer_size=1024,  # Use a small buffer for tests
                blob_max_size=1024 * 10,  # Maximum of 10KB for test
                max_retries=3,  # Number of retries for network operations
                retry_interval_ms=10  # Short interval for tests
            )

    def test_init_storage(self):
        """
        Test correct initialization of the class.
        """
        self.assertEqual(self.storage.state, 'Running')
        self.assertIsNotNone(self.storage.executor_pool)
        self.assertEqual(self.storage.current_wal_id, self.mock_block_provider.get_new_wal.return_value[0])
        self.mock_network_manager.start.assert_called_once()

    def test_read_current_root(self):
        """
        Test reading the current root.
        """
        # Set up the mock to return a specific AtomPointer
        expected_pointer = AtomPointer(uuid4(), 100)
        self.mock_block_provider.get_current_root_object.return_value = expected_pointer

        # Call the method
        result = self.storage.read_current_root()

        # Verify the result
        self.assertEqual(result, expected_pointer)
        self.mock_block_provider.get_current_root_object.assert_called_once()

    def test_read_lock_current_root(self):
        """
        Test reading and locking the current root.
        """
        # Set up the mock to return a specific AtomPointer
        expected_pointer = AtomPointer(uuid4(), 100)
        self.mock_block_provider.get_current_root_object.return_value = expected_pointer
        self.mock_network_manager.request_vote.return_value = (True, 3)  # (success, votes)

        # Call the method
        result = self.storage.read_lock_current_root()

        # Verify the result
        self.assertEqual(result, expected_pointer)
        self.mock_block_provider.get_current_root_object.assert_called_once()
        self.mock_network_manager.request_vote.assert_called_once()

    def test_read_lock_current_root_fails(self):
        """
        Test reading and locking the current root when vote fails.
        """
        # Set up the mock to return a specific AtomPointer
        expected_pointer = AtomPointer(uuid4(), 100)
        self.mock_block_provider.get_current_root_object.return_value = expected_pointer
        self.mock_network_manager.request_vote.return_value = (False, 0)  # (success, votes)

        # Call the method and expect an exception
        with self.assertRaises(ProtoUnexpectedException):  # Changed to match the actual exception type
            self.storage.read_lock_current_root()

        # Verify the mock was called
        self.mock_network_manager.request_vote.assert_called_once()

    def test_set_current_root(self):
        """
        Test setting the current root.
        """
        # Create a test AtomPointer
        test_pointer = AtomPointer(uuid4(), 200)

        # Call the method
        self.storage.set_current_root(test_pointer)

        # Verify the mocks were called correctly
        self.mock_block_provider.update_root_object.assert_called_once_with(test_pointer)
        self.mock_network_manager.broadcast_root_update.assert_called_once_with(
            test_pointer.transaction_id, test_pointer.offset
        )

    def test_unlock_current_root(self):
        """
        Test unlocking the current root.

        Note: In the actual implementation, unlock_current_root is a no-op method.
        The lock is automatically released when the vote timeout expires.
        """
        # Call the method - this is a no-op in the actual implementation
        self.storage.unlock_current_root()

        # No assertions needed as the method doesn't change any state

    def test_get_reader(self):
        """
        Test getting a reader for a specific WAL position.
        """
        # Create test parameters
        test_wal_id = uuid4()
        test_position = 300

        # Set up the mock to return a specific reader
        mock_reader = io.BytesIO(b"test data")
        self.mock_block_provider.get_reader.return_value = mock_reader

        # Call the method
        result = self.storage.get_reader(test_wal_id, test_position)

        # Verify the result and that the mock was called correctly
        self.assertEqual(result, mock_reader)
        self.mock_block_provider.get_reader.assert_called_once_with(test_wal_id, test_position)

    def test_get_reader_with_network_request(self):
        """
        Test getting a reader when local read fails and network request is needed.
        """
        # Create test parameters
        test_wal_id = uuid4()
        test_position = 300

        # Set up the mocks
        self.mock_block_provider.get_reader.side_effect = [
            ProtoUnexpectedException("File not found"),  # First call fails
            io.BytesIO(b"test data")  # Second call succeeds
        ]

        # Mock the network request to return data
        mock_data = b"network data"
        self.mock_network_manager.request_page.return_value = mock_data

        # Call the method
        result = self.storage.get_reader(test_wal_id, test_position)

        # Verify the result contains the network data
        self.assertEqual(result.read(), mock_data)
        self.mock_network_manager.request_page.assert_called_once_with(test_wal_id, test_position)

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
            self.mock_block_provider.close.assert_called_once()
            self.mock_network_manager.stop.assert_called_once()


if __name__ == '__main__':
    unittest.main()
