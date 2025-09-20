import io
import json
import struct
import unittest
from unittest.mock import Mock, MagicMock
from uuid import uuid4

import msgpack

from proto_db.standalone_file_storage import StandaloneFileStorage, AtomPointer, ProtoValidationException, \
    FORMAT_JSON_UTF8, FORMAT_MSGPACK


class TestFormatIndicators(unittest.TestCase):
    """
    Tests for the format indicator functionality in StandaloneFileStorage.
    """

    def setUp(self):
        """
        Setup for each test.
        """
        # Create a mock for the BlockProvider
        self.mock_block_provider = Mock()
        self.mock_block_provider.get_new_wal = MagicMock(return_value=(uuid4(), 0))
        self.mock_block_provider.write_streamer = MagicMock()
        self.mock_block_provider.get_reader = MagicMock()
        self.mock_block_provider.close_wal = MagicMock()

        # Create an instance of StandaloneFileStorage with the mock
        self.storage = StandaloneFileStorage(
            block_provider=self.mock_block_provider,
            buffer_size=1024,  # Use a small buffer for tests
            blob_max_size=1024 * 10  # Maximum of 10KB for test
        )

    def test_push_atom_with_json_format(self):
        """
        Test pushing an atom with JSON format.
        """
        # Test data
        test_atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}

        # Push atom with JSON format (default)
        future = self.storage.push_atom(test_atom)
        atom_pointer = future.result()

        # Verify the result is an AtomPointer
        self.assertIsInstance(atom_pointer, AtomPointer)
        self.assertEqual(atom_pointer.transaction_id, self.storage.current_wal_id)

    def test_push_atom_with_msgpack_format(self):
        """
        Test pushing an atom with MessagePack format.
        """
        # Test data
        test_atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}

        # Push atom with MessagePack format
        future = self.storage.push_atom_msgpack(test_atom)
        atom_pointer = future.result()

        # Verify the result is an AtomPointer
        self.assertIsInstance(atom_pointer, AtomPointer)
        self.assertEqual(atom_pointer.transaction_id, self.storage.current_wal_id)

    def test_get_atom_with_json_format(self):
        """
        Test retrieving an atom with JSON format.
        """
        # Test data
        test_atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}

        # Serialize the atom to JSON
        serialized_data = json.dumps(test_atom).encode('UTF-8')

        # Create a mock stream with format indicator
        len_data = struct.pack('Q', len(serialized_data))
        format_indicator = bytes([FORMAT_JSON_UTF8])
        mock_stream = io.BytesIO(len_data + format_indicator + serialized_data)
        self.mock_block_provider.get_reader.return_value = mock_stream

        # Create a fake pointer
        fake_pointer = AtomPointer(transaction_id=uuid4(), offset=0)

        # Get the atom
        future = self.storage.get_atom(fake_pointer)
        atom = future.result()

        # Verify the atom was correctly retrieved
        self.assertEqual(atom['className'], "TestAtom")
        self.assertEqual(atom['attr1'], "value1")
        self.assertEqual(atom['attr2'], 123)

    def test_get_atom_with_msgpack_format(self):
        """
        Test retrieving an atom with MessagePack format.
        """
        # Test data
        test_atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}

        # Serialize the atom to MessagePack
        serialized_data = msgpack.packb(test_atom)

        # Create a mock stream with format indicator
        len_data = struct.pack('Q', len(serialized_data))
        format_indicator = bytes([FORMAT_MSGPACK])
        mock_stream = io.BytesIO(len_data + format_indicator + serialized_data)
        self.mock_block_provider.get_reader.return_value = mock_stream

        # Create a fake pointer
        fake_pointer = AtomPointer(transaction_id=uuid4(), offset=0)

        # Get the atom
        future = self.storage.get_atom(fake_pointer)
        atom = future.result()

        # Verify the atom was correctly retrieved
        self.assertEqual(atom['className'], "TestAtom")
        self.assertEqual(atom['attr1'], "value1")
        self.assertEqual(atom['attr2'], 123)

    def test_get_atom_with_legacy_format(self):
        """
        Test retrieving an atom with legacy format (no format indicator).
        """
        # Test data
        test_atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}

        # Serialize the atom to JSON (legacy format)
        serialized_data = json.dumps(test_atom).encode('UTF-8')

        # Create a mock stream without format indicator
        len_data = struct.pack('Q', len(serialized_data))
        mock_stream = io.BytesIO(len_data + serialized_data)
        self.mock_block_provider.get_reader.return_value = mock_stream

        # Create a fake pointer
        fake_pointer = AtomPointer(transaction_id=uuid4(), offset=0)

        # Get the atom
        future = self.storage.get_atom(fake_pointer)
        atom = future.result()

        # Verify the atom was correctly retrieved
        self.assertEqual(atom['className'], "TestAtom")
        self.assertEqual(atom['attr1'], "value1")
        self.assertEqual(atom['attr2'], 123)

    def test_push_bytes_msgpack(self):
        """
        Test pushing bytes with MessagePack format.
        """
        # Test data
        test_data = {"key1": "value1", "key2": 123}

        # Push bytes with MessagePack format
        future = self.storage.push_bytes_msgpack(test_data)
        transaction_id, offset = future.result()

        # Verify the result
        self.assertEqual(transaction_id, self.storage.current_wal_id)
        self.assertEqual(offset, self.storage.current_wal_base)

    def test_invalid_format_type(self):
        """
        Test pushing data with an invalid format type.
        """
        # Test data
        test_atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}

        # Try to push atom with invalid format type
        with self.assertRaises(ProtoValidationException):
            self.storage.push_atom(test_atom, format_type=99)  # 99 is not a valid format type

        # Test data for push_bytes
        test_data = b"Test data"

        # Try to push bytes with invalid format type
        with self.assertRaises(ProtoValidationException):
            self.storage.push_bytes(test_data, format_type=99)  # 99 is not a valid format type


if __name__ == '__main__':
    unittest.main()
