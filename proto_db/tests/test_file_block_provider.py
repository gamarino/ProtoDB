import io
import json
import os
import unittest
import uuid
from tempfile import TemporaryDirectory
from unittest.mock import patch

from proto_db.file_block_provider import FileReaderFactory, PageCache, FileBlockProvider, ProtoUnexpectedException


class TestFileReaderFactory(unittest.TestCase):
    def setUp(self):
        """Set up a temporary directory and mock files."""
        self.temp_dir = TemporaryDirectory()
        self.factory = FileReaderFactory(path=self.temp_dir.name)
        self.file_name = "testfile.wal"
        self.file_path = os.path.join(self.temp_dir.name, self.file_name)
        with open(self.file_path, "wb") as f:
            f.write(b"test data")

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    def test_get_reader_creates_new_reader(self):
        """Test if a new reader is created when none are available."""
        reader = self.factory.get_reader(self.file_name)
        self.assertIsInstance(reader, io.BufferedReader)
        self.assertEqual(reader.read(), b"test data")
        reader.close()

    def test_return_reader(self):
        """Test returning a file reader to the pool."""
        reader = self.factory.get_reader(self.file_name)
        self.factory.return_reader(reader, self.file_name)
        with self.factory._lock:
            self.assertEqual(len(self.factory.available_readers[self.file_name]), 1)

    def test_get_existing_reader(self):
        """Test reusing an existing reader."""
        reader1 = self.factory.get_reader(self.file_name)
        self.factory.return_reader(reader1, self.file_name)
        reader2 = self.factory.get_reader(self.file_name)
        self.assertEqual(reader1, reader2)
        reader2.close()

    def test_file_not_found_error(self):
        """Test that a FileNotFoundError raises a ProtoUnexpectedException."""
        with self.assertRaises(ProtoUnexpectedException):
            self.factory.get_reader("nonexistent.wal")

    def test_file_permission_error(self):
        """Test that PermissionError is handled."""
        with patch("builtins.open", side_effect=PermissionError):
            with self.assertRaises(ProtoUnexpectedException):
                self.factory.get_reader(self.file_name)


class TestPageCache(unittest.TestCase):
    def setUp(self):
        """Set up the PageCache with a mocked reader factory."""
        self.temp_dir = TemporaryDirectory()
        self.file_name = "testfile.wal"
        self.file_path = os.path.join(self.temp_dir.name, self.file_name)

        # Create a test file
        self.page_size = 8
        with open(self.file_path, "wb") as f:
            f.write(b"page1---page2---page3---page4---")

        self.factory = FileReaderFactory(path=self.temp_dir.name)
        self.cache = PageCache(capacity=2, page_size=self.page_size, reader_factory=self.factory)

    def tearDown(self):
        """Clean up temporary directory."""
        self.factory.close()
        self.temp_dir.cleanup()

    def test_read_page_cache_miss(self):
        """Test reading a page that is not in cache."""
        self.assertRaises(
            ProtoUnexpectedException,
            self.cache.read_page,
            uuid.uuid4(),
            0
        )

    def test_read_page_cache_hit(self):
        """Test reading a page that is already cached."""
        wal_id = self.file_path
        self.cache.read_page(wal_id, 0)  # Cache the page

    def test_cache_eviction(self):
        """Test eviction when capacity is exceeded."""
        wal_id = self.file_path
        self.cache.read_page(wal_id, 0)  # Add page 0
        self.cache.read_page(wal_id, 1)  # Add page 1
        self.cache.read_page(wal_id, 2)  # Add page 2, should evict page 0
        with self.cache._lock:
            self.assertNotIn((wal_id, 0), self.cache.cache)

    def test_read_page_invalid(self):
        """Test reading a nonexistent page should raise an exception."""
        with self.assertRaises(ProtoUnexpectedException):
            self.cache.read_page(uuid.uuid4(), 999)


class TestFileBlockProvider(unittest.TestCase):
    def setUp(self):
        """Set up the FileBlockProvider with a temporary directory."""
        self.temp_dir = TemporaryDirectory()
        self.provider = FileBlockProvider(space_path=self.temp_dir.name, maximun_cache_size=1024 * 1024, page_size=8)

        # Mock the setup of WAL files
        self.wal_id = uuid.uuid4()
        self.file_path = os.path.join(self.temp_dir.name, self.wal_id.hex)
        with open(self.file_path, "wb") as f:
            f.write(b"test wal data")

    def tearDown(self):
        """Clean up the temporary directory."""
        self.temp_dir.cleanup()

    def test_get_new_wal_creates_wal(self):
        """Test that a new WAL is created when no reusable WALs are available."""
        new_wal_id, size = self.provider.get_new_wal()
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir.name, new_wal_id.hex)))

    def test_get_reader_streamer(self):
        """Test retrieving a reader streamer."""
        reader = self.provider.get_reader(self.wal_id, 0)
        self.assertIsInstance(reader, io.BytesIO)

    def test_root_object_read(self):
        """Test reading the root object from storage."""
        root_data = {"key": "value"}
        root_path = os.path.join(self.temp_dir.name, "space_root")
        with open(root_path, "w") as f:
            json.dump(root_data, f)

        result = self.provider.get_current_root_object()
        self.assertEqual(result, root_data)

    def test_update_root_object(self):
        """Test updating the root object."""
        root_data = {"new_key": "new_value"}
        self.provider.update_root_object(root_data)
        root_path = os.path.join(self.temp_dir.name, "space_root")
        with open(root_path, "r") as f:
            self.assertEqual(json.load(f), root_data)

    def test_close_wal(self):
        """Test closing WAL files."""
        wal_id, size = self.provider.get_new_wal()
        self.provider.close_wal(wal_id)
        self.assertIsNone(self.provider.current_wal)

    def test_close_provider(self):
        """Test closing the provider."""
        self.provider.get_new_wal()
        self.provider.close()
        self.assertIsNone(self.provider.current_wal)


if __name__ == "__main__":
    unittest.main()
