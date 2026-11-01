import os
import subprocess
import sys
import textwrap
import unittest
import uuid
from tempfile import TemporaryDirectory

from ..db_access import ObjectSpace
from ..file_block_provider import FileBlockProvider, FileReaderFactory, PageCache
from ..standalone_file_storage import StandaloneFileStorage

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Commits two transactions, then dies without closing the storage, as a crash would.
WRITER = textwrap.dedent("""
    import os, sys
    from proto_db.db_access import ObjectSpace
    from proto_db.file_block_provider import FileBlockProvider
    from proto_db.standalone_file_storage import StandaloneFileStorage

    space = ObjectSpace(StandaloneFileStorage(FileBlockProvider(sys.argv[1])))
    db = space.new_database("CrashDB")
    tr = db.new_transaction()
    tr.set_root_object("first", tr.new_list().append_last(1).append_last(2))
    tr.commit()
    tr = db.new_transaction()
    tr.set_root_object("second", tr.new_dictionary().set_at("k", "v"))
    tr.commit()
    os._exit(0)
""")


class TestWalDurability(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = os.path.join(self._tmp.name, "space")
        os.mkdir(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def test_committed_data_survives_a_crash(self):
        env = dict(os.environ, PYTHONPATH=PROJECT_ROOT)
        subprocess.run([sys.executable, "-c", WRITER, self.path], check=True, env=env, timeout=120)

        space = ObjectSpace(StandaloneFileStorage(FileBlockProvider(self.path)))
        try:
            tr = space.open_database("CrashDB").new_transaction()
            self.assertEqual(list(tr.get_root_object("first").as_iterable()), [1, 2])
            self.assertEqual(str(tr.get_root_object("second").get_at("k")), "v")
            tr.abort()
        finally:
            space.close()

    def test_data_committed_after_a_read_is_readable(self):
        space = ObjectSpace(StandaloneFileStorage(FileBlockProvider(self.path)))
        try:
            db = space.new_database("ReadDB")
            tr = db.new_transaction()
            tr.set_root_object("first", tr.new_list().append_last(1))
            tr.commit()
            tr = db.new_transaction()
            self.assertEqual(list(tr.get_root_object("first").as_iterable()), [1])
            tr.abort()
            tr = db.new_transaction()
            tr.set_root_object("second", tr.new_list().append_last(2).append_last(3))
            tr.commit()
            tr = db.new_transaction()
            self.assertEqual(list(tr.get_root_object("second").as_iterable()), [2, 3])
            tr.abort()
        finally:
            space.close()


class TestPageCacheGrowingFile(unittest.TestCase):
    def test_page_read_at_end_of_file_sees_later_appends(self):
        with TemporaryDirectory() as tmp:
            wal_id = uuid.uuid4()
            file_path = os.path.join(tmp, str(wal_id))
            with open(file_path, "wb") as f:
                f.write(b"abc")
            factory = FileReaderFactory(path=tmp)
            cache = PageCache(capacity=4, page_size=8, reader_factory=factory)
            try:
                self.assertEqual(cache.read_page(wal_id, 0), b"abc")
                with open(file_path, "ab") as f:
                    f.write(b"defgh")
                self.assertEqual(cache.read_page(wal_id, 0), b"abcdefgh")
            finally:
                factory.close()


if __name__ == "__main__":
    unittest.main()
