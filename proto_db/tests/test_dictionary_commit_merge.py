import os
import unittest
from tempfile import TemporaryDirectory

from ..db_access import ObjectSpace
from ..file_block_provider import FileBlockProvider
from ..memory_storage import MemoryStorage
from ..standalone_file_storage import StandaloneFileStorage


class DictionaryCommitMergeMixin:
    """
    Committing a Dictionary root must store exactly what the transaction wrote:
    keys it did not touch keep their values, and removals are persisted.
    """

    def make_space(self) -> ObjectSpace:
        raise NotImplementedError

    def setUp(self):
        self.space = self.make_space()
        self.db = self.space.new_database("MergeDB")
        tr = self.db.new_transaction()
        d = tr.new_dictionary()
        d = d.set_at("a", 1).set_at("b", 5).set_at("flag", False).set_at("gone", 7).set_at("s", "x")
        tr.set_root_object("d", d)
        tr.commit()

    def tearDown(self):
        self.space.close()

    def _update(self, fn):
        tr = self.db.new_transaction()
        d = fn(tr.get_root_object("d"))
        tr.set_root_object("d", d)
        tr.commit()

    def _read(self, key):
        tr = self.db.new_transaction()
        value = tr.get_root_object("d").get_at(key)
        tr.abort()
        return value

    def test_untouched_keys_keep_their_values(self):
        for i in range(3):
            self._update(lambda d, i=i: d.set_at("s", f"y{i}"))
        self.assertEqual(self._read("a"), 1)
        self.assertEqual(self._read("b"), 5)
        self.assertIs(self._read("flag"), False)

    def test_integer_write_stores_the_written_value(self):
        self._update(lambda d: d.set_at("a", 0))
        self.assertEqual(self._read("a"), 0)
        self._update(lambda d: d.set_at("b", 70))
        self.assertEqual(self._read("b"), 70)

    def test_removal_is_persisted(self):
        self._update(lambda d: d.remove_at("gone"))
        self.assertIsNone(self._read("gone"))
        self.assertEqual(self._read("a"), 1)

    def test_blind_write_merges_only_its_own_keys(self):
        # A concurrent writer commits after our snapshot was taken
        self._update(lambda d: d.set_at("a", 10))
        # A blind write (root never read in this transaction) carries only key "c"
        tr = self.db.new_transaction()
        tr.set_root_object("d", tr.new_dictionary().set_at("c", 3))
        tr.commit()
        self.assertEqual(self._read("a"), 10)
        self.assertEqual(self._read("b"), 5)
        self.assertEqual(self._read("c"), 3)


class TestDictionaryCommitMergeMemory(DictionaryCommitMergeMixin, unittest.TestCase):
    def make_space(self) -> ObjectSpace:
        return ObjectSpace(MemoryStorage())


class TestDictionaryCommitMergeFile(DictionaryCommitMergeMixin, unittest.TestCase):
    def make_space(self) -> ObjectSpace:
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        path = os.path.join(self._tmp.name, "space")
        os.mkdir(path)
        return ObjectSpace(StandaloneFileStorage(FileBlockProvider(path)))


if __name__ == "__main__":
    unittest.main()
