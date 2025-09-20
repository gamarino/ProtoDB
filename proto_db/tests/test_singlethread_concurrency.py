import os
import unittest
from tempfile import TemporaryDirectory

from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.file_block_provider import FileBlockProvider
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.dictionaries import Dictionary
from proto_db.exceptions import ProtoLockingException


class TestSingleThreadInterleaved(unittest.TestCase):
    """
    Deterministic, single-thread tests that interleave two transactions to reproduce
    the concurrency reconciliation failures without relying on thread timing or
    OS-level caching. These tests act as a diagnostic harness.

    Notes:
    - The MemoryStorage variant eliminates filesystem/cache effects to isolate logic.
    - The FileBlockProvider variant exercises the file-backed path in a single thread.
    - They are marked as expectedFailure to document the current bug without breaking
      the rest of the test suite. Once fixed, the decorators can be removed.
    """

    def _interleaved_increment(self, space: ObjectSpace):
        db = space.new_database("STDB")

        # Prepare root counter=0 using numeric keys to avoid creating new literals per transaction
        tr0 = db.new_transaction()
        d0 = Dictionary(transaction=tr0)
        d0 = d0.set_at(1, 0)
        tr0.set_root_object(1, d0)
        tr0.commit()

        # Interleave two transactions in a single thread
        tr1 = db.new_transaction()
        r1 = tr1.get_root_object(1)
        v1 = r1.get_at(1)
        r1 = r1.set_at(1, int(v1) + 1)

        tr2 = db.new_transaction()
        r2 = tr2.get_root_object(1)
        v2 = r2.get_at(1)
        r2 = r2.set_at(1, int(v2) + 1)

        # Commit tr1, then tr2 (classic interleaving scenario)
        tr1.set_root_object(1, r1)
        tr1.commit()

        # In presence of correct reconciliation, tr2 should rebase to +1 on fresh current
        tr2.set_root_object(1, r2)
        try:
            tr2.commit()
        except ProtoLockingException:
            # Allow retry once in single-thread to see if logic self-recovers
            tr2 = db.new_transaction()
            r2b = tr2.get_root_object(1)
            v2b = r2b.get_at(1)
            r2b = r2b.set_at(1, int(v2b) + 1)
            tr2.set_root_object(1, r2b)
            tr2.commit()

        # Check final value
        trf = db.new_transaction()
        df = trf.get_root_object(1)
        return int(df.get_at(1))

    def test_090_interleaved_increment_memory(self):
        """Single-thread, interleaved two transactions over MemoryStorage. Expected final=2."""
        space = ObjectSpace(MemoryStorage())
        try:
            final = self._interleaved_increment(space)
            self.assertEqual(final, 2)
        finally:
            space.close()

    def test_091_interleaved_increment_file(self):
        """Single-thread, interleaved two transactions over FileBlockProvider storage. Expected final=2."""
        with TemporaryDirectory() as tmp:
            dbdir = os.path.join(tmp, "space")
            os.mkdir(dbdir)
            space = ObjectSpace(StandaloneFileStorage(FileBlockProvider(dbdir)))
            try:
                final = self._interleaved_increment(space)
                self.assertEqual(final, 2)
            finally:
                space.close()


if __name__ == '__main__':
    unittest.main()
