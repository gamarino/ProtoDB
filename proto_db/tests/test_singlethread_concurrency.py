import os
import unittest
from tempfile import TemporaryDirectory

from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.file_block_provider import FileBlockProvider
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.dictionaries import Dictionary, RepeatedKeysDictionary
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

    def _interleaved_increment_strkeys(self, space: ObjectSpace):
        db = space.new_database("STDB_STR")
        tr0 = db.new_transaction()
        d0 = Dictionary(transaction=tr0)
        d0 = d0.set_at("counter", 0)
        tr0.set_root_object("counter_root", d0)
        tr0.commit()

        # Interleave two transactions
        tr1 = db.new_transaction()
        r1 = tr1.get_root_object("counter_root")
        v1 = r1.get_at("counter")
        r1 = r1.set_at("counter", int(v1) + 1)

        tr2 = db.new_transaction()
        r2 = tr2.get_root_object("counter_root")
        v2 = r2.get_at("counter")
        r2 = r2.set_at("counter", int(v2) + 1)

        tr1.set_root_object("counter_root", r1)
        tr1.commit()

        tr2.set_root_object("counter_root", r2)
        try:
            tr2.commit()
        except ProtoLockingException:
            tr2 = db.new_transaction()
            r2b = tr2.get_root_object("counter_root")
            v2b = r2b.get_at("counter")
            r2b = r2b.set_at("counter", int(v2b) + 1)
            tr2.set_root_object("counter_root", r2b)
            tr2.commit()

        trf = db.new_transaction()
        df = trf.get_root_object("counter_root")
        return int(df.get_at("counter"))

    def test_092_interleaved_increment_memory_strkeys(self):
        space = ObjectSpace(MemoryStorage())
        try:
            final = self._interleaved_increment_strkeys(space)
            self.assertEqual(final, 2)
        finally:
            space.close()

    def test_093_interleaved_increment_file_strkeys(self):
        with TemporaryDirectory() as tmp:
            dbdir = os.path.join(tmp, "space")
            os.mkdir(dbdir)
            space = ObjectSpace(StandaloneFileStorage(FileBlockProvider(dbdir)))
            try:
                final = self._interleaved_increment_strkeys(space)
                self.assertEqual(final, 2)
            finally:
                space.close()

    def _interleaved_many_increments(self, space: ObjectSpace, workers: int) -> int:
        db = space.new_database("STDBM")
        # Initialize counter=0 at key 1 in root 1
        tr0 = db.new_transaction()
        d0 = Dictionary(transaction=tr0)
        d0 = d0.set_at(1, 0)
        tr0.set_root_object(1, d0)
        tr0.commit()

        # Stage N transactions: all read before any commit, then commit sequentially
        txs = []
        staged = []
        for _ in range(workers):
            tr = db.new_transaction()
            r = tr.get_root_object(1)
            v = r.get_at(1)
            r = r.set_at(1, int(v) + 1)
            txs.append(tr)
            staged.append(r)

        # Commit sequentially; allow at most one retry per transaction if conflict detected
        for i, tr in enumerate(txs):
            tr.set_root_object(1, staged[i])
            try:
                tr.commit()
            except ProtoLockingException:
                tr = db.new_transaction()
                r2 = tr.get_root_object(1)
                v2 = r2.get_at(1)
                r2 = r2.set_at(1, int(v2) + 1)
                tr.set_root_object(1, r2)
                tr.commit()

        # Read final
        trf = db.new_transaction()
        df = trf.get_root_object(1)
        return int(df.get_at(1))

    def test_095_interleaved_many_increments_memory(self):
        workers = int(os.environ.get('PB_ST_MANY', '6'))
        space = ObjectSpace(MemoryStorage())
        try:
            final = self._interleaved_many_increments(space, workers)
            self.assertEqual(final, workers)
        finally:
            space.close()

    def test_096_interleaved_many_increments_file(self):
        workers = int(os.environ.get('PB_ST_MANY', '6'))
        with TemporaryDirectory() as tmp:
            dbdir = os.path.join(tmp, "space")
            os.mkdir(dbdir)
            space = ObjectSpace(StandaloneFileStorage(FileBlockProvider(dbdir)))
            try:
                final = self._interleaved_many_increments(space, workers)
                self.assertEqual(final, workers)
            finally:
                space.close()

    def _interleaved_many_repeated_keys(self, space: ObjectSpace, workers: int) -> int:
        db = space.new_database("STDBR")
        tr0 = db.new_transaction()
        rk = RepeatedKeysDictionary(transaction=tr0)
        tr0.set_root_object(1, rk)
        tr0.commit()

        # Stage N transactions that each add a unique value to the same key "k"
        txs = []
        staged = []
        for i in range(workers):
            tr = db.new_transaction()
            r = tr.get_root_object(1)
            r = r.set_at("k", f"v{i}")
            txs.append(tr)
            staged.append(r)

        # Commit sequentially with a single retry on conflict
        for i, tr in enumerate(txs):
            tr.set_root_object(1, staged[i])
            try:
                tr.commit()
            except ProtoLockingException:
                tr = db.new_transaction()
                r2 = tr.get_root_object(1)
                r2 = r2.set_at("k", f"v{i}")
                tr.set_root_object(1, r2)
                tr.commit()

        trf = db.new_transaction()
        rf = trf.get_root_object(1)
        bucket = rf.get_at("k")
        return bucket.count if bucket else 0

    def test_097_interleaved_many_repeated_keys_memory(self):
        workers = int(os.environ.get('PB_ST_MANY', '8'))
        space = ObjectSpace(MemoryStorage())
        try:
            total = self._interleaved_many_repeated_keys(space, workers)
            self.assertEqual(total, workers)
        finally:
            space.close()

    def test_098_interleaved_many_repeated_keys_file(self):
        workers = int(os.environ.get('PB_ST_MANY', '8'))
        with TemporaryDirectory() as tmp:
            dbdir = os.path.join(tmp, "space")
            os.mkdir(dbdir)
            space = ObjectSpace(StandaloneFileStorage(FileBlockProvider(dbdir)))
            try:
                total = self._interleaved_many_repeated_keys(space, workers)
                self.assertEqual(total, workers)
            finally:
                space.close()


    if __name__ == '__main__':
        unittest.main()
