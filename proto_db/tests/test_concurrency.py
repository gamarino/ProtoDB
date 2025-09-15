import os
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from tempfile import TemporaryDirectory

from ..db_access import ObjectSpace
from ..exceptions import ProtoLockingException
from ..file_block_provider import FileBlockProvider
from ..standalone_file_storage import StandaloneFileStorage
from ..dictionaries import Dictionary, RepeatedKeysDictionary


class TestConcurrency(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.directory_name = "testDB"
        self.db_path = os.path.join(self.temp_dir.name, self.directory_name)
        os.mkdir(self.db_path)

        block_provider = FileBlockProvider(self.db_path)
        self.space = ObjectSpace(StandaloneFileStorage(block_provider))
        self.db = self.space.new_database("TestDB")

    def tearDown(self):
        self.space.close()
        self.temp_dir.cleanup()

    def _retrying_commit(self, func):
        # Helper to retry a function body in a new transaction when concurrent conflict occurs
        while True:
            try:
                func()
                return
            except ProtoLockingException:
                # Retry on concurrent update
                continue

    def test_101_concurrent_increment_dictionary(self):
        # Prepare root dictionary with counter=0
        tr = self.db.new_transaction()
        d = tr.new_dictionary()
        d = d.set_at("counter", 0)
        tr.set_root_object("counter_root", d)
        tr.commit()

        workers = 16

        def worker():
            def body():
                trw = self.db.new_transaction()
                curr = trw.get_root_object("counter_root")
                val = curr.get_at("counter")
                curr = curr.set_at("counter", int(val) + 1)
                trw.set_root_object("counter_root", curr)
                trw.commit()
            self._retrying_commit(body)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(worker) for _ in range(workers)]
            for f in as_completed(futures):
                f.result()

        # Verify
        tr2 = self.db.new_transaction()
        d2 = tr2.get_root_object("counter_root")
        self.assertEqual(int(d2.get_at("counter")), workers)
        tr2.commit()

    def test_102_concurrent_add_repeated_keys_dictionary(self):
        # Prepare root repeated-keys dictionary
        tr = self.db.new_transaction()
        rk = RepeatedKeysDictionary(transaction=tr)
        tr.set_root_object("rk_root", rk)
        tr.commit()

        workers = 12

        def worker(i):
            value = f"v{i}"

            def body():
                trw = self.db.new_transaction()
                curr = trw.get_root_object("rk_root")
                curr = curr.set_at("k", value)
                trw.set_root_object("rk_root", curr)
                trw.commit()
            self._retrying_commit(body)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(worker, i) for i in range(workers)]
            for f in as_completed(futures):
                f.result()

        # Verify bucket contents and counts
        tr2 = self.db.new_transaction()
        r2 = tr2.get_root_object("rk_root")
        bucket = r2.get_at("k")
        vals = set(v for v in bucket.as_iterable())
        self.assertEqual(vals, {f"v{i}" for i in range(workers)})
        # CountedSet.total_count should equal number of insertions
        self.assertEqual(bucket.total_count, workers)
        tr2.commit()
