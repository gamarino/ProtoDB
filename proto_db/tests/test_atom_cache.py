import threading
import time
import uuid
import unittest

from proto_db.atom_cache import AtomObjectCache, AtomBytesCache, AtomCacheBundle


class TestAtomCaches(unittest.TestCase):
    def test_object_cache_basic(self):
        cache = AtomObjectCache(max_entries=10, max_bytes=1024)
        tx = uuid.uuid4()
        cache.put(tx, 1, {"a": 1})
        self.assertTrue(cache.contains(tx, 1))
        obj = cache.get(tx, 1)
        self.assertEqual(obj, {"a": 1})
        stats = cache.stats()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 0)

    def test_bytes_cache_basic(self):
        cache = AtomBytesCache(max_entries=10, max_bytes=1024)
        tx = uuid.uuid4()
        b = b"hello"
        cache.put(tx, 2, b)
        self.assertTrue(cache.contains(tx, 2))
        mv = cache.get(tx, 2)
        self.assertEqual(bytes(mv), b)
        stats = cache.stats()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 0)

    def test_twoq_eviction_prefers_probation(self):
        # Small cache: 2 entries, ensure first gets evicted from probation before protected
        cache = AtomObjectCache(max_entries=2, max_bytes=1_000_000)
        tx = uuid.uuid4()
        cache.put(tx, 1, {"k": 1})  # probation: [1]
        cache.put(tx, 2, {"k": 2})  # probation: [1,2]
        # touch key=1 to promote to protected
        _ = cache.get(tx, 1)
        # Now insert third -> should evict from probation (key=2)
        cache.put(tx, 3, {"k": 3})
        self.assertTrue(cache.contains(tx, 1))
        self.assertFalse(cache.contains(tx, 2))
        self.assertTrue(cache.contains(tx, 3))

    def test_singleflight_dedup(self):
        bundle = AtomCacheBundle()
        tx = uuid.uuid4()
        key = (tx, 123, None)
        results = []

        def worker(i):
            ev = bundle.singleflight.begin(key)
            if ev is None:
                bundle.singleflight.wait(key)
                results.append("follower")
                return
            # leader work
            time.sleep(0.05)
            bundle.singleflight.done(key)
            results.append("leader")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # exactly one leader
        self.assertEqual(results.count("leader"), 1)
        self.assertEqual(results.count("follower"), 4)


if __name__ == '__main__':
    unittest.main()
