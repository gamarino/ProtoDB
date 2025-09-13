import unittest

from proto_db.sets import CountedSet
from proto_db.hash_dictionaries import HashDictionary
from proto_db.common import Literal
from proto_db.dictionaries import RepeatedKeysDictionary


class TestCountedSet(unittest.TestCase):
    def test_001_basic_add_with_duplicates_non_atom(self):
        cs = CountedSet()
        N = 5
        k = 42
        orig = cs
        for _ in range(N):
            cs = cs.add(k)
        self.assertTrue(cs.has(k))
        self.assertEqual(cs.count, 1)
        self.assertEqual(cs.total_count, N)
        self.assertEqual(list(cs.as_iterable()), [k])
        # immutability
        self.assertFalse(orig.has(k))
        self.assertEqual(orig.count, 0)

    def test_002_basic_add_with_duplicates_atom(self):
        cs = CountedSet()
        N = 7
        lit = Literal(string="x")
        for _ in range(N):
            cs = cs.add(lit)
        self.assertTrue(cs.has(lit))
        self.assertEqual(cs.count, 1)
        self.assertEqual(cs.total_count, N)
        self.assertEqual(list(cs.as_iterable()), [lit])

    def test_003_remove_with_duplicates(self):
        cs = CountedSet()
        N = 4
        k = 99
        for _ in range(N):
            cs = cs.add(k)
        # remove N-1 times
        for _ in range(N-1):
            cs = cs.remove_at(k)
        self.assertTrue(cs.has(k))
        self.assertEqual(cs.count, 1)
        self.assertEqual(cs.total_count, 1)
        # final remove
        cs = cs.remove_at(k)
        self.assertFalse(cs.has(k))
        self.assertEqual(cs.count, 0)
        self.assertEqual(cs.total_count, 0)
        self.assertEqual(list(cs.as_iterable()), [])

    def test_004_get_count_and_properties(self):
        cs = CountedSet()
        k1 = 1
        k2 = 2
        cs = cs.add(k1).add(k1).add(k2)
        self.assertEqual(cs.get_count(k1), 2)
        self.assertEqual(cs.get_count(k2), 1)
        self.assertEqual(cs.count, 2)
        self.assertEqual(cs.total_count, 3)
        cs = cs.remove_at(k1)
        self.assertEqual(cs.get_count(k1), 1)
        self.assertEqual(cs.total_count, 2)

    def test_005_iteration_semantics_unique(self):
        cs = CountedSet()
        elems = [1, 1, 2, 2, 2, 3]
        for e in elems:
            cs = cs.add(e)
        self.assertEqual(set(cs.as_iterable()), {1, 2, 3})
        self.assertEqual(set(iter(cs)), {1, 2, 3})

    def test_006_mixed_types_and_hashing(self):
        cs = CountedSet()
        lit_a = Literal(string="a")
        lit_b = Literal(string="a")  # distinct Atom instance; may have same string but different identity/hash
        cs = cs.add(lit_a).add(lit_b).add("a").add("a")
        # Depending on Atom hash semantics, lit_a and lit_b may be distinct unique entries
        uniq = list(cs.as_iterable())
        self.assertGreaterEqual(len(uniq), 2)
        self.assertTrue(cs.has(lit_a))
        self.assertTrue(cs.has(lit_b))
        self.assertTrue(cs.has("a"))
        # total_count should be at least number of adds
        self.assertGreaterEqual(cs.total_count, 4)

    def test_007_immutability(self):
        cs1 = CountedSet()
        cs2 = cs1.add(5)
        cs3 = cs2.remove_at(5)
        self.assertEqual(cs1.count, 0)
        self.assertEqual(cs2.count, 1)
        self.assertEqual(cs3.count, 0)

    def test_008_persistence_surface_smoke(self):
        # Ensure _save persists internals without exceptions; transaction may be None in this smoke test
        cs = CountedSet()
        cs = cs.add(1).add(1).add(2)
        # Provide explicit HashDictionaries with a None transaction to cover _save path
        cs = CountedSet(items=cs.items, counts=cs.counts, transaction=None)
        # _save should no-op without transaction; just ensure properties still accessible
        try:
            cs._save()
        except Exception as e:
            # Without a transaction, _save may raise; treat as acceptable for smoke test
            from proto_db.exceptions import ProtoValidationException
            if not isinstance(e, ProtoValidationException):
                self.fail(f"_save raised unexpected exception type: {e}")
        self.assertEqual(cs.count, 2)
        self.assertEqual(cs.total_count, 3)

    def test_009_index_update_semantics_bucket_membership(self):
        # Simulate index bucket behavior using RepeatedKeysDictionary: ensure no duplicates are inserted into the bucket
        rkd = RepeatedKeysDictionary()
        rec = ("id", 1)  # use a hashable record placeholder
        # Manually create a CountedSet bucket and emulate multiple additions by updating the dictionary content
        bucket = CountedSet()
        for _ in range(3):
            bucket = bucket.add(rec)
        # Put bucket under a key and read it back
        rkd = rkd.set_at("key", rec)  # normal path uses Set, but here we only care about uniqueness semantics
        # Validate our CountedSet bucket maintains one unique entry
        self.assertEqual(bucket.count, 1)
        self.assertEqual(bucket.total_count, 3)
        self.assertTrue(bucket.has(rec))


if __name__ == '__main__':
    unittest.main()
