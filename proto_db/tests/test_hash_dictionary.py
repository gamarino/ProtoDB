import unittest
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.hash_dictionaries import HashDictionary


class TestHashDictionary(unittest.TestCase):
    def setUp(self):
        self.space = ObjectSpace(MemoryStorage())
        self.db = self.space.new_database('HDTest')
        self.tr = self.db.new_transaction()

    def tearDown(self):
        try:
            self.tr.abort()
        except Exception:
            pass
        self.space.close()

    def test_set_get_has_and_iteration_order(self):
        hd = HashDictionary(transaction=self.tr)
        # Insert keys out of order
        for k in [5, 1, 3, 2, 4]:
            hd = hd.set_at(k, f"v{k}")
        # Basic get/has
        self.assertTrue(hd.has(3))
        self.assertEqual(hd.set_at(3), "v3")
        self.assertFalse(hd.has(99))
        self.assertIsNone(hd.set_at(99))
        # Iteration should be in key order
        keys = [k for k, _ in hd.as_iterable()]
        self.assertEqual(keys, [1, 2, 3, 4, 5])

    def test_replace_existing_key(self):
        hd = HashDictionary(transaction=self.tr)
        hd = hd.set_at(10, "a")
        hd = hd.set_at(10, "b")  # replace
        self.assertTrue(hd.has(10))
        self.assertEqual(hd.get_at(10), "b")

    def test_remove_leaf_and_promotions(self):
        hd = HashDictionary(transaction=self.tr)
        for k in [10, 5, 15, 3, 7, 12, 18]:
            hd = hd.set_at(k, str(k))
        # Remove a leaf
        hd = hd.remove_at(3)
        self.assertFalse(hd.has(3))
        # Remove a node with right child -> should promote successor
        self.assertTrue(hd.has(10))
        old_inorder = [k for k, _ in hd.as_iterable()]
        hd = hd.remove_at(10)
        self.assertFalse(hd.has(10))
        # In-order property preserved
        self.assertEqual([k for k, _ in hd.as_iterable()], sorted([k for k in old_inorder if k != 10]))

    def test_remove_to_empty(self):
        hd = HashDictionary(transaction=self.tr)
        hd = hd.set_at(1, 'x')
        hd = hd.remove_at(1)
        # Should return an explicit empty HashDictionary, not None
        self.assertIsInstance(hd, HashDictionary)
        self.assertFalse(hd.has(1))
        self.assertEqual(hd.count, 0)
        # Iteration should be empty
        self.assertEqual(list(hd.as_iterable()), [])

    def test_balance_invariant_basic(self):
        # Insert a sequence that forces rotations
        hd = HashDictionary(transaction=self.tr)
        for k in [30, 20, 40, 10, 25, 35, 50, 5, 15, 22, 27, 33, 37, 45, 55]:
            hd = hd.set_at(k, k)
        # Quick check that balance of the root is within AVL bounds [-1, 1]
        bal = hd._balance()
        self.assertGreaterEqual(bal, -1)
        self.assertLessEqual(bal, 1)
        # Removing several nodes keeps structure valid and queryable
        for k in [10, 30, 37, 5, 55]:
            hd = hd.remove_at(k)
        # In-order keys should be sorted and contain expected survivors
        keys = [k for k, _ in hd.as_iterable()]
        self.assertEqual(keys, sorted(keys))


if __name__ == '__main__':
    unittest.main()
