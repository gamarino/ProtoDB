import unittest

from proto_db.vectors import Vector, cosine_similarity
from proto_db.vector_index import ExactVectorIndex
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import ListPlan, WherePlan, Expression


class TestVectorBasics(unittest.TestCase):
    def test_vector_create_normalize_serialize(self):
        v = Vector.from_list([3.0, 4.0], normalize=True)
        self.assertTrue(v.normalized)
        self.assertAlmostEqual(sum(x * x for x in v.data), 1.0, places=6)
        b = v.to_bytes()
        v2 = Vector.from_bytes(b)
        self.assertEqual(v.dim, v2.dim)
        self.assertEqual(v.normalized, v2.normalized)
        self.assertEqual(v.to_list(), v2.to_list())

    def test_cosine_similarity(self):
        a = [1.0, 0.0, 0.0]
        b = [1.0, 0.0, 0.0]
        c = [0.0, 1.0, 0.0]
        self.assertAlmostEqual(cosine_similarity(a, b), 1.0, places=6)
        self.assertAlmostEqual(cosine_similarity(a, c), 0.0, places=6)


class TestExactVectorIndex(unittest.TestCase):
    def test_build_and_search(self):
        idx = ExactVectorIndex(metric='cosine')
        vecs = [[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]]
        ids = ['a', 'b', 'c']
        idx.build(vecs, ids, metric='cosine')
        res = idx.search([1.0, 0.0], k=2)
        self.assertEqual(res[0][0], 'a')  # nearest to [1,0]
        # range search with high threshold should only include 'a'
        res2 = idx.range_search([1.0, 0.0], threshold=0.95)
        ids2 = [r[0] for r in res2]
        self.assertIn('a', ids2)
        self.assertNotIn('b', ids2)


class TestNearOperator(unittest.TestCase):
    def setUp(self):
        self.space = ObjectSpace(storage=MemoryStorage())
        self.db = self.space.new_database('DB')
        self.tr = self.db.new_transaction()

    def test_where_with_near_operator_linear(self):
        rows = [
            {'id': 1, 'emb': [1.0, 0.0]},
            {'id': 2, 'emb': [0.0, 1.0]},
            {'id': 3, 'emb': [0.7, 0.7]},
        ]
        base = ListPlan(base_list=rows, transaction=self.tr)
        # cosine near threshold 0.8 to [1,0] should match id 1 only
        expr = Expression.compile(['emb', 'near[]', [1.0, 0.0], 0.8])
        plan = WherePlan(filter=expr, based_on=base, transaction=self.tr)
        out = list(plan.execute())
        self.assertEqual([r['id'] for r in out], [1])


if __name__ == '__main__':
    unittest.main()
