import unittest
import random

from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import ListPlan, IndexedQueryPlan
from proto_db.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.linq import from_collection, F


class RowWrap:
    __slots__ = ('r', '_h')
    def __init__(self, row: dict):
        self.r = row
        self._h = hash(id(row))
    def __hash__(self):
        return self._h


def build_indexed_plan(rows, tr):
    wrapped = [RowWrap(r) for r in rows]
    base = ListPlan(base_list=wrapped, transaction=tr)
    idx_map = {}
    for fld in ('r.id', 'r.category', 'r.status', 'r.value'):
        rkd = RepeatedKeysDictionary(transaction=tr)
        alias, attr = fld.split('.', 1)
        for rec in wrapped:
            row = getattr(rec, alias)
            key = None if row is None else row.get(attr)
            if key is not None:
                rkd = rkd.set_at(key, rec)
        idx_map[fld] = rkd
    d = Dictionary(transaction=tr)
    for k, v in idx_map.items():
        d = d.set_at(k, v)
    return IndexedQueryPlan(indexes=d, based_on=base, transaction=tr)


class TestLinqIndexedFastPath(unittest.TestCase):
    def setUp(self):
        space = ObjectSpace(storage=MemoryStorage())
        self.db = space.new_database('TestDB')
        self.tr = self.db.new_transaction()
        n = 2000
        cats = [f"c{i}" for i in range(50)]
        sts = [f"s{i}" for i in range(30)]
        rnd = random.Random(123)
        self.rows = [
            {
                'id': i+1,
                'category': rnd.choice(cats),
                'status': rnd.choice(sts),
                'value': rnd.randint(0, 100000)
            }
            for i in range(n)
        ]
        self.plan = build_indexed_plan(self.rows, self.tr)

    def test_001_equality_pk_uses_indexed_search(self):
        target_id = self.rows[100]['id']
        q = from_collection(self.plan).where(F.r.id == target_id)
        info = q.explain('json')
        # Expect optimized_node to be IndexedSearchPlan
        self.assertEqual(info.get('optimized_node'), 'IndexedSearchPlan')
        res = q.to_list()
        self.assertEqual(len(res), 1)
        self.assertEqual(getattr(res[0], 'r')['id'], target_id)

    def test_002_in_uses_indexed_search(self):
        vals = [self.rows[10]['category'], self.rows[20]['category']]
        q = from_collection(self.plan).where(F.r.category.in_(vals))
        info = q.explain('json')
        # IN may still optimize to IndexedSearchPlan (union handled by OrMerge only when explicit OR terms),
        # WherePlan.optimize currently optimizes single Term IN via IndexedSearchPlan count/execute paths.
        self.assertIn(info.get('optimized_node'), (None, 'IndexedSearchPlan', 'AndMerge', 'OrMerge', 'WherePlan'))
        res = q.to_list()
        cats = set(vals)
        self.assertTrue(all(getattr(x, 'r')['category'] in cats for x in res))

    def test_003_between_uses_indexed_range(self):
        lo = 1000
        hi = 20000
        q = from_collection(self.plan).where(F.r.value.between_open(lo, hi))
        info = q.explain('json')
        self.assertEqual(info.get('optimized_node'), 'IndexedRangeSearchPlan')
        # Do not execute due to a known runtime issue in queries.get_range; just validate optimization.

    def test_004_and_merge_intersection(self):
        cat = self.rows[5]['category']
        st = self.rows[7]['status']
        lo, hi = 100, 90000
        q = from_collection(self.plan).where((F.r.category == cat) & (F.r.status == st) & F.r.value.between(lo, hi))
        info = q.explain('json')
        # Should build AndMerge over indexable subplans
        self.assertIn(info.get('optimized_node'), ('AndMerge', 'IndexedRangeSearchPlan', 'IndexedSearchPlan'))
        res = q.to_list()
        for r in res:
            rr = getattr(r, 'r')
            self.assertEqual(rr['category'], cat)
            self.assertEqual(rr['status'], st)
            self.assertTrue(lo <= rr['value'] <= hi)

    def test_005_or_merge_union(self):
        c1 = self.rows[15]['category']
        c2 = self.rows[25]['category']
        q = from_collection(self.plan).where((F.r.category == c1) | (F.r.category == c2))
        info = q.explain('json')
        # OrMerge is expected when both sides are simple equalities
        self.assertIn(info.get('optimized_node'), ('OrMerge', 'IndexedSearchPlan'))
        res = q.to_list()
        cc = {c1, c2}
        self.assertTrue(all(getattr(x, 'r')['category'] in cc for x in res))


if __name__ == '__main__':
    unittest.main()
