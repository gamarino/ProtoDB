import unittest

from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import Expression, Term, ListPlan, WherePlan, Between


class TestBetweenOperator(unittest.TestCase):
    def setUp(self):
        self.space = ObjectSpace(storage=MemoryStorage())
        self.db = self.space.new_database('DB')
        self.tr = self.db.new_transaction()

    def test_between_match_inclusive_exclusive(self):
        op_inc = Between(include_lower=True, include_upper=True)
        op_exc = Between(include_lower=False, include_upper=False)
        # inclusive includes endpoints
        self.assertTrue(op_inc.match(10, (10, 20)))
        self.assertTrue(op_inc.match(20, (10, 20)))
        self.assertTrue(op_inc.match(15, (10, 20)))
        # exclusive excludes endpoints
        self.assertFalse(op_exc.match(10, (10, 20)))
        self.assertFalse(op_exc.match(20, (10, 20)))
        self.assertTrue(op_exc.match(15, (10, 20)))
        # lo > hi -> empty set (no matches)
        self.assertFalse(op_inc.match(15, (30, 20)))
        # strings
        self.assertTrue(op_inc.match('b', ('a', 'c')))
        self.assertFalse(op_exc.match('a', ('a', 'c')))

    def test_parser_three_components(self):
        expr = Expression.compile(['age', 'between[]', 10, 20])
        self.assertIsInstance(expr, Term)
        self.assertIsInstance(expr.operation, Between)
        self.assertEqual(expr.value, (10, 20))

    def test_where_linear_scan(self):
        data = [
            {'id': 1, 'age': 9},
            {'id': 2, 'age': 10},
            {'id': 3, 'age': 15},
            {'id': 4, 'age': 20},
            {'id': 5, 'age': 21},
        ]
        base = ListPlan(base_list=data, transaction=self.tr)
        plan = WherePlan(filter=Expression.compile(['age', 'between()', 10, 20]), based_on=base, transaction=self.tr)
        out = list(plan.execute())
        # 10 and 20 excluded
        self.assertEqual([r['id'] for r in out], [3])


if __name__ == '__main__':
    unittest.main()