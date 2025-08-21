import unittest

from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import (
    Expression, NotTrue, TrueTerm, OrExpression, Term, Equal,
    ListPlan, WherePlan, UnnestPlan, CollectionFieldPlan
)


class TestFixesAndCollections(unittest.TestCase):
    def setUp(self):
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()

    def test_expression_compile_unary_and_binary(self):
        # Binary
        expr = Expression.compile(['id', '==', 1])
        self.assertIsInstance(expr, Term)
        self.assertIsInstance(expr.operation, Equal)
        self.assertEqual(expr.target_attribute, 'id')
        self.assertEqual(expr.value, 1)
        # Unary true
        expr_t = Expression.compile(['active', '?T'])
        # Should evaluate true for record with active True
        record = {'active': True}
        self.assertTrue(expr_t.match(record))
        # Unary not none
        expr_n = Expression.compile(['x', '?!N'])
        self.assertTrue(expr_n.match({'x': 1}))
        self.assertFalse(expr_n.match({'x': None}))

    def test_not_true_operator(self):
        op = NotTrue()
        self.assertTrue(op.match(False))
        self.assertFalse(op.match(True))

    def test_or_merge_execute_and_count(self):
        # Build two lists with overlap to test dedup in count
        a = ListPlan(base_list=[1, 2, 3], transaction=self.transaction)
        b = ListPlan(base_list=[3, 4], transaction=self.transaction)
        from proto_db.queries import OrMerge
        plan = OrMerge(or_queries=[a, b], transaction=self.transaction)
        items = list(plan.execute())
        self.assertEqual(items, [1, 2, 3, 3, 4])  # execution yields union stream
        # count should be unique ids via keys_iterator; but fallback to execute if not available
        cnt = plan.count()
        self.assertEqual(cnt, len(set(items)))

    def test_unnest_plan(self):
        base = ListPlan(base_list=[{'u': {'tags': ['a', 'b']}}, {'u': {'tags': []}}, {'u': {'tags': None}}], transaction=self.transaction)
        plan = UnnestPlan('u.tags', 'tag', based_on=base, transaction=self.transaction)
        rows = list(plan.execute())
        self.assertEqual(rows, [
            {'u': {'tags': ['a', 'b']}, 'tag': 'a'},
            {'u': {'tags': ['a', 'b']}, 'tag': 'b'}
        ])
        # Without alias, the element should replace the record
        plan2 = UnnestPlan('u.tags', None, based_on=base, transaction=self.transaction)
        rows2 = list(plan2.execute())
        self.assertEqual(rows2, ['a', 'b'])

    def test_collection_field_plan(self):
        users = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        orders = [
            {'id': 10, 'userId': 1},
            {'id': 11, 'userId': 1},
            {'id': 12, 'userId': 2},
        ]
        users_plan = ListPlan(base_list=users, transaction=self.transaction)
        orders_plan = ListPlan(base_list=orders, transaction=self.transaction)
        # Builder: returns a WherePlan filtering orders for each user
        def build_for_user(u_rec):
            # Filter by equality
            return WherePlan(filter=Expression.compile(['userId', '==', u_rec['id']]), based_on=orders_plan, transaction=self.transaction)
        coll = CollectionFieldPlan('orders', build_for_user, based_on=users_plan, transaction=self.transaction)
        out = list(coll.execute())
        self.assertEqual(out, [
            {'id': 1, 'name': 'Alice', 'orders': [
                {'id': 10, 'userId': 1},
                {'id': 11, 'userId': 1},
            ]},
            {'id': 2, 'name': 'Bob', 'orders': [
                {'id': 12, 'userId': 2},
            ]},
        ])


if __name__ == '__main__':
    unittest.main()
