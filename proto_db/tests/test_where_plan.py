import unittest
import uuid
from unittest.mock import MagicMock

from proto_db.common import AtomPointer, DBObject
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import (
    WherePlan, ListPlan, Term, AndExpression, OrExpression, NotExpression,
    Equal, NotEqual, Greater, GreaterOrEqual, Lower, LowerOrEqual, OrMerge, IndexedQueryPlan
)
from proto_db.dictionaries import Dictionary, RepeatedKeysDictionary


class TestWherePlan(unittest.TestCase):
    """
    Unit test class for WherePlan.

    This test class verifies various behaviors of the WherePlan class,
    ensuring that it correctly filters records based on different expressions.
    """

    def setUp(self):
        """
        Set up common resources for the tests.

        This includes creating base data (a list of DBObject instances) and
        mock dependencies for the WherePlan, such as transaction and atom pointer.
        """
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()
        self.atom_pointer = AtomPointer(transaction_id=uuid.uuid4(), offset=0)

        # Create test data
        self.mock_data = []
        for i in range(1, 6):
            obj = DBObject(transaction=self.transaction)
            obj = obj._setattr('id', i)
            obj = obj._setattr('name', f'Person {i}')
            obj = obj._setattr('age', 20 + i * 2)
            obj = obj._setattr('active', i % 2 == 0)  # Even IDs are active
            self.mock_data.append(obj)

        self.base_plan = ListPlan(base_list=self.mock_data, transaction=self.transaction)

    def test_simple_term_filter(self):
        """
        Test WherePlan with a simple Term expression.

        Verify that records are correctly filtered based on a single condition.
        """
        # Filter: id == 3
        term = Term('id', Equal(), 3)
        where_plan = WherePlan(filter=term, based_on=self.base_plan, transaction=self.transaction)

        result = list(where_plan.execute())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, 3)
        self.assertEqual(result[0].name, 'Person 3')

    def test_and_expression_filter(self):
        """
        Test WherePlan with an AndExpression.

        Verify that records are correctly filtered when multiple conditions must all be true.
        """
        # Filter: age > 25 AND active == True
        term1 = Term('age', Greater(), 25)
        term2 = Term('active', Equal(), True)
        and_expr = AndExpression([term1, term2])

        where_plan = WherePlan(filter=and_expr, based_on=self.base_plan, transaction=self.transaction)

        result = list(where_plan.execute())

        # Only records with age > 25 AND active == True should be included
        self.assertEqual(len(result), 1)  # Should be Person 4
        self.assertTrue(all(r.age > 25 and r.active for r in result))
        self.assertEqual(set(r.id for r in result), {4})

    def test_or_expression_filter(self):
        """
        Test WherePlan with an OrExpression.

        Verify that records are correctly filtered when at least one of multiple conditions must be true.
        """
        # Filter: age > 28 OR id == 1
        term1 = Term('age', Greater(), 28)
        term2 = Term('id', Equal(), 1)
        or_expr = OrExpression([term1, term2])

        where_plan = WherePlan(filter=or_expr, based_on=self.base_plan, transaction=self.transaction)

        result = list(where_plan.execute())

        # Records with age > 28 OR id == 1 should be included
        self.assertEqual(len(result), 2)  # Should be Person 1 and Person 5
        self.assertTrue(all(r.age > 28 or r.id == 1 for r in result))
        self.assertEqual(set(r.id for r in result), {1, 5})

    def test_not_expression_filter(self):
        """
        Test WherePlan with a NotExpression.

        Verify that records are correctly filtered when a condition must be false.
        """
        # Filter: NOT (active == True)
        term = Term('active', Equal(), True)
        not_expr = NotExpression(term)

        where_plan = WherePlan(filter=not_expr, based_on=self.base_plan, transaction=self.transaction)

        result = list(where_plan.execute())

        # Only records with active == False should be included
        self.assertEqual(len(result), 3)  # Should be Person 1, Person 3, and Person 5
        self.assertTrue(all(not r.active for r in result))
        self.assertEqual(set(r.id for r in result), {1, 3, 5})

    def test_complex_expression_filter(self):
        """
        Test WherePlan with a complex expression combining AND, OR, and NOT.

        Verify that records are correctly filtered based on a complex logical expression.
        """
        # Filter: (age > 25 AND active == True) OR NOT (id == 1)
        term1 = Term('age', Greater(), 25)
        term2 = Term('active', Equal(), True)
        term3 = Term('id', Equal(), 1)

        and_expr = AndExpression([term1, term2])
        not_expr = NotExpression(term3)
        or_expr = OrExpression([and_expr, not_expr])

        where_plan = WherePlan(filter=or_expr, based_on=self.base_plan, transaction=self.transaction)

        result = list(where_plan.execute())

        # Records that satisfy (age > 25 AND active == True) OR NOT (id == 1)
        # This should include all records except Person 1
        self.assertEqual(len(result), 4)
        self.assertEqual(set(r.id for r in result), {2, 3, 4, 5})

    def test_filter_with_no_matches(self):
        """
        Test WherePlan with a filter that matches no records.

        Verify that an empty result is returned when no records match the filter.
        """
        # Filter: age > 100 (no records have age > 100)
        term = Term('age', Greater(), 100)
        where_plan = WherePlan(filter=term, based_on=self.base_plan, transaction=self.transaction)

        result = list(where_plan.execute())

        self.assertEqual(len(result), 0)  # No records should match

    def test_filter_with_all_matches(self):
        """
        Test WherePlan with a filter that matches all records.

        Verify that all records are returned when they all match the filter.
        """
        # Filter: age > 0 (all records have age > 0)
        term = Term('age', Greater(), 0)
        where_plan = WherePlan(filter=term, based_on=self.base_plan, transaction=self.transaction)

        result = list(where_plan.execute())

        self.assertEqual(len(result), 5)  # All records should match
        self.assertEqual(set(r.id for r in result), {1, 2, 3, 4, 5})

    def test_filter_with_different_operators(self):
        """
        Test WherePlan with different operators.

        Verify that different comparison operators work correctly in filters.
        """
        # Test Equal
        where_plan = WherePlan(filter=Term('id', Equal(), 2), based_on=self.base_plan, transaction=self.transaction)
        result = list(where_plan.execute())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, 2)

        # Test NotEqual
        where_plan = WherePlan(filter=Term('id', NotEqual(), 2), based_on=self.base_plan, transaction=self.transaction)
        result = list(where_plan.execute())
        self.assertEqual(len(result), 4)
        self.assertEqual(set(r.id for r in result), {1, 3, 4, 5})

        # Test Greater
        where_plan = WherePlan(filter=Term('age', Greater(), 26), based_on=self.base_plan, transaction=self.transaction)
        result = list(where_plan.execute())
        self.assertEqual(len(result), 2)
        self.assertEqual(set(r.id for r in result), {4, 5})

        # Test GreaterOrEqual
        where_plan = WherePlan(filter=Term('age', GreaterOrEqual(), 26), based_on=self.base_plan,
                               transaction=self.transaction)
        result = list(where_plan.execute())
        self.assertEqual(len(result), 3)
        self.assertEqual(set(r.id for r in result), {3, 4, 5})

        # Test Lower
        where_plan = WherePlan(filter=Term('age', Lower(), 26), based_on=self.base_plan, transaction=self.transaction)
        result = list(where_plan.execute())
        self.assertEqual(len(result), 2)
        self.assertEqual(set(r.id for r in result), {1, 2})

        # Test LowerOrEqual
        where_plan = WherePlan(filter=Term('age', LowerOrEqual(), 26), based_on=self.base_plan,
                               transaction=self.transaction)
        result = list(where_plan.execute())
        self.assertEqual(len(result), 3)
        self.assertEqual(set(r.id for r in result), {1, 2, 3})

    def test_optimize(self):
        """
        Test that the optimize method of WherePlan works as expected.

        Optimization should return a new WherePlan instance with optimized inner query plans
        if the underlying plan doesn't have an accept_filter method.
        """
        term = Term('id', Equal(), 3)
        where_plan = WherePlan(filter=term, based_on=self.base_plan, transaction=self.transaction)

        # Mock the optimize method of the base plan
        optimized_base_plan = MagicMock()
        # Ensure the optimized_base_plan doesn't have an accept_filter method
        del optimized_base_plan.accept_filter
        self.base_plan.optimize = MagicMock(return_value=optimized_base_plan)

        # Call optimize on the WherePlan
        result = where_plan.optimize(None)

        # Verify that the result is a WherePlan with the same filter and the optimized base plan
        self.assertIsInstance(result, WherePlan)
        self.assertEqual(result.filter, term)
        self.assertEqual(result.based_on, optimized_base_plan)

        # Verify that optimize was called on the base plan
        self.base_plan.optimize.assert_called_once()


    def _build_indexed_plan(self, fields: list[str]):
        # Build indexes over given fields mapping key->Set(DBObject) for current mock_data
        base_plan = ListPlan(base_list=self.mock_data, transaction=self.transaction)
        idx_map = {}
        for fld in fields:
            rkd = RepeatedKeysDictionary(transaction=self.transaction)
            for rec in self.mock_data:
                key = getattr(rec, fld)
                rkd = rkd.set_at(key, rec)
            idx_map[fld] = rkd
        indexes_dict = Dictionary(transaction=self.transaction)
        for k, v in idx_map.items():
            indexes_dict = indexes_dict.set_at(k, v)
        return IndexedQueryPlan(indexes=indexes_dict, based_on=base_plan, transaction=self.transaction)

    def test_single_term_indexed_lookup(self):
        indexed = self._build_indexed_plan(['id'])
        term = Term('id', Equal(), 3)
        wp = WherePlan(filter=term, based_on=indexed, transaction=self.transaction)
        optimized = wp.optimize(wp)
        from proto_db.queries import IndexedSearchPlan
        self.assertIsInstance(optimized, IndexedSearchPlan)

    def test_or_expression_indexed_lookup(self):
        indexed = self._build_indexed_plan(['id'])
        expr = OrExpression([Term('id', Equal(), 1), Term('id', Equal(), 3)])
        wp = WherePlan(filter=expr, based_on=indexed, transaction=self.transaction)
        optimized = wp.optimize(wp)
        self.assertIsInstance(optimized, OrMerge)

    def test_or_expression_fallback_on_unindexed_term(self):
        indexed = self._build_indexed_plan(['id'])
        expr = OrExpression([Term('id', Equal(), 1), Term('name', Equal(), 'Person 2')])
        wp = WherePlan(filter=expr, based_on=indexed, transaction=self.transaction)
        optimized = wp.optimize(wp)
        # Should not be OrMerge because one term is unindexed
        self.assertNotIsInstance(optimized, OrMerge)


if __name__ == "__main__":
    unittest.main()
