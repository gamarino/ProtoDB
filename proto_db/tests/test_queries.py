import unittest
from unittest.mock import MagicMock, PropertyMock

from proto_db.db_access import ObjectTransaction, ObjectSpace, Database
from proto_db.memory_storage import MemoryStorage
from proto_db.common import AtomPointer
from proto_db.queries import SelectPlan, ListPlan, WherePlan, AndExpression, Term, CountPlan, CountResultPlan, QueryPlan


class TestSelectPlan(unittest.TestCase):
    """
    Unit test class for the SelectPlan executor.

    This test suite verifies the core functionality of the SelectPlan, ensuring it
    correctly transforms input records based on various field mapping strategies,
    handles edge cases, and interacts correctly with the query plan lifecycle.
    """

    def setUp(self):
        """
        Set up common resources for all tests in this class.

        This method initializes a mock data source, a memory-based database
        environment, and a base ListPlan that serves as the data provider for
        the SelectPlan instances under test.
        """
        self.mock_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 40},
        ]
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()
        self.base_plan = ListPlan(base_list=self.mock_data, transaction=self.transaction)

    def test_select_with_direct_fields(self):
        """
        Test SelectPlan with direct field remapping.

        Verifies that fields are correctly selected and renamed from the source records.
        """
        fields = {
            "identifier": "id",
            "person_name": "name",
            "years_old": "age",
        }
        select_plan = SelectPlan(fields=fields, based_on=self.base_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        expected_result = [
            {"identifier": 1, "person_name": "Alice", "years_old": 30},
            {"identifier": 2, "person_name": "Bob", "years_old": 25},
            {"identifier": 3, "person_name": "Charlie", "years_old": 40},
        ]
        self.assertEqual(result, expected_result)

    def test_select_with_callable_fields(self):
        """
        Test SelectPlan with fields generated by callable functions (lambdas).

        Verifies that new fields can be dynamically computed from source records.
        """
        fields = {
            "full_name": lambda record: f"ID-{record['id']} {record['name']}",
            "is_adult": lambda record: record["age"] >= 18,
        }
        select_plan = SelectPlan(fields=fields, based_on=self.base_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        expected_result = [
            {"full_name": "ID-1 Alice", "is_adult": True},
            {"full_name": "ID-2 Bob", "is_adult": True},
            {"full_name": "ID-3 Charlie", "is_adult": True},
        ]
        self.assertEqual(result, expected_result)

    def test_select_with_mixed_fields(self):
        """
        Test SelectPlan with a mix of direct and callable field mappings.

        Ensures that both static remapping and dynamic computation can be used together.
        """
        fields = {
            "user_id": "id",
            "upper_name": lambda record: record["name"].upper(),
            "is_senior": lambda record: record["age"] > 35,
        }
        select_plan = SelectPlan(fields=fields, based_on=self.base_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        expected_result = [
            {"user_id": 1, "upper_name": "ALICE", "is_senior": False},
            {"user_id": 2, "upper_name": "BOB", "is_senior": False},
            {"user_id": 3, "upper_name": "CHARLIE", "is_senior": True},
        ]
        self.assertEqual(result, expected_result)

    def test_empty_select(self):
        """
        Test SelectPlan with an empty field mapping.

        Verifies that an empty `fields` dictionary results in a list of empty records.
        """
        select_plan = SelectPlan(fields={}, based_on=self.base_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        expected_result = [{} for _ in self.mock_data]
        self.assertEqual(result, expected_result)

    def test_execute_with_empty_base_data(self):
        """
        Test SelectPlan execution on an empty data source.

        Ensures the plan returns an empty list when the underlying plan yields no records.
        """
        empty_plan = ListPlan(base_list=[], transaction=self.transaction)
        fields = {"field_1": "id"}
        select_plan = SelectPlan(fields=fields, based_on=empty_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        self.assertEqual(result, [])

    def test_optimization(self):
        """
        Test the `optimize` method of SelectPlan.

        Verifies that optimization is delegated to the underlying plan and a new
        SelectPlan instance is returned with the optimized base.
        """
        # Create a mock for the base plan to control its optimization behavior
        mock_base_plan = MagicMock(spec=ListPlan)
        optimized_mock_plan = MagicMock(spec=ListPlan)
        mock_base_plan.optimize.return_value = optimized_mock_plan

        select_plan = SelectPlan(fields={"field": "id"}, based_on=mock_base_plan, transaction=self.transaction)
        
        result = select_plan.optimize(None)

        mock_base_plan.optimize.assert_called_once_with(None)
        self.assertIsInstance(result, SelectPlan)
        self.assertIs(result.based_on, optimized_mock_plan)

    def test_with_invalid_field_mapping(self):
        """
        Test SelectPlan with a mapping to a non-existent source field.

        Verifies that missing source fields are handled gracefully and do not
        appear in the output record.
        """
        fields = {
            "valid_field": "id",
            "invalid_field": "non_existent_field",
        }
        select_plan = SelectPlan(fields=fields, based_on=self.base_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        # The key "invalid_field" should not be present in the results.
        expected_result = [
            {"valid_field": 1},
            {"valid_field": 2},
            {"valid_field": 3},
        ]
        self.assertEqual(result, expected_result)

    def test_with_callable_exceptions(self):
        """
        Test that SelectPlan properly propagates exceptions from callable fields.

        Ensures that if a lambda or function raises an exception during execution,
        it is not silently ignored and bubbles up as expected.
        """
        def faulty_callable(record):
            if record["id"] == 2:
                raise ValueError("A test-induced processing error")
            return record["name"]

        fields = {"safe_name": faulty_callable}
        select_plan = SelectPlan(fields=fields, based_on=self.base_plan, transaction=self.transaction)

        # The exception should propagate up when the iterator is consumed.
        with self.assertRaisesRegex(ValueError, "A test-induced processing error"):
            list(select_plan.execute())


class TestWherePlanOptimizer(unittest.TestCase):
    """
    Unit test class for the WherePlan's optimization logic.

    This suite validates the specific optimization strategies implemented in the
    `WherePlan.optimize` method, such as filter reordering and predicate pushdown.
    """
    def setUp(self):
        """
        Set up common resources for the optimizer tests.
        """
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()
        self.base_plan = ListPlan(base_list=[], transaction=self.transaction)

    def test_optimizer_reorders_and_expression(self):
        """
        Verify that the optimizer reorders AND terms to place cheaper ones first.

        This test ensures that for an `AndExpression`, the optimizer sorts the
        terms according to their cost heuristic, improving performance by
        evaluating more selective filters first.
        """
        # Define a "cheaper" term (equality) and a more "expensive" one.
        cheaper_term = Term(target_attribute='id', operation='==', value=1)
        expensive_term = Term(target_attribute='age', operation='>', value=30)

        # Create an AND expression with the expensive term first.
        and_filter = AndExpression(terms=[expensive_term, cheaper_term])
        where_plan = WherePlan(filter=and_filter, based_on=self.base_plan, transaction=self.transaction)
        
        # We can directly test the helper method to isolate this functionality.
        reordered_filter = where_plan._reorder_and_expression(and_filter)

        # The first term of the reordered filter should now be the "cheaper" one.
        self.assertIs(reordered_filter.terms[0], cheaper_term, "Cheaper term should be first.")
        self.assertIs(reordered_filter.terms[1], expensive_term, "Expensive term should be second.")

    def test_optimizer_pushes_down_predicate(self):
        """
        Verify that the optimizer attempts to push the filter down to the underlying plan.

        This test checks the "predicate pushdown" mechanism. If the underlying plan
        exposes an `accept_filter` method, the `WherePlan` should delegate its
        filter to it instead of processing the filter itself.
        """
        # Create a mock for the underlying plan that supports `accept_filter`.
        mock_based_on = MagicMock(spec=ListPlan)
        mock_based_on.optimize.return_value = mock_based_on
        mock_based_on.accept_filter = MagicMock()
        mock_based_on.accept_filter.return_value = "Optimized plan with pushed-down filter"

        term_filter = Term(target_attribute='id', operation='==', value=1)
        where_plan = WherePlan(filter=term_filter, based_on=mock_based_on, transaction=self.transaction)

        # Execute the optimization.
        result_plan = where_plan.optimize(full_plan=None)

        # Verify that the underlying plan's `optimize` was called.
        mock_based_on.optimize.assert_called_once()
        # Verify that the predicate pushdown was triggered by calling `accept_filter`.
        mock_based_on.accept_filter.assert_called_once_with(term_filter)
        # Verify that the final returned plan is the one from the pushdown.
        self.assertEqual(result_plan, "Optimized plan with pushed-down filter")


class TestCountPlan(unittest.TestCase):
    """
    Unit test class for the CountPlan executor.

    This test suite verifies that the CountPlan correctly counts records,
    especially focusing on its ability to use optimizations by delegating
    the count to underlying plans that support it.
    """

    def setUp(self):
        """
        Set up common resources for all tests in this class.
        """
        self.mock_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "David"},
        ]
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()
        self.base_plan = ListPlan(base_list=self.mock_data, transaction=self.transaction)

    def test_count_with_no_optimization(self):
        """
        Test CountPlan when the underlying plan does not support fast counting.

        Verifies that CountPlan falls back to iterating through the results
        to get the count.
        """
        count_plan = CountPlan(based_on=self.base_plan, transaction=self.transaction)

        # The base ListPlan doesn't have a .count() method, so it will iterate.
        result = list(count_plan.execute())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['count'], 4)

    def test_optimization_delegates_to_subplan_with_count(self):
        """
        Test that CountPlan's optimizer correctly uses the .count() method
        from an optimizable sub-plan.
        """
        # Create a mock for the base plan that supports a fast .count() method.
        mock_base_plan = MagicMock(spec=QueryPlan)

        # The optimize() method of the mock plan returns another mock
        # that has the count() method.
        optimized_mock_plan = MagicMock(spec=QueryPlan)
        optimized_mock_plan.count.return_value = 123  # A specific count value
        mock_base_plan.optimize.return_value = optimized_mock_plan

        # Create the CountPlan on top of the mock base plan.
        count_plan = CountPlan(based_on=mock_base_plan, transaction=self.transaction)

        # Optimize the plan
        optimized_count_plan = count_plan.optimize(None)

        # 1. Assert that the base plan's optimization was called.
        mock_base_plan.optimize.assert_called_once_with(None)

        # 2. Assert that the resulting plan is a CountResultPlan.
        self.assertIsInstance(optimized_count_plan, CountResultPlan,
                              "Optimizer should produce a CountResultPlan when sub-plan supports .count()")

        # 3. Execute the optimized plan and check the result.
        result = list(optimized_count_plan.execute())
        self.assertEqual(result[0]['count'], 123)

        # 4. Verify that the sub-plan's count method was called.
        optimized_mock_plan.count.assert_called_once()

    def test_count_on_empty_base_data(self):
        """
        Test CountPlan execution on an empty data source.
        """
        empty_plan = ListPlan(base_list=[], transaction=self.transaction)
        count_plan = CountPlan(based_on=empty_plan, transaction=self.transaction)

        result = list(count_plan.execute())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['count'], 0)

    def test_optimizer_returns_self_if_subplan_has_no_count(self):
        """
        Verify that the optimizer returns the plan itself if the sub-plan
        cannot be optimized for fast counting.
        """
        # ListPlan does not have a .count() method, so it's a perfect candidate.
        # Its optimize() method returns another ListPlan.
        count_plan = CountPlan(based_on=self.base_plan, transaction=self.transaction)

        optimized_plan = count_plan.optimize(None)

        # The plan should not have been replaced with a CountResultPlan
        self.assertIsInstance(optimized_plan, CountPlan)
        self.assertNotIsInstance(optimized_plan, CountResultPlan)
        # The base plan inside should be the optimized version of the original base plan
        self.assertIsInstance(optimized_plan.based_on, ListPlan)


if __name__ == "__main__":
    unittest.main()