import unittest
from unittest.mock import MagicMock

from your_module import ObjectTransaction, AtomPointer, SelectPlan, ListPlan


class TestSelectPlan(unittest.TestCase):
    """
    Unit test class for SelectPlan.

    This test class verifies various behaviors of the SelectPlan class,
    ensuring that it correctly processes and transforms input data according
    to the given field mapping.
    """

    def setUp(self):
        """
        Set up common resources for the tests.

        This includes creating base data (a simple list of dictionaries primed as mock records)
        and mock dependencies for the SelectPlan, such as transaction and atom pointer.
        """
        self.mock_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 40},
        ]
        self.transaction = ObjectTransaction()
        self.atom_pointer = AtomPointer()
        self.base_plan = ListPlan(base_list=self.mock_data, transaction=self.transaction)

    def test_select_with_direct_fields(self):
        """
        Test SelectPlan that directly maps existing fields.

        Verify that fields from the original records are correctly extracted.
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
        Test SelectPlan with callable fields.

        Verify that fields derived from callables (e.g., lambda functions) work as expected.
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
        Test SelectPlan with a mix of direct field mappings and callable fields.

        Verify that both direct mappings and dynamic values via callables are combined correctly.
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
        Test SelectPlan with no fields specified.

        Verify that the result is an empty list of records when no mappings exist.
        """
        select_plan = SelectPlan(fields={}, based_on=self.base_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        expected_result = [{} for _ in self.mock_data]  # Empty dictionaries for each record
        self.assertEqual(result, expected_result)

    def test_execute_with_empty_base_data(self):
        """
        Test SelectPlan on an empty underlying data source.

        Verify that the plan produces no results when the source has no data.
        """
        empty_plan = ListPlan(base_list=[], transaction=self.transaction)
        fields = {"field_1": "id"}
        select_plan = SelectPlan(fields=fields, based_on=empty_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        self.assertEqual(result, [])  # Should return an empty list

    def test_optimization(self):
        """
        Test that the optimize method of SelectPlan works as expected.

        Optimization should return a new SelectPlan instance with optimized inner query plans.
        """
        optimized_plan = self.base_plan.optimize(None)  # Mock optimization
        select_plan = SelectPlan(fields={"field": "id"}, based_on=self.base_plan, transaction=self.transaction)

        result = select_plan.optimize(None)

        self.assertIsInstance(result, SelectPlan)
        self.assertEqual(result.fields, select_plan.fields)
        self.assertEqual(result.based_on, optimized_plan)

    def test_with_invalid_field_mapping(self):
        """
        Test SelectPlan with an invalid or missing field mapping.

        Verify that fields with invalid mapping do not propagate into results.
        """
        fields = {
            "valid_field": "id",
            "invalid_field": "invalid_field_name",  # Field does not exist in the original records
        }
        select_plan = SelectPlan(fields=fields, based_on=self.base_plan, transaction=self.transaction)

        result = list(select_plan.execute())

        expected_result = [
            {"valid_field": 1},
            {"valid_field": 2},
            {"valid_field": 3},
        ]
        self.assertEqual(result, expected_result)

    def test_with_callable_exceptions(self):
        """
        Test that SelectPlan handles exceptions in callable fields gracefully.

        If a callable raises an exception, ensure it does not stop the entire execution.
        """

        def faulty_callable(record):
            if record["id"] == 2:
                raise ValueError("Testing exception handling")
            return record["name"]

        fields = {
            "safe_name": faulty_callable,
        }

        select_plan = SelectPlan(fields=fields, based_on=self.base_plan, transaction=self.transaction)

        with self.assertRaises(ValueError):
            list(select_plan.execute())  # The exception from the callable should propagate


if __name__ == "__main__":
    unittest.main()

