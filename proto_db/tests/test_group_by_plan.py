import unittest
from unittest.mock import MagicMock

from proto_db.common import AtomPointer, DBObject
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import (
    GroupByPlan, ListPlan, SumAgreggator, AvgAggregator, CountAggregator,
    MinAgreggator, MaxAgreggator, AgreggatorSpec
)


class TestGroupByPlan(unittest.TestCase):
    """
    Unit test class for GroupByPlan.

    This test class verifies various behaviors of the GroupByPlan class,
    ensuring that it correctly groups records and applies aggregation functions.
    """

    def setUp(self):
        """
        Set up common resources for the tests.

        This includes creating base data (a list of DBObject instances) and
        mock dependencies for the GroupByPlan, such as transaction and atom pointer.
        """
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()
        self.atom_pointer = AtomPointer()

        # Create test data - sales records with department, product, and amount
        self.mock_data = []

        # Department A, Product X
        for i in range(1, 4):
            obj = DBObject(transaction=self.transaction)
            obj = obj._setattr('id', i)
            obj = obj._setattr('department', 'A')
            obj = obj._setattr('product', 'X')
            obj = obj._setattr('amount', 100 * i)
            self.mock_data.append(obj)

        # Department A, Product Y
        for i in range(4, 7):
            obj = DBObject(transaction=self.transaction)
            obj = obj._setattr('id', i)
            obj = obj._setattr('department', 'A')
            obj = obj._setattr('product', 'Y')
            obj = obj._setattr('amount', 50 * i)
            self.mock_data.append(obj)

        # Department B, Product X
        for i in range(7, 10):
            obj = DBObject(transaction=self.transaction)
            obj = obj._setattr('id', i)
            obj = obj._setattr('department', 'B')
            obj = obj._setattr('product', 'X')
            obj = obj._setattr('amount', 75 * i)
            self.mock_data.append(obj)

        # Department B, Product Z
        for i in range(10, 13):
            obj = DBObject(transaction=self.transaction)
            obj = obj._setattr('id', i)
            obj = obj._setattr('department', 'B')
            obj = obj._setattr('product', 'Z')
            obj = obj._setattr('amount', 60 * i)
            self.mock_data.append(obj)

        self.base_plan = ListPlan(base_list=self.mock_data, transaction=self.transaction)

    def test_group_by_single_field(self):
        """
        Test GroupByPlan with grouping by a single field.

        Verify that records are correctly grouped by department and aggregations are computed.
        """
        # Group by department and sum amounts
        group_fields = ['department']
        aggregated_fields = {
            'total_amount': AgreggatorSpec(SumAgreggator(), 'amount', 'total_amount')
        }

        group_plan = GroupByPlan(
            group_fields=group_fields,
            agreggated_fields=aggregated_fields,
            based_on=self.base_plan,
            transaction=self.transaction
        )

        result = list(group_plan.execute())

        # Should have 2 groups: Department A and Department B
        self.assertEqual(len(result), 2)

        # Find the groups in the result
        dept_a = next((r for r in result if r.department == 'A'), None)
        dept_b = next((r for r in result if r.department == 'B'), None)

        self.assertIsNotNone(dept_a)
        self.assertIsNotNone(dept_b)

        # Department A: 100 + 200 + 300 + 200 + 250 + 300 = 1350
        self.assertEqual(dept_a.total_amount, 1350)

        # Department B: 525 + 600 + 675 + 600 + 660 + 720 = 3780
        self.assertEqual(dept_b.total_amount, 3780)

    def test_group_by_multiple_fields(self):
        """
        Test GroupByPlan with grouping by multiple fields.

        Verify that records are correctly grouped by department and product,
        and aggregations are computed for each group.
        """
        # Group by department and product, and sum amounts
        group_fields = ['department', 'product']
        aggregated_fields = {
            'total_amount': AgreggatorSpec(SumAgreggator(), 'amount', 'total_amount')
        }

        group_plan = GroupByPlan(
            group_fields=group_fields,
            agreggated_fields=aggregated_fields,
            based_on=self.base_plan,
            transaction=self.transaction
        )

        result = list(group_plan.execute())

        # Should have 4 groups: (A,X), (A,Y), (B,X), (B,Z)
        self.assertEqual(len(result), 4)

        # Find the groups in the result
        a_x = next((r for r in result if r.department == 'A' and r.product == 'X'), None)
        a_y = next((r for r in result if r.department == 'A' and r.product == 'Y'), None)
        b_x = next((r for r in result if r.department == 'B' and r.product == 'X'), None)
        b_z = next((r for r in result if r.department == 'B' and r.product == 'Z'), None)

        self.assertIsNotNone(a_x)
        self.assertIsNotNone(a_y)
        self.assertIsNotNone(b_x)
        self.assertIsNotNone(b_z)

        # Department A, Product X: 100 + 200 + 300 = 600
        self.assertEqual(a_x.total_amount, 600)

        # Department A, Product Y: 200 + 250 + 300 = 750
        self.assertEqual(a_y.total_amount, 750)

        # Department B, Product X: 525 + 600 + 675 = 1800
        self.assertEqual(b_x.total_amount, 1800)

        # Department B, Product Z: 600 + 660 + 720 = 1980
        self.assertEqual(b_z.total_amount, 1980)

    def test_multiple_aggregations(self):
        """
        Test GroupByPlan with multiple aggregation functions.

        Verify that different aggregation functions (sum, avg, count, min, max)
        can be applied simultaneously.
        """
        # Group by department and apply multiple aggregations
        group_fields = ['department']
        aggregated_fields = {
            'total_amount': AgreggatorSpec(SumAgreggator(), 'amount', 'total_amount'),
            'avg_amount': AgreggatorSpec(AvgAggregator(), 'amount', 'avg_amount'),
            'count': AgreggatorSpec(CountAggregator(), 'amount', 'count'),
            'min_amount': AgreggatorSpec(MinAgreggator(), 'amount', 'min_amount'),
            'max_amount': AgreggatorSpec(MaxAgreggator(), 'amount', 'max_amount')
        }

        group_plan = GroupByPlan(
            group_fields=group_fields,
            agreggated_fields=aggregated_fields,
            based_on=self.base_plan,
            transaction=self.transaction
        )

        result = list(group_plan.execute())

        # Should have 2 groups: Department A and Department B
        self.assertEqual(len(result), 2)

        # Find the groups in the result
        dept_a = next((r for r in result if r.department == 'A'), None)
        dept_b = next((r for r in result if r.department == 'B'), None)

        self.assertIsNotNone(dept_a)
        self.assertIsNotNone(dept_b)

        # Department A
        self.assertEqual(dept_a.total_amount, 1350)  # Sum
        self.assertEqual(dept_a.avg_amount, 1350 / 6)  # Average
        self.assertEqual(dept_a.count, 6)  # Count
        self.assertEqual(dept_a.min_amount, 100)  # Min
        self.assertEqual(dept_a.max_amount, 300)  # Max

        # Department B
        self.assertEqual(dept_b.total_amount, 3780)  # Sum
        self.assertEqual(dept_b.avg_amount, 3780 / 6)  # Average
        self.assertEqual(dept_b.count, 6)  # Count
        self.assertEqual(dept_b.min_amount, 525)  # Min
        self.assertEqual(dept_b.max_amount, 720)  # Max

    def test_empty_group_by(self):
        """
        Test GroupByPlan with an empty input.

        Verify that no groups are returned when the input is empty.
        """
        empty_plan = ListPlan(base_list=[], transaction=self.transaction)

        group_fields = ['department']
        aggregated_fields = {
            'total_amount': AgreggatorSpec(SumAgreggator(), 'amount', 'total_amount')
        }

        group_plan = GroupByPlan(
            group_fields=group_fields,
            agreggated_fields=aggregated_fields,
            based_on=empty_plan,
            transaction=self.transaction
        )

        result = list(group_plan.execute())

        # Should have no groups
        self.assertEqual(len(result), 0)

    def test_group_by_with_missing_fields(self):
        """
        Test GroupByPlan with records missing some fields.

        Verify that records with missing fields are handled correctly.
        """
        # Create data with some missing fields
        mixed_data = []

        # Complete record
        obj1 = DBObject(transaction=self.transaction)
        obj1 = obj1._setattr('id', 1)
        obj1 = obj1._setattr('department', 'A')
        obj1 = obj1._setattr('amount', 100)
        mixed_data.append(obj1)

        # Record missing amount
        obj2 = DBObject(transaction=self.transaction)
        obj2 = obj2._setattr('id', 2)
        obj2 = obj2._setattr('department', 'A')
        mixed_data.append(obj2)

        # Record missing department
        obj3 = DBObject(transaction=self.transaction)
        obj3 = obj3._setattr('id', 3)
        obj3 = obj3._setattr('amount', 300)
        mixed_data.append(obj3)

        mixed_plan = ListPlan(base_list=mixed_data, transaction=self.transaction)

        group_fields = ['department']
        aggregated_fields = {
            'total_amount': AgreggatorSpec(SumAgreggator(), 'amount', 'total_amount')
        }

        group_plan = GroupByPlan(
            group_fields=group_fields,
            agreggated_fields=aggregated_fields,
            based_on=mixed_plan,
            transaction=self.transaction
        )

        result = list(group_plan.execute())

        # Should have 2 groups: Department A and None (for missing department)
        self.assertEqual(len(result), 2)

        # Find the groups in the result
        dept_a = next((r for r in result if r.department == 'A'), None)
        dept_none = next((r for r in result if r.department is None), None)

        self.assertIsNotNone(dept_a)
        self.assertIsNotNone(dept_none)

        # Department A: 100 + 0 (missing amount treated as 0) = 100
        self.assertEqual(dept_a.total_amount, 100)

        # Department None: 300
        self.assertEqual(dept_none.total_amount, 300)

    def test_optimize(self):
        """
        Test that the optimize method of GroupByPlan works as expected.

        Optimization should return a new GroupByPlan instance with optimized inner query plans.
        """
        group_fields = ['department']
        aggregated_fields = {
            'total_amount': AgreggatorSpec(SumAgreggator(), 'amount', 'total_amount')
        }

        group_plan = GroupByPlan(
            group_fields=group_fields,
            agreggated_fields=aggregated_fields,
            based_on=self.base_plan,
            transaction=self.transaction
        )

        # Mock the optimize method of the base plan
        optimized_base_plan = MagicMock()
        self.base_plan.optimize = MagicMock(return_value=optimized_base_plan)

        # Call optimize on the GroupByPlan
        result = group_plan.optimize()

        # Verify that the result is a GroupByPlan with the same fields and the optimized base plan
        self.assertIsInstance(result, GroupByPlan)
        self.assertEqual(result.group_fields, group_fields)
        self.assertEqual(result.agreggated_fields, aggregated_fields)
        self.assertEqual(result.based_on, optimized_base_plan)

        # Verify that optimize was called on the base plan
        self.base_plan.optimize.assert_called_once()


if __name__ == "__main__":
    unittest.main()
