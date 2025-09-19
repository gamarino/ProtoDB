import unittest
from unittest.mock import MagicMock

from proto_db.common import AtomPointer, DBObject
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import JoinPlan, ListPlan, FromPlan


class TestJoinPlan(unittest.TestCase):
    """
    Unit test class for JoinPlan.

    This test class verifies various behaviors of the JoinPlan class,
    ensuring that it correctly joins records from different sources.
    """

    def setUp(self):
        """
        Set up common resources for the tests.

        This includes creating base data (lists of DBObject instances) and
        mock dependencies for the JoinPlan, such as transaction and atom pointer.
        """
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()
        self.atom_pointer = AtomPointer()

        # Create test data - employees
        self.employees = []
        for i in range(1, 6):
            obj = DBObject(transaction=self.transaction)
            obj = obj.set_at('id', i)
            obj = obj.set_at('name', f'Employee {i}')
            obj = obj.set_at('department_id', i % 3 + 1)  # Departments 1, 2, 3
            self.employees.append(obj)

        # Create test data - departments
        self.departments = []
        for i in range(1, 4):
            obj = DBObject(transaction=self.transaction)
            obj = obj.set_at('id', i)
            obj = obj.set_at('name', f'Department {i}')
            self.departments.append(obj)

        # Create test data - projects (for testing right joins)
        self.projects = []
        for i in range(1, 5):
            obj = DBObject(transaction=self.transaction)
            obj = obj.set_at('id', i)
            obj = obj.set_at('name', f'Project {i}')
            obj = obj.set_at('department_id', i % 4 + 1)  # Departments 1, 2, 3, 4 (4 doesn't exist)
            self.projects.append(obj)

        # Create query plans
        self.employees_plan = ListPlan(base_list=self.employees, transaction=self.transaction)
        self.departments_plan = ListPlan(base_list=self.departments, transaction=self.transaction)
        self.projects_plan = ListPlan(base_list=self.projects, transaction=self.transaction)

        # Create FromPlans with aliases
        self.employees_from = FromPlan(alias='employee', based_on=self.employees_plan, transaction=self.transaction)
        self.departments_from = FromPlan(alias='department', based_on=self.departments_plan,
                                         transaction=self.transaction)
        self.projects_from = FromPlan(alias='project', based_on=self.projects_plan, transaction=self.transaction)

    def test_inner_join(self):
        """
        Test JoinPlan with an inner join.

        Verify that only records with matching keys in both sources are included.
        """
        join_plan = JoinPlan(
            join_query=self.departments_from,
            join_type='inner',
            based_on=self.employees_from,
            transaction=self.transaction
        )

        result = list(join_plan.execute())

        # Should have one result for each employee (5 employees)
        self.assertEqual(len(result), 5)

        # Verify that each result has both employee and department data
        for r in result:
            self.assertTrue(hasattr(r, 'employee'))
            self.assertTrue(hasattr(r, 'department'))

            # Verify that the department_id in employee matches the id in department
            self.assertEqual(r.employee.department_id, r.department.id)

    def test_left_join(self):
        """
        Test JoinPlan with a left join.

        Verify that all records from the left source are included, with matching
        records from the right source where available.
        """
        # Create an employee with a non-existent department
        obj = DBObject(transaction=self.transaction)
        obj = obj.set_at('id', 6)
        obj = obj.set_at('name', 'Employee 6')
        obj = obj.set_at('department_id', 99)  # Non-existent department
        self.employees.append(obj)

        # Update the employees plan
        self.employees_plan = ListPlan(base_list=self.employees, transaction=self.transaction)
        self.employees_from = FromPlan(alias='employee', based_on=self.employees_plan, transaction=self.transaction)

        join_plan = JoinPlan(
            join_query=self.departments_from,
            join_type='left',
            based_on=self.employees_from,
            transaction=self.transaction
        )

        result = list(join_plan.execute())

        # Should have one result for each employee (6 employees)
        self.assertEqual(len(result), 6)

        # Verify that each result has employee data
        for r in result:
            self.assertTrue(hasattr(r, 'employee'))

        # Find the employee with the non-existent department
        emp6 = next((r for r in result if r.employee.id == 6), None)
        self.assertIsNotNone(emp6)

        # Verify that the department is None for the employee with a non-existent department
        self.assertFalse(hasattr(emp6, 'department'))

    def test_right_join(self):
        """
        Test JoinPlan with a right join.

        Verify that all records from the right source are included, with matching
        records from the left source where available.
        """
        join_plan = JoinPlan(
            join_query=self.projects_from,
            join_type='right',
            based_on=self.departments_from,
            transaction=self.transaction
        )

        result = list(join_plan.execute())

        # Should have one result for each project (4 projects)
        self.assertEqual(len(result), 4)

        # Verify that each result has project data
        for r in result:
            self.assertTrue(hasattr(r, 'project'))

        # Find the project with department_id 4 (non-existent department)
        proj4 = next((r for r in result if r.project.department_id == 4), None)
        self.assertIsNotNone(proj4)

        # Verify that the department is None for the project with a non-existent department
        self.assertFalse(hasattr(proj4, 'department'))

    def test_external_join(self):
        """
        Test JoinPlan with an external join.

        Verify that all records from both sources are included, with matching
        records combined where available.
        """
        # Create an employee with a non-existent department
        obj = DBObject(transaction=self.transaction)
        obj = obj.set_at('id', 6)
        obj = obj.set_at('name', 'Employee 6')
        obj = obj.set_at('department_id', 99)  # Non-existent department
        self.employees.append(obj)

        # Update the employees plan
        self.employees_plan = ListPlan(base_list=self.employees, transaction=self.transaction)
        self.employees_from = FromPlan(alias='employee', based_on=self.employees_plan, transaction=self.transaction)

        join_plan = JoinPlan(
            join_query=self.departments_from,
            join_type='external',
            based_on=self.employees_from,
            transaction=self.transaction
        )

        result = list(join_plan.execute())

        # Should have results for:
        # - All employees (6)
        # - All employees joined with all departments (6 * 3 = 18)
        # - All departments (3)
        # Total: 6 + 18 + 3 = 27
        self.assertEqual(len(result), 27)

        # Count results with only employee data
        emp_only = sum(1 for r in result if hasattr(r, 'employee') and not hasattr(r, 'department'))
        self.assertEqual(emp_only, 6)

        # Count results with only department data
        dept_only = sum(1 for r in result if hasattr(r, 'department') and not hasattr(r, 'employee'))
        self.assertEqual(dept_only, 3)

        # Count results with both employee and department data
        both = sum(1 for r in result if hasattr(r, 'employee') and hasattr(r, 'department'))
        self.assertEqual(both, 18)

    def test_external_left_join(self):
        """
        Test JoinPlan with an external left join.

        Verify that all records from the left source are included, along with
        the combined records.
        """
        # Create an employee with a non-existent department
        obj = DBObject(transaction=self.transaction)
        obj = obj.set_at('id', 6)
        obj = obj.set_at('name', 'Employee 6')
        obj = obj.set_at('department_id', 99)  # Non-existent department
        self.employees.append(obj)

        # Update the employees plan
        self.employees_plan = ListPlan(base_list=self.employees, transaction=self.transaction)
        self.employees_from = FromPlan(alias='employee', based_on=self.employees_plan, transaction=self.transaction)

        join_plan = JoinPlan(
            join_query=self.departments_from,
            join_type='external_left',
            based_on=self.employees_from,
            transaction=self.transaction
        )

        result = list(join_plan.execute())

        # Should have results for:
        # - All employees (6)
        # - All employees joined with all departments (6 * 3 = 18)
        # Total: 6 + 18 = 24
        self.assertEqual(len(result), 24)

        # Count results with only employee data
        emp_only = sum(1 for r in result if hasattr(r, 'employee') and not hasattr(r, 'department'))
        self.assertEqual(emp_only, 6)

        # Count results with both employee and department data
        both = sum(1 for r in result if hasattr(r, 'employee') and hasattr(r, 'department'))
        self.assertEqual(both, 18)

    def test_external_right_join(self):
        """
        Test JoinPlan with an external right join.

        Verify that all records from the right source are included, along with
        the combined records.
        """
        join_plan = JoinPlan(
            join_query=self.projects_from,
            join_type='external_right',
            based_on=self.departments_from,
            transaction=self.transaction
        )

        result = list(join_plan.execute())

        # Should have results for:
        # - All departments joined with all projects (3 * 4 = 12)
        # - All projects (4)
        # Total: 12 + 4 = 16
        self.assertEqual(len(result), 16)

        # Count results with only project data
        proj_only = sum(1 for r in result if hasattr(r, 'project') and not hasattr(r, 'department'))
        self.assertEqual(proj_only, 4)

        # Count results with both department and project data
        both = sum(1 for r in result if hasattr(r, 'department') and hasattr(r, 'project'))
        self.assertEqual(both, 12)

    def test_outer_join(self):
        """
        Test JoinPlan with an outer join.

        Verify that all records from both sources are included, without combining them.
        """
        join_plan = JoinPlan(
            join_query=self.departments_from,
            join_type='outer',
            based_on=self.employees_from,
            transaction=self.transaction
        )

        result = list(join_plan.execute())

        # Should have results for:
        # - All employees (5)
        # - All departments (3)
        # Total: 5 + 3 = 8
        self.assertEqual(len(result), 8)

        # Count results with only employee data
        emp_only = sum(1 for r in result if hasattr(r, 'employee') and not hasattr(r, 'department'))
        self.assertEqual(emp_only, 5)

        # Count results with only department data
        dept_only = sum(1 for r in result if hasattr(r, 'department') and not hasattr(r, 'employee'))
        self.assertEqual(dept_only, 3)

        # Count results with both employee and department data
        both = sum(1 for r in result if hasattr(r, 'employee') and hasattr(r, 'department'))
        self.assertEqual(both, 0)  # Outer join doesn't combine records

    def test_optimize(self):
        """
        Test that the optimize method of JoinPlan works as expected.

        Optimization should return a new JoinPlan instance with optimized inner query plans.
        """
        join_plan = JoinPlan(
            join_query=self.departments_from,
            join_type='inner',
            based_on=self.employees_from,
            transaction=self.transaction
        )

        # Mock the optimize methods
        optimized_employees = MagicMock()
        optimized_departments = MagicMock()
        self.employees_from.optimize = MagicMock(return_value=optimized_employees)
        self.departments_from.optimize = MagicMock(return_value=optimized_departments)

        # Call optimize on the JoinPlan
        result = join_plan.optimize()

        # Verify that the result is a JoinPlan with the same join_type and the optimized query plans
        self.assertIsInstance(result, JoinPlan)
        self.assertEqual(result.join_type, 'inner')
        self.assertEqual(result.based_on, optimized_employees)
        self.assertEqual(result.join_query, optimized_departments)

        # Verify that optimize was called on both query plans
        self.employees_from.optimize.assert_called_once()
        self.departments_from.optimize.assert_called_once()


if __name__ == "__main__":
    unittest.main()
