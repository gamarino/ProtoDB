import unittest

from proto_db.common import DBObject
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import (
    Expression, AndExpression, OrExpression, NotExpression, Term, TrueTerm, FalseTerm,
    Equal, NotEqual, Greater, GreaterOrEqual, Lower, LowerOrEqual, Contains, In,
    IsTrue, NotTrue, IsNone, NotNone
)


class TestExpressions(unittest.TestCase):
    """
    Unit test class for Expression classes.

    This test class verifies various behaviors of the Expression classes,
    ensuring that they correctly evaluate conditions.
    """

    def setUp(self):
        """
        Set up common resources for the tests.

        This includes creating test records with various attributes.
        """
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()

        # Create test records
        self.record1 = DBObject(transaction=self.transaction)
        self.record1 = self.record1.set_at('id', 1)
        self.record1 = self.record1.set_at('name', 'Alice')
        self.record1 = self.record1.set_at('age', 30)
        self.record1 = self.record1.set_at('active', True)
        self.record1 = self.record1.set_at('tags', ['user', 'admin'])
        self.record1 = self.record1.set_at('manager', None)

        self.record2 = DBObject(transaction=self.transaction)
        self.record2 = self.record2.set_at('id', 2)
        self.record2 = self.record2.set_at('name', 'Bob')
        self.record2 = self.record2.set_at('age', 25)
        self.record2 = self.record2.set_at('active', False)
        self.record2 = self.record2.set_at('tags', ['user'])
        self.record2 = self.record2.set_at('manager', 'Alice')

    def test_term_with_equal_operator(self):
        """
        Test Term with Equal operator.

        Verify that Term correctly evaluates equality conditions.
        """
        # Term: id == 1
        term = Term('id', Equal(), 1)

        self.assertTrue(term.match(self.record1))
        self.assertFalse(term.match(self.record2))

        # Term: name == 'Bob'
        term = Term('name', Equal(), 'Bob')

        self.assertFalse(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

    def test_term_with_not_equal_operator(self):
        """
        Test Term with NotEqual operator.

        Verify that Term correctly evaluates inequality conditions.
        """
        # Term: id != 1
        term = Term('id', NotEqual(), 1)

        self.assertFalse(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

        # Term: name != 'Bob'
        term = Term('name', NotEqual(), 'Bob')

        self.assertTrue(term.match(self.record1))
        self.assertFalse(term.match(self.record2))

    def test_term_with_comparison_operators(self):
        """
        Test Term with comparison operators (Greater, GreaterOrEqual, Lower, LowerOrEqual).

        Verify that Term correctly evaluates comparison conditions.
        """
        # Term: age > 25
        term = Term('age', Greater(), 25)

        self.assertTrue(term.match(self.record1))
        self.assertFalse(term.match(self.record2))

        # Term: age >= 25
        term = Term('age', GreaterOrEqual(), 25)

        self.assertTrue(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

        # Term: age < 30
        term = Term('age', Lower(), 30)

        self.assertFalse(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

        # Term: age <= 30
        term = Term('age', LowerOrEqual(), 30)

        self.assertTrue(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

    def test_term_with_contains_operator(self):
        """
        Test Term with Contains operator.

        Verify that Term correctly evaluates containment conditions.
        """
        # Term: 'admin' in tags
        term = Term('tags', Contains(), 'admin')

        self.assertTrue(term.match(self.record1))
        self.assertFalse(term.match(self.record2))

        # Term: 'user' in tags
        term = Term('tags', Contains(), 'user')

        self.assertTrue(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

    def test_term_with_in_operator(self):
        """
        Test Term with In operator.

        Verify that Term correctly evaluates membership conditions.
        """
        # Term: name in ['Alice', 'Charlie']
        term = Term('name', In(), ['Alice', 'Charlie'])

        self.assertTrue(term.match(self.record1))
        self.assertFalse(term.match(self.record2))

        # Term: name in ['Bob', 'Charlie']
        term = Term('name', In(), ['Bob', 'Charlie'])

        self.assertFalse(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

    def test_term_with_boolean_operators(self):
        """
        Test Term with boolean operators (IsTrue, NotTrue).

        Verify that Term correctly evaluates boolean conditions.
        """
        # Term: active is True
        term = Term('active', IsTrue(), None)

        self.assertTrue(term.match(self.record1))
        self.assertFalse(term.match(self.record2))

        # Term: active is not True
        term = Term('active', NotTrue(), None)

        self.assertFalse(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

    def test_term_with_none_operators(self):
        """
        Test Term with None operators (IsNone, NotNone).

        Verify that Term correctly evaluates None conditions.
        """
        # Term: manager is None
        term = Term('manager', IsNone(), None)

        self.assertTrue(term.match(self.record1))
        self.assertFalse(term.match(self.record2))

        # Term: manager is not None
        term = Term('manager', NotNone(), None)

        self.assertFalse(term.match(self.record1))
        self.assertTrue(term.match(self.record2))

    def test_and_expression(self):
        """
        Test AndExpression.

        Verify that AndExpression correctly combines multiple conditions with AND logic.
        """
        # Expression: id == 1 AND name == 'Alice'
        term1 = Term('id', Equal(), 1)
        term2 = Term('name', Equal(), 'Alice')
        and_expr = AndExpression([term1, term2])

        self.assertTrue(and_expr.match(self.record1))
        self.assertFalse(and_expr.match(self.record2))

        # Expression: id == 2 AND name == 'Alice'
        term1 = Term('id', Equal(), 2)
        term2 = Term('name', Equal(), 'Alice')
        and_expr = AndExpression([term1, term2])

        self.assertFalse(and_expr.match(self.record1))
        self.assertFalse(and_expr.match(self.record2))

        # Expression: id == 2 AND name == 'Bob'
        term1 = Term('id', Equal(), 2)
        term2 = Term('name', Equal(), 'Bob')
        and_expr = AndExpression([term1, term2])

        self.assertFalse(and_expr.match(self.record1))
        self.assertTrue(and_expr.match(self.record2))

    def test_or_expression(self):
        """
        Test OrExpression.

        Verify that OrExpression correctly combines multiple conditions with OR logic.
        """
        # Expression: id == 1 OR name == 'Bob'
        term1 = Term('id', Equal(), 1)
        term2 = Term('name', Equal(), 'Bob')
        or_expr = OrExpression([term1, term2])

        self.assertTrue(or_expr.match(self.record1))
        self.assertTrue(or_expr.match(self.record2))

        # Expression: id == 3 OR name == 'Charlie'
        term1 = Term('id', Equal(), 3)
        term2 = Term('name', Equal(), 'Charlie')
        or_expr = OrExpression([term1, term2])

        self.assertFalse(or_expr.match(self.record1))
        self.assertFalse(or_expr.match(self.record2))

        # Expression: id == 1 OR id == 2
        term1 = Term('id', Equal(), 1)
        term2 = Term('id', Equal(), 2)
        or_expr = OrExpression([term1, term2])

        self.assertTrue(or_expr.match(self.record1))
        self.assertTrue(or_expr.match(self.record2))

    def test_not_expression(self):
        """
        Test NotExpression.

        Verify that NotExpression correctly negates a condition.
        """
        # Expression: NOT (id == 1)
        term = Term('id', Equal(), 1)
        not_expr = NotExpression(term)

        self.assertFalse(not_expr.match(self.record1))
        self.assertTrue(not_expr.match(self.record2))

        # Expression: NOT (name == 'Bob')
        term = Term('name', Equal(), 'Bob')
        not_expr = NotExpression(term)

        self.assertTrue(not_expr.match(self.record1))
        self.assertFalse(not_expr.match(self.record2))

    def test_complex_expression(self):
        """
        Test complex expressions combining AND, OR, and NOT.

        Verify that complex expressions correctly evaluate combined conditions.
        """
        # Expression: (id == 1 AND active == True) OR (name == 'Bob' AND age < 30)
        term1 = Term('id', Equal(), 1)
        term2 = Term('active', Equal(), True)
        term3 = Term('name', Equal(), 'Bob')
        term4 = Term('age', Lower(), 30)

        and_expr1 = AndExpression([term1, term2])
        and_expr2 = AndExpression([term3, term4])
        or_expr = OrExpression([and_expr1, and_expr2])

        self.assertTrue(or_expr.match(self.record1))
        self.assertTrue(or_expr.match(self.record2))

        # Expression: NOT ((id == 1 AND active == True) OR (name == 'Bob' AND age < 30))
        not_expr = NotExpression(or_expr)

        self.assertFalse(not_expr.match(self.record1))
        self.assertFalse(not_expr.match(self.record2))

    def test_true_term(self):
        """
        Test TrueTerm.

        Verify that TrueTerm always evaluates to True.
        """
        term = TrueTerm()

        self.assertTrue(term.match(self.record1))
        self.assertTrue(term.match(self.record2))
        self.assertTrue(term.match(None))

    def test_false_term(self):
        """
        Test FalseTerm.

        Verify that FalseTerm always evaluates to False.
        """
        term = FalseTerm()

        self.assertFalse(term.match(self.record1))
        self.assertFalse(term.match(self.record2))
        self.assertFalse(term.match(None))

    def test_expression_compile(self):
        """
        Test Expression.compile method.

        Verify that the compile method correctly builds expression trees from lists.
        """
        # Compile: ['id', '==', 1]
        expr = Expression.compile(['id', '==', 1])

        self.assertIsInstance(expr, Term)
        self.assertEqual(expr.target_attribute, 'id')
        self.assertIsInstance(expr.operation, Equal)
        self.assertEqual(expr.value, 1)

        # Compile: ['&', ['id', '==', 1], ['name', '==', 'Alice']]
        expr = Expression.compile(['&', ['id', '==', 1], ['name', '==', 'Alice']])

        self.assertIsInstance(expr, AndExpression)
        self.assertEqual(len(expr.terms), 2)
        self.assertIsInstance(expr.terms[0], Term)
        self.assertIsInstance(expr.terms[1], Term)

        # Compile: ['|', ['id', '==', 1], ['name', '==', 'Bob']]
        expr = Expression.compile(['|', ['id', '==', 1], ['name', '==', 'Bob']])

        self.assertIsInstance(expr, OrExpression)
        self.assertEqual(len(expr.terms), 2)
        self.assertIsInstance(expr.terms[0], Term)
        self.assertIsInstance(expr.terms[1], Term)

        # Compile: ['!', ['id', '==', 1]]
        expr = Expression.compile(['!', ['id', '==', 1]])

        self.assertIsInstance(expr, NotExpression)
        self.assertIsInstance(expr.negated_expression, Term)

        # Compile: ['&', ['id', '==', 1], ['|', ['name', '==', 'Alice'], ['name', '==', 'Bob']]]
        expr = Expression.compile(['&', ['id', '==', 1], ['|', ['name', '==', 'Alice'], ['name', '==', 'Bob']]])

        self.assertIsInstance(expr, AndExpression)
        self.assertEqual(len(expr.terms), 2)
        self.assertIsInstance(expr.terms[0], Term)
        self.assertIsInstance(expr.terms[1], OrExpression)


if __name__ == "__main__":
    unittest.main()
