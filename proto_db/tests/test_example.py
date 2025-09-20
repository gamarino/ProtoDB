import unittest

from proto_db.common import Atom, Literal


class TestExample(unittest.TestCase):
    """Example test to demonstrate testing in ProtoBase."""

    def test_literal_creation(self):
        """Test creating and using a Literal."""
        # Create a literal with a string value
        literal = Literal(literal="test_value")

        # Verify the literal's string attribute
        self.assertEqual(literal.string, "test_value")

        # Test string representation
        self.assertEqual(str(literal), "test_value")

        # Test equality comparison
        self.assertEqual(literal, "test_value")

    def test_atom_basic_functionality(self):
        """Test basic Atom functionality."""
        # Create an atom
        atom1 = Atom()
        atom2 = Atom()

        # Verify atoms are different objects
        self.assertIsNot(atom1, atom2)

        # Verify atom pointers are initially None
        self.assertIsNone(atom1.atom_pointer)

        # Test that _loaded is initially False
        self.assertFalse(atom1._loaded)


if __name__ == '__main__':
    unittest.main()
