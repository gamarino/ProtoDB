import unittest
from unittest.mock import MagicMock
import uuid
from ..common import Atom, QueryPlan
from ..dictionaries import HashDictionary
from ..sets import Set  # Import the Set class


class TestSet(unittest.TestCase):
    def setUp(self):
        """
        Set up mock objects to test the `Set` class without relying on external dependencies.
        """
        # Mock HashDictionary
        self.mock_content = MagicMock(spec=HashDictionary)
        # Create a transaction ID for testing
        self.transaction_id = uuid.uuid4()
        # Create a Set instance with the mocked HashDictionary
        self.test_set = Set(content=self.mock_content, transaction_id=self.transaction_id)

    def test_initialization(self):
        """Test that a Set instance is initialized properly."""
        self.assertEqual(self.test_set.content, self.mock_content,
                         "The content of the Set should match the provided HashDictionary.")
        self.assertEqual(self.test_set.transaction_id, self.transaction_id,
                         "The transaction ID should match the one provided.")
        self.assertIsInstance(self.test_set, Set, "The instance should be of type `Set`.")

    def test_add(self):
        """Test adding keys to the Set."""
        key = MagicMock(spec=Atom)  # Mock an Atom object
        key.hash.return_value = 123  # Simulate hash behavior for the Atom

        # Simulate the behavior of the HashDictionary's `set_at` method
        mock_new_content = MagicMock()
        self.mock_content.set_at.return_value = mock_new_content

        # Verify that the new Set includes the key
        updated_set = self.test_set.add(key)
        self.mock_content.set_at.assert_called_once_with(123, key)  # Check that `set_at` was called correctly
        self.assertEqual(updated_set.content, mock_new_content,
                         "The new Set should have the updated dictionary content.")

    def test_remove_key(self):
        """Test removing a key from the Set."""
        key = MagicMock(spec=Atom)  # Mock an Atom object
        key.hash.return_value = 123  # Simulate hash behavior for the Atom

        # Simulate the behavior of the HashDictionary's `remove_key` method
        mock_new_content = MagicMock()
        self.mock_content.remove_key.return_value = mock_new_content

        # Verify that the key is removed from the Set
        updated_set = self.test_set.remove_key(key)
        self.mock_content.remove_key.assert_called_once_with(123)  # Check that `remove_key` was called correctly
        self.assertEqual(updated_set.content, mock_new_content,
                         "The new Set should have the updated dictionary content.")

    def test_has_key(self):
        """Test checking if a key exists in the Set."""
        key = MagicMock(spec=Atom)  # Mock an Atom object
        key.hash.return_value = 123  # Simulate hash behavior for the Atom

        # Simulate the behavior of the HashDictionary's `has` method
        self.mock_content.has.return_value = True

        # Check if the Set contains the key
        result = self.test_set.has(key)
        self.mock_content.has.assert_called_once_with(123)  # Check that `has` was called correctly with the key's hash
        self.assertTrue(result, "The `has` method should return True when the key exists in the Set.")

    def test_as_iterable(self):
        """Test the `as_iterable` method."""
        # Simulate the contents of the Set as an iterable
        mock_iterable = [
            (123, MagicMock(spec=Atom)),
            (456, MagicMock(spec=Atom)),
        ]
        self.mock_content.as_iterable.return_value = iter(mock_iterable)

        # Convert the Set to an iterable
        items = list(self.test_set.as_iterable())

        # Verify the items match the simulated contents
        self.assertEqual(len(items), 2, "The `as_iterable` method should return all items from the Set.")
        self.assertTrue(all(isinstance(item, Atom) for item in items),
                        "All items in the iterable should be of type Atom.")

    def test_as_query_plan(self):
        """Test converting the Set into a QueryPlan."""
        mock_query_plan = MagicMock(spec=QueryPlan)
        self.mock_content.as_query_plan.return_value = mock_query_plan

        # Convert the Set into a QueryPlan
        query_plan = self.test_set.as_query_plan()

        # Verify the conversion
        self.mock_content.as_query_plan.assert_called_once()
        self.assertEqual(query_plan, mock_query_plan,
                         "The `as_query_plan` method should return the QueryPlan from the HashDictionary.")

    def test_non_atom_key_handling(self):
        """Test handling of non-Atom keys."""
        key = "string_key"  # A non-Atom object
        self.mock_content.has.return_value = True

        # Verify that the key's built-in hash is used
        self.test_set.has(key)
        self.mock_content.has.assert_called_once_with(hash(key))

        self.mock_content.set_at.return_value = MagicMock()
        updated_set = self.test_set.add(key)
        self.mock_content.set_at.assert_called_once_with(hash(key), key)
        self.assertIsInstance(updated_set, Set, "Adding a non-Atom key should still return a new `Set`.")

    def test_remove_non_atom_key(self):
        """Test removing a non-Atom key from the Set."""
        key = 123  # A non-Atom object
        mock_new_content = MagicMock()
        self.mock_content.remove_key.return_value = mock_new_content

        updated_set = self.test_set.remove_key(key)
        self.mock_content.remove_key.assert_called_once_with(hash(key))
        self.assertEqual(updated_set.content, mock_new_content,
                         "Removing a non-Atom key should update the Set content properly.")


if __name__ == '__main__':
    unittest.main()
