import unittest

from proto_db.sets import Set  # Import the Set class


class TestSet(unittest.TestCase):
    def setUp(self):
        """
        Set up mock objects to test the `Set` class without relying on external dependencies.
        """
        # Mock HashDictionary
        self.test_set = Set()

    def test_add(self):
        """Test adding keys to the Set."""

        test_set = self.test_set.add(1)
        self.assertTrue(test_set.has(1), "Fail adding one element")
        self.assertEqual(test_set.count, 1, "Fail adding one element, wrong count")
        set_content = [element for element in test_set.as_iterable()]
        self.assertEqual(set_content, [1], "Fail adding one element, wrong content")

        test_set = test_set.add(2)
        self.assertTrue(test_set.has(2), "Fail adding two elements")
        self.assertEqual(test_set.count, 2, "Fail adding two elements, wrong count")
        set_content = set(test_set.as_iterable())
        self.assertEqual(set_content, {1, 2}, "Fail adding two elements, wrong content")

    def test_remove_key(self):
        """Test removing a key from the Set."""
        test_set = self.test_set.add(1)
        test_set = test_set.add(2)
        test_set = test_set.add(3)

        test_set = test_set.remove_at(2)

        self.assertFalse(test_set.has(2), "Fail removing one element")
        self.assertEqual(test_set.count, 2, "Fail removing one element, wrong count")
        set_content = set(test_set.as_iterable())
        self.assertEqual(set_content, {1, 3}, "Fail removing one element, wrong content")

    def test_has_key(self):
        """Test checking if a key exists in the Set."""
        """Test removing a key from the Set."""
        test_set = self.test_set.add(1)
        test_set = test_set.add(2)
        test_set = test_set.add(3)

        self.assertFalse(test_set.has(4), "Fail has_key element, non included")
        self.assertTrue(test_set.has(2), "Fail has_key element, included")

    def test_as_iterable(self):
        """Test the `as_iterable` method."""
        test_set = self.test_set.add(1)
        test_set = test_set.add(2)
        test_set = test_set.add(3)

        set_content = set(test_set.as_iterable())
        self.assertEqual(set_content, {1, 2, 3}, "Fail as_iterable, wrong content")


if __name__ == '__main__':
    unittest.main()
