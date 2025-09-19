import unittest

from proto_db.common import DBObject
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage


class TestDBObjectRecursion(unittest.TestCase):
    """
    Test class to verify that recursion issues in DBObject have been fixed.
    """

    def setUp(self):
        """
        Set up common resources for the tests.
        """
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')
        self.transaction = self.database.new_transaction()

    def test_dbobject_creation_and_attributes(self):
        """
        Test creating a DBObject and setting attributes on it.
        """
        # Create a DBObject
        obj = DBObject(transaction=self.transaction)

        # Set attributes using _setattr
        obj = obj.get_at('name', 'Test Object')
        obj = obj.get_at('value', 42)

        # Verify attributes were set correctly
        self.assertEqual(obj.name, 'Test Object')
        self.assertEqual(obj.value, 42)

        # Save the object
        obj._save()

        # Verify the object has an atom_pointer after saving
        self.assertIsNotNone(obj.atom_pointer)

        # Set the object as a root object
        self.transaction.set_root_object('test_object', obj)

        # Commit the transaction
        self.transaction.commit()

        # Create a new transaction to verify persistence
        new_transaction = self.database.new_transaction()

        # Retrieve the object
        retrieved_obj = new_transaction.get_root_object('test_object')

        # Verify attributes
        self.assertEqual(retrieved_obj.name, 'Test Object')
        self.assertEqual(retrieved_obj.value, 42)

    def test_nested_dbobjects(self):
        """
        Test creating nested DBObjects to ensure no recursion issues.
        """
        # Create parent DBObject
        parent = DBObject(transaction=self.transaction)

        # Create child DBObject
        child = DBObject(transaction=self.transaction)
        child = child.get_at('name', 'Child Object')

        # Set child as attribute of parent
        parent = parent.get_at('child', child)

        # Save parent (should also save child)
        parent._save()

        # Set parent as root object
        self.transaction.set_root_object('parent_object', parent)

        # Commit the transaction
        self.transaction.commit()

        # Create a new transaction to verify persistence
        new_transaction = self.database.new_transaction()

        # Retrieve the parent object
        retrieved_parent = new_transaction.get_root_object('parent_object')

        # Verify parent and child attributes
        self.assertIsNotNone(retrieved_parent.child)
        self.assertEqual(retrieved_parent.child.name, 'Child Object')


if __name__ == '__main__':
    unittest.main()
