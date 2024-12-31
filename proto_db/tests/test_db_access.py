import unittest
import uuid

from ..db_access import ObjectSpace, Database, ObjectTransaction
from ..memory_storage import MemoryStorage
from ..lists import List

TEST_SIZE = 100_000

class TestDBAccess(unittest.TestCase):

    def setUp(self):
        """
        Configuración inicial para las pruebas.
        """
        self.storage_space = ObjectSpace(storage=MemoryStorage())
        self.database = self.storage_space.new_database('TestDB')

    # --- Test set_at ---
    def test_001_db_creation(self):
        tr = self.database.new_transaction()
        tr.set_root_object('test_001', 'test_001_data')
        tr.commit()

        tr2 = self.database.new_transaction()
        self.assertEqual(tr2.get_root_object('test_001'), 'test_001_data',
                         "Value from previous transaction not preserved!")
        tr2.commit()

    def test_002_object_creation(self):
        tr = self.database.new_transaction()

        test_list = tr.new_list()
        for i in range(0, TEST_SIZE):
            test_list = test_list.set_at(i, i)
        tr.set_root_object('test_002', test_list)
        tr.commit()

        tr = self.database.new_transaction()

        check_list = tr.get_root_object('test_002')
        self.assertTrue(isinstance(check_list, List), 'A list was not recovered')
        self.assertTrue(check_list.count == TEST_SIZE, f'The recovered list has the wrong size ({check_list.count})')
        for i in range(0, TEST_SIZE):
            self.assertTrue(check_list.get_at(i) == i, f'Element {i} check failed')
        tr.commit()

