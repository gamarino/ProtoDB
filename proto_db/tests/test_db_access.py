import unittest
import uuid

from ..db_access import ObjectSpace, Database, ObjectTransaction
from ..memory_storage import MemoryStorage

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
        self.assertEqual(tr2.get_root_object('test_001_data'), 'test_001',
                         "Value from previous transaction not preserved!")
        tr2.commit()



