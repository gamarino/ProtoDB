import unittest
import os
from tempfile import TemporaryDirectory

from ..db_access import ObjectSpace, Database, ObjectTransaction
from ..standalone_file_storage import StandaloneFileStorage
from ..file_block_provider import FileBlockProvider
from ..lists import List



TEST_SIZE = 100_000

class TestDBAccess(unittest.TestCase):

    def setUp(self):
        """
        Configuración inicial para las pruebas.
        """
        self.temp_dir = TemporaryDirectory()
        self.directory_name = "testDB"
        self.db_path = os.path.join(self.temp_dir.name, self.directory_name)
        os.mkdir(self.db_path)

        block_provider = FileBlockProvider(self.db_path)
        self.storage_space = ObjectSpace(
            storage=StandaloneFileStorage(
                block_provider=block_provider
            ))
        self.database = self.storage_space.new_database('TestDB')

    def tearDown(self):
        """Clean up temporary directory."""
        self.storage_space.close()
        self.temp_dir.cleanup()

    def reopenDB(self):
        block_provider = FileBlockProvider(self.db_path)
        self.storage_space = ObjectSpace(
            storage=StandaloneFileStorage(
                block_provider=block_provider
            ))
        self.database = self.storage_space.open_database('TestDB')

    # --- Test set_at ---
    def test_001_db_creation(self):
        tr = self.database.new_transaction()
        tr.set_root_object('test_001', 'test_001_data')
        tr.commit()
        self.storage_space.close()

        self.assertTrue(os.path.exists(self.db_path), 'Space object directory does not exists!')
        self.assertTrue(os.path.exists(os.path.join(self.db_path, 'space_root')), 'Space root does not exists!')

        self.reopenDB()

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

        self.storage_space.close()
        self.reopenDB()

        tr = self.database.new_transaction()
        check_list = tr.get_root_object('test_002')
        self.assertTrue(isinstance(check_list, List), 'A list was not recovered')
        self.assertTrue(check_list.count == TEST_SIZE, f'The recovered list has the wrong size ({check_list.count})')
        for i in range(0, TEST_SIZE):
            self.assertTrue(check_list.get_at(i) == i, f'Element {i} check failed')
        tr.commit()

