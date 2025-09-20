import os
import unittest
from tempfile import TemporaryDirectory

from proto_db.db_access import ObjectSpace
from proto_db.file_block_provider import FileBlockProvider
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.common import DBObject
from proto_db.lists import List
from proto_db.dictionaries import Dictionary
from proto_db.queries import WherePlan, Term, Equal, ListPlan


class Person(DBObject):
    def __init__(self, name: str = None, tags=None, **kwargs):
        # Pass attributes through to DBObject so they are set during construction
        super().__init__(name=name, tags=tags, **kwargs)


class TestIntegrationRoundTrip(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.directory_name = "testDB"
        self.db_path = os.path.join(self.temp_dir.name, self.directory_name)
        os.mkdir(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _new_space(self):
        block_provider = FileBlockProvider(self.db_path)
        return ObjectSpace(StandaloneFileStorage(block_provider))

    def test_201_write_close_reopen_and_query_with_index(self):
        # Phase 1: write
        space = self._new_space()
        db = space.new_database("TestDB")

        tr = db.new_transaction()
        people = tr.new_list()

        # Build object graph: a list of Person objects, each has a Set of tags
        from proto_db.sets import Set
        tags1 = Set(transaction=tr)
        tags1 = tags1.add("team")
        tags1 = tags1.add("blue")
        p1 = Person(transaction=tr, name="Alice", tags=tags1)

        tags2 = Set(transaction=tr)
        tags2 = tags2.add("team")
        tags2 = tags2.add("red")
        p2 = Person(transaction=tr, name="Bob", tags=tags2)

        tags3 = Set(transaction=tr)
        tags3 = tags3.add("green")
        p3 = Person(transaction=tr, name="Carol", tags=tags3)

        people = people.append_last(p1)
        people = people.append_last(p2)
        people = people.append_last(p3)

        # Add index on name
        people = people.add_index("name")

        tr.set_root_object("people", people)
        tr.commit()

        # Close space completely to flush and simulate process restart
        space.close()

        # Phase 2: reopen and verify
        space2 = self._new_space()
        db2 = space2.open_database("TestDB")
        tr2 = db2.new_transaction()
        loaded_people = tr2.get_root_object("people")

        # Data assertion: structure and cardinality after reload
        loaded = list(loaded_people.as_iterable())
        self.assertEqual(len(loaded), 3)
        # Basic sanity: tags collections are present
        self.assertTrue(hasattr(loaded[0], 'tags'))
        self.assertTrue(hasattr(loaded[1], 'tags'))

        # Index assertion: query by name should use IndexedSearchPlan
        base = loaded_people.as_query_plan()
        plan = WherePlan(filter=Term("name", Equal(), "Bob"), based_on=base, transaction=tr2)
        optimized = plan.optimize()
        self.assertEqual(type(optimized).__name__, "IndexedSearchPlan")

        results = list(plan.execute())
        self.assertEqual(len(results), 1)

        tr2.commit()
        space2.close()
