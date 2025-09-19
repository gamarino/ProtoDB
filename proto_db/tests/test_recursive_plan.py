import unittest
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.common import DBObject
from proto_db.lists import List
from proto_db.sets import Set
from proto_db.linq import from_collection, F


class TestRecursivePlan(unittest.TestCase):
    def setUp(self):
        # Set up an in-memory database and a collection with a hierarchy
        self.space = ObjectSpace(storage=MemoryStorage())
        self.db = self.space.new_database('test_recursion')
        tr = self.db.new_transaction()
        collection = Set(transaction=tr)

        # Create nodes
        ceo = DBObject(transaction=tr, name='CEO')
        vp1 = DBObject(transaction=tr, name='VP1')
        vp2 = DBObject(transaction=tr, name='VP2')
        dir1 = DBObject(transaction=tr, name='Director1')
        mgr1 = DBObject(transaction=tr, name='Manager1')
        emp1 = DBObject(transaction=tr, name='Employee1')

        # Create hierarchy: emp1 -> mgr1 -> dir1 -> vp1 -> ceo
        # Build from top to bottom ensuring references point to updated copies
        vp1 = vp1.set_at('manager', ceo)
        dir1 = dir1.set_at('manager', vp1)
        mgr1 = mgr1.set_at('manager', dir1)
        emp1 = emp1.set_at('manager', mgr1)
        # First set vp1.reports, then point ceo.reports to the updated vp1
        vp1 = vp1.set_at('reports', List(transaction=tr).append_last(dir1))
        ceo = ceo.set_at('reports', List(transaction=tr).append_last(vp1).append_last(vp2))

        # Cycle peer <-> peer
        cyclic1 = DBObject(transaction=tr, name='Cyclic1')
        cyclic2 = DBObject(transaction=tr, name='Cyclic2')
        cyclic1 = cyclic1.set_at('peer', cyclic2)
        cyclic2 = cyclic2.set_at('peer', cyclic1)

        # Add all to collection
        for item in [ceo, vp1, vp2, dir1, mgr1, emp1, cyclic1, cyclic2]:
            collection = collection.add(item)

        tr.set_root_object('employees', collection)
        tr.commit()

    def test_traverse_up_dfs(self):
        tr = self.db.new_transaction()
        collection = tr.get_root_object('employees')
        q = (from_collection(collection)
             .where(F.name == 'Employee1')
             .traverse('manager', direction='up', strategy='dfs'))
        def _nm(e):
            v = getattr(e, 'name', None)
            return v.string if hasattr(v, 'string') else v
        results = [_nm(e) for e in q]
        self.assertEqual(results, ['Manager1', 'Director1', 'VP1', 'CEO'])

    def test_traverse_down_bfs(self):
        tr = self.db.new_transaction()
        collection = tr.get_root_object('employees')
        q = (from_collection(collection)
             .where(F.name == 'CEO')
             .traverse('reports', direction='down', strategy='bfs'))
        def _nm(e):
            v = getattr(e, 'name', None)
            return v.string if hasattr(v, 'string') else v
        results = [_nm(e) for e in q]
        self.assertIn('VP1', results)
        self.assertIn('VP2', results)
        self.assertIn('Director1', results)
        self.assertEqual(len(results), 3)

    def test_max_depth(self):
        tr = self.db.new_transaction()
        collection = tr.get_root_object('employees')
        q = (from_collection(collection)
             .where(F.name == 'Employee1')
             .traverse('manager', max_depth=2))
        def _nm(e):
            v = getattr(e, 'name', None)
            return v.string if hasattr(v, 'string') else v
        results = [_nm(e) for e in q]
        self.assertEqual(results, ['Manager1', 'Director1'])

    def test_cycle_detection(self):
        tr = self.db.new_transaction()
        collection = tr.get_root_object('employees')
        q = (from_collection(collection)
             .where(F.name == 'Cyclic1')
             .traverse('peer', max_depth=5))
        results = [e.name for e in q]
        self.assertEqual(results, ['Cyclic2'])

    def test_include_start_node(self):
        tr = self.db.new_transaction()
        collection = tr.get_root_object('employees')
        q = (from_collection(collection)
             .where(F.name == 'Manager1')
             .traverse('manager', include_start_node=True))
        def _nm(e):
            v = getattr(e, 'name', None)
            return v.string if hasattr(v, 'string') else v
        results = [_nm(e) for e in q]
        self.assertIn('Manager1', results)
        self.assertIn('Director1', results)
        # Manager1, Director1, VP1, CEO
        self.assertEqual(len(results), 4)

    def test_chaining_after_traverse(self):
        tr = self.db.new_transaction()
        collection = tr.get_root_object('employees')
        q = (from_collection(collection)
             .where(F.name == 'Employee1')
             .traverse('manager')
             .where(F.name.contains('VP')))
        def _nm(e):
            v = getattr(e, 'name', None)
            return v.string if hasattr(v, 'string') else v
        results = [_nm(e) for e in q]
        self.assertEqual(results, ['VP1'])


if __name__ == '__main__':
    unittest.main()
