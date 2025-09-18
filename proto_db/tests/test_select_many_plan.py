import unittest
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.common import DBObject
from proto_db.sets import Set
from proto_db.linq import from_collection, F


class TestSelectManyPlan(unittest.TestCase):
    def setUp(self):
        self.space = ObjectSpace(storage=MemoryStorage())
        self.db = self.space.new_database('test_lateral_joins')
        tr = self.db.new_transaction()

        categories = Set(transaction=tr)
        products = Set(transaction=tr)

        cat1 = DBObject(id=1, name='Electronics', transaction=tr)
        cat2 = DBObject(id=2, name='Books', transaction=tr)

        prod1 = DBObject(id=101, name='Laptop', category_id=1, price=1200, tags=['tech', 'work'], transaction=tr)
        prod2 = DBObject(id=102, name='Keyboard', category_id=1, price=75, transaction=tr)
        prod3 = DBObject(id=103, name='Monitor', category_id=1, price=300, transaction=tr)
        
        prod4 = DBObject(id=201, name='Sci-Fi Novel', category_id=2, price=15, tags=['reading', 'fiction'], transaction=tr)
        prod5 = DBObject(id=202, name='Cookbook', category_id=2, price=25, transaction=tr)

        categories = categories.add(cat1).add(cat2)
        products = products.add(prod1).add(prod2).add(prod3).add(prod4).add(prod5)

        products = products.add_index('category_id')

        tr.set_root_object('categories', categories)
        tr.set_root_object('products', products)
        tr.commit()

    def test_simple_unnesting(self):
        """Tests flattening a list attribute from each object."""
        tr = self.db.new_transaction()
        products = tr.get_root_object('products')
        q = (
            from_collection(products)
            .where(F.tags != None)
            .select_many(
                collection_selector=lambda p: p.tags,
                result_selector=lambda p, tag: {'product': p.name, 'tag': tag}
            )
        )
        results = list(q)
        self.assertIn({'product': 'Laptop', 'tag': 'tech'}, results)
        self.assertIn({'product': 'Laptop', 'tag': 'work'}, results)
        self.assertIn({'product': 'Sci-Fi Novel', 'tag': 'reading'}, results)
        self.assertIn({'product': 'Sci-Fi Novel', 'tag': 'fiction'}, results)
        self.assertEqual(len(results), 4)

    def test_top_n_per_category(self):
        """Tests the classic Top-N per category use case."""
        tr = self.db.new_transaction()
        categories = tr.get_root_object('categories')
        products = tr.get_root_object('products')

        q = (
            from_collection(categories).order_by(F.name)
            .select_many(
                collection_selector=lambda c: (
                    from_collection(products)
                    .where(F.category_id == c.id)
                    .order_by(F.price, ascending=False)
                    .take(2)
                ),
                result_selector=lambda c, p: {'category': c.name, 'product': p.name}
            )
        )
        results = list(q)
        self.assertEqual(len(results), 4)
        self.assertIn({'category': 'Electronics', 'product': 'Laptop'}, results)
        self.assertIn({'category': 'Electronics', 'product': 'Monitor'}, results)
        self.assertIn({'category': 'Books', 'product': 'Cookbook'}, results)
        self.assertIn({'category': 'Books', 'product': 'Sci-Fi Novel'}, results)

    def test_chaining_after_select_many(self):
        """Tests that other operators can be chained after select_many."""
        tr = self.db.new_transaction()
        categories = tr.get_root_object('categories')
        products = tr.get_root_object('products')

        q = (
            from_collection(categories)
            .select_many(
                collection_selector=lambda c: (
                    from_collection(products)
                    .where(F.category_id == c.id)
                    .order_by(F.price, ascending=False)
                    .take(1)
                )
            )
            .where(F.price > 100)
        )
        results = list(q)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, 'Laptop')


if __name__ == '__main__':
    unittest.main()
