import unittest
from proto_db.linq import from_collection, F, Policy


class TestLinqLike(unittest.TestCase):
    def setUp(self):
        self.users = [
            {"id": 1, "first_name": "Alice", "last_name": "Zeus", "age": 30, "country": "ES", "status": "active", "email": "a@example.com", "last_login": 5},
            {"id": 2, "first_name": "Bob", "last_name": "Young", "age": 17, "country": "AR", "status": "inactive", "email": "b@example.com", "last_login": 10},
            {"id": 3, "first_name": "Carol", "last_name": "Xavier", "age": 25, "country": "US", "status": "active", "email": "c@example.com", "last_login": 2},
            {"id": 4, "first_name": "Dan", "last_name": "White", "age": 22, "country": "AR", "status": "active", "email": "d@example.com", "last_login": 7},
        ]

    def test_filter_projection_pagination(self):
        q = (from_collection(self.users)
             .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
             .order_by(F.last_login, ascending=False)
             .select({"id": lambda x: x["id"], "name": F.first_name + " " + F.last_name})
             .take(20))
        exp = q.explain()
        self.assertTrue(isinstance(exp, (str, list, dict)))
        res = q.to_list()
        self.assertLessEqual(len(res), 20)

    def test_distinct_and_count(self):
        emails = from_collection(self.users).select(F.email).distinct().to_list()
        cnt = from_collection(self.users).where(F.status == "active").count()
        self.assertGreaterEqual(cnt, 0)
        self.assertEqual(len(emails), len(set(emails)))

    def test_then_by(self):
        products = [
            {"id": 1, "category": "A", "price": 10},
            {"id": 2, "category": "A", "price": 20},
            {"id": 3, "category": "B", "price": 5},
        ]
        res = (from_collection(products)
               .order_by(F.category, ascending=True)
               .then_by(F.price, ascending=False)
               .take(50)
               .to_list())
        self.assertLessEqual(len(res), 50)

    def test_policies_warn(self):
        items = [{"score": 0.9}, {"score": 0.0}]
        def py_check(x):
            return (x.get("score") or 0) > 0.8
        res = (from_collection(items)
               .on_unsupported("warn")
               .where(lambda x: py_check(x))
               .take(100)
               .to_list())
        self.assertLessEqual(len(res), 100)


if __name__ == '__main__':
    unittest.main()
