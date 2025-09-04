import unittest
from datetime import datetime, timedelta
from proto_db.linq import from_collection, F


class TestLinqBetween(unittest.TestCase):
    def setUp(self):
        self.rows = [
            {"id": 1, "age": 9},
            {"id": 2, "age": 10},
            {"id": 3, "age": 15},
            {"id": 4, "age": 20},
            {"id": 5, "age": 21},
            {"id": 6, "age": None},
        ]

    def test_between_inclusive_default(self):
        res = from_collection(self.rows).where(F.age.between(10,20)).to_list()
        ids = [r["id"] for r in res]
        self.assertEqual(ids, [2,3,4])

    def test_between_exclusive(self):
        res = from_collection(self.rows).where(F.age.between(10,20, inclusive=(False, False))).to_list()
        ids = [r["id"] for r in res]
        self.assertEqual(ids, [3])

    def test_between_sugars(self):
        res1 = from_collection(self.rows).where(F.age.between_closed(10,20)).count()
        res2 = from_collection(self.rows).where(F.age.between_open(10,20)).count()
        res3 = from_collection(self.rows).where(F.age.between_left_open(10,20)).count()
        res4 = from_collection(self.rows).where(F.age.between_right_open(10,20)).count()
        self.assertEqual(res1, 3)
        self.assertEqual(res2, 1)
        self.assertEqual(res3, 2)
        self.assertEqual(res4, 2)

    def test_between_range_bounds(self):
        res = from_collection(self.rows).where(F.age.range(10,20, bounds='[)')).to_list()
        ids = [r["id"] for r in res]
        self.assertEqual(ids, [2,3])

    def test_lambda_chained_comparison(self):
        res = from_collection(self.rows).where(lambda x: 10 <= x["age"] <= 20).to_list()
        ids = [r["id"] for r in res]
        self.assertEqual(ids, [2,3,4])

    def test_datetime_between(self):
        start = datetime(2024,1,1)
        end = datetime(2024,1,3)
        data = [
            {"d": datetime(2023,12,31)},
            {"d": datetime(2024,1,1)},
            {"d": datetime(2024,1,2)},
            {"d": datetime(2024,1,3)},
            {"d": datetime(2024,1,4)},
        ]
        res = from_collection(data).where(F.d.between(start, end)).to_list()
        self.assertEqual(len(res), 3)
        res2 = from_collection(data).where(F.d.between(start, end, inclusive=(True, False))).to_list()
        self.assertEqual(len(res2), 2)


if __name__ == '__main__':
    unittest.main()
