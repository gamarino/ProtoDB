"""
LINQ-like API: between/range examples.
Run: python examples/linq_between.py
"""

from datetime import datetime
from proto_db.linq import from_collection, F


def main():
    rows = [
        {"id": 1, "age": 9},
        {"id": 2, "age": 10},
        {"id": 3, "age": 15},
        {"id": 4, "age": 20},
        {"id": 5, "age": 21},
        {"id": 6, "age": None},
    ]

    print("Inclusive [10,20]:")
    print(from_collection(rows).where(F.age.between(10, 20)).to_list())

    print("Exclusive (10,20):")
    print(from_collection(rows).where(F.age.between(10, 20, inclusive=(False, False))).to_list())

    print("Bounds '[)':")
    print(from_collection(rows).where(F.age.range(10, 20, bounds='[)')).to_list())

    # Lambda chained comparison automatically recognized
    print("Lambda 10 <= age <= 20:")
    print(from_collection(rows).where(lambda x: 10 <= x["age"] <= 20).to_list())

    # Datetime between
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 3)
    dates = [
        {"d": datetime(2023, 12, 31)},
        {"d": datetime(2024, 1, 1)},
        {"d": datetime(2024, 1, 2)},
        {"d": datetime(2024, 1, 3)},
        {"d": datetime(2024, 1, 4)},
    ]
    print("Datetime inclusive [start,end]:", from_collection(dates).where(F.d.between(start, end)).count())
    print("Datetime [start,end):", from_collection(dates).where(F.d.between(start, end, inclusive=(True, False))).count())

    # Explain the pipeline
    q = from_collection(rows).where(F.age.between(10, 20)).take(2)
    print("Explain JSON:", q.explain("json"))


if __name__ == "__main__":
    main()
