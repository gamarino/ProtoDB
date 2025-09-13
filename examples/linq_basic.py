"""
LINQ-like API basic usage examples.
Run: python examples/linq_basic.py
"""

import os
import sys
# Ensure project root is on sys.path for direct execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from proto_db.linq import from_collection, F


def main():
    users = [
        {"id": 1, "first_name": "Alice", "last_name": "Zeus", "age": 30, "country": "ES", "status": "active", "email": "a@example.com", "last_login": 5},
        {"id": 2, "first_name": "Bob", "last_name": "Young", "age": 17, "country": "AR", "status": "inactive", "email": "b@example.com", "last_login": 10},
        {"id": 3, "first_name": "Carol", "last_name": "Xavier", "age": 25, "country": "US", "status": "active", "email": "c@example.com", "last_login": 2},
        {"id": 4, "first_name": "Dan", "last_name": "White", "age": 22, "country": "AR", "status": "active", "email": "d@example.com", "last_login": 7},
    ]

    # Filter + projection + order + pagination
    q = (
        from_collection(users)
        .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
        .order_by(F.last_login, ascending=False)
        .select({"id": F.id, "name": F.first_name + " " + F.last_name})
        .take(3)
    )

    print("Explain (text):", q.explain())
    res = q.to_list()
    print("Top 3 ES/AR adults by last_login:")
    for r in res:
        print(r)

    # Distinct and count
    emails = from_collection(users).select(F.email).distinct().to_list()
    active_cnt = from_collection(users).where(F.status == "active").count()
    print("Distinct emails:", emails)
    print("Active users count:", active_cnt)

    # Stable order with then_by
    products = [
        {"id": 1, "category": "A", "price": 10},
        {"id": 2, "category": "A", "price": 20},
        {"id": 3, "category": "B", "price": 5},
    ]
    ordered = (
        from_collection(products)
        .order_by(F.category, ascending=True)
        .then_by(F.price, ascending=False)
        .to_list()
    )
    print("Ordered products:")
    for p in ordered:
        print(p)


if __name__ == "__main__":
    main()
