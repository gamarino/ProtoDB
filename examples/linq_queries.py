#!/usr/bin/env python3
"""The LINQ front end over plain Python data: filters, ordering, paging, grouping, joins and flattening.

Run from the repository root::

    python examples/linq_queries.py

``from_collection`` accepts a ProtoDB collection, a query plan or any iterable.  Every
query builds a physical plan that ``explain()`` shows; Python lambdas are allowed and
appear in the plan as an explicit ``PythonFilter``.  The script checks every result.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import Agg, F, from_collection  # noqa: E402

CUSTOMERS = [
    {"id": 1, "name": "Alice", "country": "ES"},
    {"id": 2, "name": "Bob", "country": "AR"},
    {"id": 3, "name": "Carol", "country": "ES"},
]
ORDERS = [
    {"id": 10, "customer_id": 1, "total": 120.0, "day": date(2026, 1, 3), "items": ["pen", "ink"]},
    {"id": 11, "customer_id": 1, "total": 35.5, "day": date(2026, 2, 14), "items": ["paper"]},
    {"id": 12, "customer_id": 2, "total": 410.0, "day": date(2026, 2, 20), "items": ["desk"]},
    {"id": 13, "customer_id": 3, "total": 18.0, "day": date(2026, 3, 1), "items": []},
    {"id": 14, "customer_id": 2, "total": 77.0, "day": date(2026, 3, 9), "items": ["lamp", "pen"]},
]


def main() -> None:
    """Run a handful of queries and check their results."""
    # Filter, order, page and project.
    big = (from_collection(ORDERS)
           .where(F.total >= 50)
           .order_by(F.total, descending=True)
           .take(2)
           .select({"order": F.id, "total": F.total}))
    print("two largest orders over 50:", big.to_list())
    assert big.to_list() == [{"order": 12, "total": 410.0}, {"order": 10, "total": 120.0}]

    # Ranges, including dates; between() is inclusive unless told otherwise.
    february = from_collection(ORDERS).where(F.day.between(date(2026, 2, 1), date(2026, 2, 28)))
    print("February orders:", [o["id"] for o in february])
    assert [o["id"] for o in february] == [11, 12]

    # A Python lambda is a visible PythonFilter node, never a silent fallback.
    odd = from_collection(ORDERS).where(lambda o: o["id"] % 2 == 1)
    print("plan with a lambda:", odd.explain()["plan"])
    assert odd.explain()["plan"] == "PythonFilter" and [o["id"] for o in odd] == [11, 13]

    # Group and aggregate.
    per_customer = (from_collection(ORDERS)
                    .group_by(F.customer_id, {"customer": Agg.key(), "n": Agg.count(), "spent": Agg.sum(F.total)})
                    .order_by(F.customer))
    rows = per_customer.to_list()
    print("per customer:", rows)
    assert rows == [{"customer": 1, "n": 2, "spent": 155.5}, {"customer": 2, "n": 2, "spent": 487.0},
                    {"customer": 3, "n": 1, "spent": 18.0}]

    # Join customers to their orders (inner join), then filter on the pair.
    spain = (from_collection(CUSTOMERS)
             .join(ORDERS, F.id, F.customer_id)
             .where(lambda pair: pair[0]["country"] == "ES")
             .select(lambda pair: (pair[0]["name"], pair[1]["id"])))
    print("orders from Spain:", spain.to_list())
    assert sorted(spain.to_list()) == [("Alice", 10), ("Alice", 11), ("Carol", 13)]

    # Flatten the items of every order; distinct keeps one of each.
    items = from_collection(ORDERS).select_many(F.items).distinct().order_by(F())
    print("distinct items:", items.to_list())
    assert items.to_list() == ["desk", "ink", "lamp", "paper", "pen"]

    print("linq_queries: OK")


if __name__ == "__main__":
    main()
