#!/usr/bin/env python3
"""Indexed collections and the query planner: indexes are used transparently and explain() shows how.

Run from the repository root::

    python examples/indexed_query.py

A dictionary of people carries an equality index on ``city`` and a range index on
``age``.  The same LINQ queries run with and without the indexes; the script prints
the plans, checks that both runs return the same rows and exits with an error if not.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import EqualityIndex, F, MemoryStorage, ObjectSpace, RangeIndex, from_collection  # noqa: E402

CITIES = ("Paris", "Rome", "Lima", "Oslo")


def plan_nodes(plan: dict) -> list:
    """The node names of an explain() tree, root first."""
    names = [plan["plan"]]
    for child in plan.get("children", ()):
        names.extend(plan_nodes(child))
    return names


def main() -> None:
    """Build an indexed and an unindexed version of the same data and compare the query plans."""
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("people")
        with db.new_transaction() as tr:
            people = tr.new_dictionary().add_index(EqualityIndex("by_city", "city")) \
                .add_index(RangeIndex("by_age", "age"))
            for i in range(1000):
                person = tr.new_object(name=f"person{i:04d}", city=CITIES[i % 4], age=18 + i % 60)
                people = people.set_at(person.name, person)
            tr.set_root_object("people", people)

        tr = db.new_transaction()
        people = tr.get_root_object("people")
        print("indexes:", people.index_names())
        plain = people.remove_index("by_city").remove_index("by_age")

        queries = {
            "city == Lima": lambda source: from_collection(source).where(F.city == "Lima"),
            "30 <= age < 35 and city == Oslo": lambda source: from_collection(source).where(
                F.age.between(30, 35, inclusive=(True, False)) & (F.city == "Oslo")),
            # Ties keep the order of the access path, so a total order needs a tie-breaker.
            "age > 70, by age then name, top 3": lambda source: (
                from_collection(source).where(F.age > 70).order_by(F.age).then_by(F.name).take(3)),
        }
        for label, build in queries.items():
            indexed, scanned = build(people), build(plain)
            print(f"\n{label}")
            print("  with indexes:   ", " <- ".join(plan_nodes(indexed.explain())))
            print("  without indexes:", " <- ".join(plan_nodes(scanned.explain())))
            got = [p.name for p in indexed.to_list()]
            expected = [p.name for p in scanned.to_list()]
            ordered = "top" in label  # only the ordered query fixes the order of its rows
            assert (got == expected) if ordered else (sorted(got) == sorted(expected)), (label, got, expected)
            print(f"  {len(got)} rows, identical in both plans")

        lima = queries["city == Lima"](people)
        assert plan_nodes(lima.explain())[0] == "IndexLookup"
        assert lima.count() == 250
        tr.abort()
    finally:
        space.close()
    print("\nindexed_query: OK")


if __name__ == "__main__":
    main()
