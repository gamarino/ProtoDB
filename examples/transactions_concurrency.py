#!/usr/bin/env python3
"""Transactions under concurrency: snapshot isolation, conflicts, retries and per-key writes.

Run from the repository root::

    python examples/transactions_concurrency.py

Eight threads update the same database at once.  The script shows how proto_db's
precondition model (docs/source/concurrency-model.rst) resolves concurrent commits and
checks that no update is lost:

1. A read-modify-write of a root raises ``ProtoConflictError`` when another commit
   changed that root first, and the error names the unit that failed; ``Database.run``
   retries the whole function on a fresh snapshot.
2. A dictionary root records what each transaction read and wrote per key: its
   changes are reapplied on the current version at commit, so concurrent writers
   of different keys never conflict.
3. A ``CountedSet`` composes concurrent counts: writers that only add never conflict.
4. A ``MutableObject`` names a mutable cell outside the roots, with the same
   conflict detection; a write made without reading the cell is compared too.
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import MemoryStorage, ObjectSpace, ProtoConflictError  # noqa: E402

THREADS = 8
INCREMENTS = 25


def run_threads(target) -> None:
    """Run ``target(thread_number)`` in THREADS threads and wait for all of them."""
    threads = [threading.Thread(target=target, args=(n,)) for n in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def main() -> None:
    """Show conflicts, retried read-modify-writes, per-key dictionary writes and a mutable counter."""
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("counters")
        with db.new_transaction() as tr:
            tr.set_root_object("counter", 0)
            tr.set_root_object("events", tr.new_dictionary())
            visits_cell = tr.new_mutable("visits", 0)  # a MutableObject handle

        # 1. Two transactions read the same snapshot; the second commit is a conflict.
        first, second = db.new_transaction(), db.new_transaction()
        first.set_root_object("counter", first.get_root_object("counter") + 1)
        second.set_root_object("counter", second.get_root_object("counter") + 1)
        first.commit()
        try:
            second.commit()
            raise AssertionError("the second commit should have conflicted")
        except ProtoConflictError as error:
            print("conflict detected: the second read-modify-write was not applied "
                  f"(unit {error.unit.kind.name} {error.unit.root!r})")
            assert error.unit.root == "counter"

        # Database.run retries the function on a fresh snapshot until it commits.
        def increment(tr):
            tr.set_root_object("counter", tr.get_root_object("counter") + 1)

        def worker(_n):
            for _ in range(INCREMENTS):
                db.run(increment, retries=1000)

        run_threads(worker)

        # 2. Per-key writes: each thread adds its own keys to the dictionary, without retries.
        def key_writer(n):
            for i in range(INCREMENTS):
                with db.new_transaction() as tr:
                    tr.set_root_object("events", tr.get_root_object("events").set_at(f"t{n}-{i}", i))

        run_threads(key_writer)

        # 3. Counts compose: every thread adds to the same CountedSet, without retries.
        with db.new_transaction() as tr:
            tr.set_root_object("visits_by_page", tr.new_counted_set())

        def page_visitor(n):
            for _ in range(INCREMENTS):
                with db.new_transaction() as tr:
                    pages = tr.get_root_object("visits_by_page")
                    tr.set_root_object("visits_by_page", pages.add("home").add(f"page-{n % 2}"))

        run_threads(page_visitor)

        # 4. A mutable cell, incremented through its handle.
        def visit(tr):
            visits_cell.set(tr, visits_cell.get(tr) + 1)

        def visitor(_n):
            for _ in range(INCREMENTS):
                db.run(visit, retries=1000)

        run_threads(visitor)

        # A write made without reading the cell is compared with the cell's value in the
        # transaction's snapshot: there is no "last writer wins".
        # Two creators of one cell conflict, and so do two blind writers that change it (a blind
        # write of the value the cell already holds would not: a precondition compares values).
        for label, write, values in (("creator", lambda tr, v: tr.new_mutable("note", v), ("a", "b")),
                                     ("blind writer", lambda tr, v: tr.set_mutable("note", v), ("x", "y"))):
            blind_a, blind_b = db.new_transaction(), db.new_transaction()
            write(blind_a, values[0])
            write(blind_b, values[1])
            blind_a.commit()
            try:
                blind_b.commit()
                raise AssertionError(f"the second {label} of one cell should have conflicted")
            except ProtoConflictError as error:
                print(f"second {label} of a cell refused (unit {error.unit.kind.name} {error.unit.key!r})")
                assert error.unit.key == "note"

        tr = db.new_transaction()
        counter = tr.get_root_object("counter")
        events = tr.get_root_object("events")
        visits = visits_cell.get(tr)
        pages = tr.get_root_object("visits_by_page")
        home, page_0, page_1 = pages.count_of("home"), pages.count_of("page-0"), pages.count_of("page-1")
        tr.abort()
        print(f"counter = {counter} (expected {1 + THREADS * INCREMENTS})")
        print(f"events  = {len(events)} keys (expected {THREADS * INCREMENTS})")
        print(f"visits  = {visits} (expected {THREADS * INCREMENTS})")
        print(f"pages   = home {home}, page-0 {page_0}, page-1 {page_1} "
              f"(expected {THREADS * INCREMENTS}, {THREADS * INCREMENTS // 2} each)")
        assert counter == 1 + THREADS * INCREMENTS
        assert len(events) == THREADS * INCREMENTS
        assert visits == THREADS * INCREMENTS
        assert home == THREADS * INCREMENTS and page_0 == page_1 == THREADS * INCREMENTS // 2
    finally:
        space.close()
    print("transactions_concurrency: OK")


if __name__ == "__main__":
    main()
