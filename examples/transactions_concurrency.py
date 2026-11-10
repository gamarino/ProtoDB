#!/usr/bin/env python3
"""Transactions under concurrency: snapshot isolation, conflicts, retries and merges.

Run from the repository root::

    python examples/transactions_concurrency.py

Eight threads update the same database at once.  The script shows the three ways
ProtoDB resolves concurrent commits and checks that no update is lost:

1. A read-modify-write of a root raises ``ProtoConflictError`` when another commit
   changed that root first; ``Database.run`` retries the whole function on a fresh
   snapshot.
2. A collection written without being read (a "blind" write) is merged: its
   operations are replayed onto the current version, so concurrent writers of
   different keys never conflict.
3. A ``MutableObject`` names a mutable cell outside the roots, with the same
   conflict detection.
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
    """Show conflicts, retried read-modify-writes, blind merges and a mutable counter."""
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("counters")
        with db.new_transaction() as tr:
            tr.set_root_object("counter", 0)
            visits_cell = tr.new_mutable("visits", 0)  # a MutableObject handle

        # 1. Two transactions read the same snapshot; the second commit is a conflict.
        first, second = db.new_transaction(), db.new_transaction()
        first.set_root_object("counter", first.get_root_object("counter") + 1)
        second.set_root_object("counter", second.get_root_object("counter") + 1)
        first.commit()
        try:
            second.commit()
            raise AssertionError("the second commit should have conflicted")
        except ProtoConflictError:
            print("conflict detected: the second read-modify-write was not applied")

        # Database.run retries the function on a fresh snapshot until it commits.
        def increment(tr):
            tr.set_root_object("counter", tr.get_root_object("counter") + 1)

        def worker(_n):
            for _ in range(INCREMENTS):
                db.run(increment, retries=1000)

        run_threads(worker)

        # 2. Blind writes: each thread adds its own keys to a dictionary it never reads.
        def blind_writer(n):
            for i in range(INCREMENTS):
                with db.new_transaction() as tr:
                    tr.set_root_object("events", tr.new_dictionary().set_at(f"t{n}-{i}", i))

        run_threads(blind_writer)

        # 3. A mutable cell, incremented through its handle.
        def visit(tr):
            visits_cell.set(tr, visits_cell.get(tr) + 1)

        def visitor(_n):
            for _ in range(INCREMENTS):
                db.run(visit, retries=1000)

        run_threads(visitor)

        tr = db.new_transaction()
        counter = tr.get_root_object("counter")
        events = tr.get_root_object("events")
        visits = visits_cell.get(tr)
        tr.abort()
        print(f"counter = {counter} (expected {1 + THREADS * INCREMENTS})")
        print(f"events  = {len(events)} keys (expected {THREADS * INCREMENTS})")
        print(f"visits  = {visits} (expected {THREADS * INCREMENTS})")
        assert counter == 1 + THREADS * INCREMENTS
        assert len(events) == THREADS * INCREMENTS
        assert visits == THREADS * INCREMENTS
    finally:
        space.close()
    print("transactions_concurrency: OK")


if __name__ == "__main__":
    main()
