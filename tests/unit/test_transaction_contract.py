"""The transaction is an explicit object, never bound to a thread (rulings R72, R74b).

Pinned here: a transaction handed from one thread to another commits; transactions are used
from asyncio coroutines (several open at once on one event-loop thread); one thread keeps
several transactions open and commits them in any order; ONE transaction used concurrently
from two threads is supported -- its operations are serialized by the transaction's lock,
and every write lands; ``Database.run`` re-runs its function in a fresh transaction after a
conflict, so the retry reads the root the conflicting commit published.
"""
import asyncio
import threading

import pytest

from proto_db import MemoryStorage, ObjectSpace
from proto_db.errors import ProtoConflictError
from proto_db.storage.file import FileStorage


@pytest.fixture(params=["memory", "file"])
def db(request, tmp_path):
    storage = MemoryStorage() if request.param == "memory" else FileStorage(str(tmp_path / "s"), durability="none")
    space = ObjectSpace(storage)
    database = space.new_database("db")
    yield database
    space.close()


def _value(db, name):
    with db.new_transaction() as tr:
        return tr.get_root_object(name)


def test_a_transaction_started_in_one_thread_commits_in_another(db):
    tr = db.new_transaction()
    tr.set_root_object("a", 1)
    done = []

    def finish():
        tr.set_root_object("b", 2)
        tr.commit()
        done.append(True)
    t = threading.Thread(target=finish)
    t.start()
    t.join()
    assert done == [True] and (_value(db, "a"), _value(db, "b")) == (1, 2)


def test_transactions_are_used_from_asyncio_coroutines(db):
    async def writer(i, started):
        tr = db.new_transaction()
        tr.set_root_object(f"k{i}", i)
        started.append(i)
        await asyncio.sleep(0)            # every coroutine holds an open transaction here
        assert len(started) == 5          # all five are open at once on this thread
        tr.commit()

    async def main():
        started = []
        await asyncio.gather(*(writer(i, started) for i in range(5)))
    asyncio.run(main())
    assert [_value(db, f"k{i}") for i in range(5)] == list(range(5))


def test_one_thread_keeps_several_transactions_open(db):
    db.run(lambda tr: tr.set_root_object("n", 0))
    first, second, third = (db.new_transaction() for _ in range(3))
    first.set_root_object("x", 1)
    second.set_root_object("y", 2)
    third.set_root_object("n", third.get_root_object("n") + 1)
    third.commit()
    first.commit()
    second.commit()
    assert (_value(db, "x"), _value(db, "y"), _value(db, "n")) == (1, 2, 1)
    stale = db.new_transaction()
    stale.get_root_object("n")
    db.run(lambda tr: tr.set_root_object("n", 5))
    stale.set_root_object("n", 99)
    with pytest.raises(ProtoConflictError):
        stale.commit()                    # it read "n" before a concurrent commit changed it


def test_one_transaction_used_from_two_threads_keeps_every_write(db):
    tr = db.new_transaction()
    start = threading.Barrier(2)

    def write(prefix):
        start.wait()
        for i in range(200):
            tr.set_root_object(f"{prefix}{i}", i)
    threads = [threading.Thread(target=write, args=(p,)) for p in "ab"]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    tr.commit()
    with db.new_transaction() as check:
        assert all(check.get_root_object(f"{p}{i}") == i for p in "ab" for i in range(200))


def test_run_retries_in_a_fresh_transaction_after_a_conflict(db):
    """R74b: objects of a transaction whose commit failed are not reused; ``run`` calls the
    function again with a new transaction, which reads the newly published root."""
    db.run(lambda tr: tr.set_root_object("n", 0))
    seen, transactions = [], []

    def increment(tr):
        transactions.append(tr)
        n = tr.get_root_object("n")
        seen.append(n)
        if len(seen) == 1:                       # a concurrent commit lands before ours
            db.run(lambda other: other.set_root_object("n", 10))
        tr.set_root_object("n", n + 1)
    db.run(increment, retries=3)
    assert seen == [0, 10] and transactions[0] is not transactions[1]
    assert _value(db, "n") == 11


def test_a_nested_run_restores_the_outer_runs_retry_state(db):
    """Review M1 (Task 8.3f-B fix round 1): a ``Database.run`` inside ``fn`` reset the outer
    commit's "retries left" state, so the outer commit lost the held retries of D50."""
    import proto_db.transactions as tx
    seen = []

    def outer(tr):
        db.run(lambda inner: inner.set_root_object("inner", 1))
        seen.append(getattr(tx._RUN, "retry_left", None))
        tr.set_root_object("outer", 1)

    db.run(outer)
    assert seen == [True]
    assert getattr(tx._RUN, "retry_left", False) is False
