"""Mixed workload for a fixed duration: transfers, blind counters, readers and index queries (Task 5.2).

Roots of one database:

* ``accounts``: a ``Dictionary`` account -> balance (ints), total fixed at creation;
* ``transfers``: an indexed ``List`` of transfer records (``EqualityIndex`` on
  ``src``, ``RangeIndex`` on ``amt``), appended in the same commit as the balance
  change it describes;
* ``hits``: a ``CountedSet`` receiving blind adds (intent merge, never read).

Writers loop on ``Database.run`` transfers (read-modify-write of two roots) and
blind hits; readers loop on snapshots and check, every time, that the balances add
up to the fixed total and equal the initial balances plus the net of the transfer
log of the same snapshot, and periodically that both indexes validate and answer
exactly like a scan.  At the end the committed counts match what the workers
report.

Catches: a commit that publishes one root without the other (torn multi-root
commit), lost or duplicated transfers, a snapshot mixing two versions, stale
indexes under contention, and interference between the conflict path and the
merge path.
"""
import random
import threading
import time

import pytest

from proto_db.core.dbobject import DBObject
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.range import RangeIndex
from proto_db.query.terms import Term
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads

DURATION = 8.0
WRITERS = 6
READERS = 6
ACCOUNTS = [f"a{i:02d}" for i in range(20)]
INITIAL = 1000
RUN_KW = dict(retries=10_000, backoff=(0.002, 0.2))

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]


@pytest.fixture(params=["memory", "file"])
def space(request, tmp_path):
    storage = (MemoryStorage() if request.param == "memory"
               else FileStorage(str(tmp_path / "space"), durability="flush"))
    s = ObjectSpace(storage)
    yield s
    s.close()


def check_snapshot(tr, full: bool, rng: random.Random) -> int:
    """Invariants of one snapshot; returns its number of transfers."""
    accounts, transfers = tr.get_root_object("accounts"), tr.get_root_object("transfers")
    balances = dict(accounts.items())
    assert sorted(balances) == ACCOUNTS
    assert sum(balances.values()) == INITIAL * len(ACCOUNTS), "money created or destroyed"
    expected = dict.fromkeys(ACCOUNTS, INITIAL)
    records = list(transfers.elements())
    for rec in records:
        expected[rec.src] -= rec.amt
        expected[rec.dst] += rec.amt
    assert balances == expected, "balances disagree with the transfer log of the same snapshot"
    assert transfers.count == len(records)
    if full:
        for name, terms in (("by_src", [Term("src", "==", rng.choice(ACCOUNTS)),
                                        Term("src", "in", rng.sample(ACCOUNTS, 3))]),
                            ("by_amt", [Term("amt", "between[]", (10, 30)), Term("amt", ">", 45)])):
            index = transfers.get_index(name)
            index.validate(records)
            for term in terms:
                planned = sorted(r.oid.int for r in index.plan(term).execute())
                assert planned == sorted(r.oid.int for r in records if term.matches(r)), f"{name}: {term}"
    return len(records)


def test_mixed_workload_keeps_every_invariant(space, request):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        accounts = tr.new_dictionary()
        for name in ACCOUNTS:
            accounts = accounts.set_at(name, INITIAL)
        tr.set_root_object("accounts", accounts)
        tr.set_root_object("transfers", tr.new_list()
                           .add_index(EqualityIndex("by_src", "src"))
                           .add_index(RangeIndex("by_amt", "amt")))
        tr.set_root_object("hits", tr.new_counted_set())
    transfers_run, hits_run = Attempts(), Attempts()
    stop = threading.Event()
    reads = [0] * READERS

    def writer(w):
        rng = random.Random(w)

        def body():
            i = 0
            while not stop.is_set():
                src, dst = rng.sample(ACCOUNTS, 2)
                amt = rng.randint(1, 50)
                rec = DBObject(src=src, dst=dst, amt=amt, w=w, i=i)

                def transfer(tr):
                    acc = tr.get_root_object("accounts")
                    acc = acc.set_at(src, acc.get_at(src) - amt).set_at(dst, acc.get_at(dst) + amt)
                    tr.set_root_object("accounts", acc)
                    tr.set_root_object("transfers", tr.get_root_object("transfers").append_last(rec))

                transfers_run.run(db, transfer, **RUN_KW)
                hits_run.run(db, lambda tr: tr.set_root_object("hits", tr.new_counted_set().add(w)), **RUN_KW)
                i += 1
        return body

    def reader(r):
        rng = random.Random(1000 + r)

        def body():
            seen = 0
            while not stop.is_set():
                tr = db.new_transaction()
                try:
                    n = check_snapshot(tr, full=reads[r] % 5 == 0, rng=rng)
                finally:
                    tr.abort()
                assert n >= seen, "a later snapshot lost transfers"
                seen = n
                reads[r] += 1
        return body

    def timer():
        time.sleep(DURATION)
        stop.set()

    elapsed = run_threads([writer(w) for w in range(WRITERS)] + [reader(r) for r in range(READERS)] + [timer])
    tr = db.new_transaction()
    try:
        assert check_snapshot(tr, full=True, rng=random.Random(0)) == transfers_run.commits
        hits = tr.get_root_object("hits")
        assert hits.total_count == hits_run.commits
        assert sum(hits.count_of(w) for w in range(WRITERS)) == hits_run.commits
    finally:
        tr.abort()
    assert hits_run.conflicts == 0, hits_run
    assert transfers_run.commits > 0 and min(reads) > 0
    report(request.node.name, elapsed, f"transfers: {transfers_run}", f"hits: {hits_run}",
           f"reader snapshots {reads}")
