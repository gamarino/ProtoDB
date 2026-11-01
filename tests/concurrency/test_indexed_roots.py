"""Indexed collection roots written by many threads (Task 5.2).

* 16 threads append records to an indexed ``List`` root.  ``List`` is not
  ``ConcurrentOptimized``, so every append is a read-modify-write that conflicts and
  retries.  The final list holds every record exactly once, each thread's records in
  its own order, and both indexes validate and answer exactly like a scan.
* 16 threads blind ``set_at`` into an indexed ``Dictionary`` root (written, never
  read: intent merge, D33), over disjoint keys and keys shared by all threads.
  Disjoint keys hold exactly what their writer wrote; a shared key holds one of the
  values written to it; the indexes validate and match a scan; no attempt conflicts.

Catches: lost appends, duplicated records from a retried attempt, an index updated
by one interleaving but not another (stale or doubled entries), and a blind merge
that bypasses the current root's indexes.
"""
import pytest

from proto_db.core.dbobject import DBObject
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.range import RangeIndex
from proto_db.query.terms import Term
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads

THREADS = 16
RUN_KW = dict(retries=10_000, backoff=(0.002, 0.2))

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]


@pytest.fixture(params=["memory", "file"])
def space(request, tmp_path):
    storage = (MemoryStorage() if request.param == "memory"
               else FileStorage(str(tmp_path / "space"), durability="flush"))
    s = ObjectSpace(storage)
    yield s
    s.close()


def _read(db, name):
    tr = db.new_transaction()
    try:
        return tr.get_root_object(name)
    finally:
        tr.abort()


def assert_index_matches_scan(coll, index_name, terms):
    """The index validates against the elements and every term's plan returns the scan's answer."""
    elements = list(coll.elements())
    index = coll.get_index(index_name)
    index.validate(elements)
    for term in terms:
        assert index.supports(term)
        planned = sorted(e.oid.int for e in index.plan(term).execute())
        scanned = sorted(e.oid.int for e in elements if term.matches(e))
        assert planned == scanned, f"{index_name}: {term} plan differs from scan"


PER_THREAD_APPENDS = 40


def test_appends_to_an_indexed_list(space, request):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("log", tr.new_list()
                           .add_index(EqualityIndex("by_t", "t"))
                           .add_index(RangeIndex("by_seq", "seq")))
    attempts = Attempts()

    def worker(t):
        def body():
            for i in range(PER_THREAD_APPENDS):
                rec = DBObject(t=t, i=i, seq=t * 1000 + i)
                attempts.run(db, lambda tr: tr.set_root_object(
                    "log", tr.get_root_object("log").append_last(rec)), **RUN_KW)
        return body

    elapsed = run_threads([worker(t) for t in range(THREADS)])
    log = _read(db, "log")
    records = [(e.t, e.i) for e in log.elements()]
    assert log.count == len(records) == THREADS * PER_THREAD_APPENDS
    assert sorted(records) == [(t, i) for t in range(THREADS) for i in range(PER_THREAD_APPENDS)]
    for t in range(THREADS):
        assert [i for tt, i in records if tt == t] == list(range(PER_THREAD_APPENDS)), \
            f"thread {t}'s appends are out of order"
    assert attempts.commits == THREADS * PER_THREAD_APPENDS
    assert_index_matches_scan(log, "by_t", [Term("t", "==", t) for t in range(THREADS)]
                              + [Term("t", "in", [0, 5, 15]), Term("t", "==", 99)])
    assert_index_matches_scan(log, "by_seq", [Term("seq", "between[]", (2000, 5039)),
                                              Term("seq", ">", 14_000), Term("seq", "<", 10)])
    report(request.node.name, elapsed, attempts)


PER_THREAD_SETS = 60
SHARED_KEYS = 10


def test_blind_set_at_on_an_indexed_dictionary(space, request):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary()
                           .add_index(EqualityIndex("by_t", "t"))
                           .add_index(RangeIndex("by_i", "i"))
                           .set_at("seed", DBObject(t=-1, i=-1)))
    attempts = Attempts()
    written: dict = {}      # shared key -> set of (t, i) written to it

    def worker(t):
        def body():
            for i in range(PER_THREAD_SETS):
                own_key, value = f"t{t:02d}-{i:03d}", DBObject(t=t, i=i)
                shared_key, shared_value = f"shared-{i % SHARED_KEYS}", DBObject(t=t, i=i)
                attempts.run(db, lambda tr: tr.set_root_object(
                    "d", tr.new_dictionary().set_at(own_key, value).set_at(shared_key, shared_value)), **RUN_KW)
        return body

    for t in range(THREADS):
        for i in range(PER_THREAD_SETS):
            written.setdefault(f"shared-{i % SHARED_KEYS}", set()).add((t, i))
    elapsed = run_threads([worker(t) for t in range(THREADS)])
    d = _read(db, "d")
    content = {k: (v.t, v.i) for k, v in d.items()}
    expected_own = {f"t{t:02d}-{i:03d}": (t, i) for t in range(THREADS) for i in range(PER_THREAD_SETS)}
    assert {k: v for k, v in content.items() if k.startswith("t")} == expected_own
    assert content["seed"] == (-1, -1)
    shared = {k: v for k, v in content.items() if k.startswith("shared-")}
    assert set(shared) == set(written)
    for key, value in shared.items():
        assert value in written[key], f"{key} holds {value}, which nobody wrote there"
    assert d.count == len(content) == 1 + len(expected_own) + SHARED_KEYS
    assert attempts.commits == THREADS * PER_THREAD_SETS
    assert attempts.conflicts == 0, attempts
    assert_index_matches_scan(d, "by_t", [Term("t", "==", t) for t in range(-1, THREADS)])
    assert_index_matches_scan(d, "by_i", [Term("i", "between[]", (10, 20)), Term("i", ">=", 55)])
    report(request.node.name, elapsed, attempts)
