"""Counters under real parallelism: no lost update, through both commit paths (Task 5.2).

32 threads x 200 ``Database.run`` increments must end at exactly 6 400, on
``MemoryStorage`` and ``FileStorage``, for:

* a plain ``int`` root read and rewritten (read set -> conflicts -> retries);
* the key ``"n"`` of a ``Dictionary`` root, read-modify-write (same conflict path:
  the root was read, so it is never merged);
* a ``CountedSet`` root receiving blind adds (never read -> intent merge, D33):
  no attempt may conflict.

Each test also checks the version chain (one ``DatabaseRoot`` per commit) and
reports runtime and conflict rate.

Catches: lost updates (a commit published over a concurrent one), a read set that
misses a change, a merge applied to a read root, a blind merge that drops or
duplicates an intent, and deadlocks between the root lock and atom stripes.
"""
import pytest

from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads

THREADS = 32
PER_THREAD = 200
TOTAL = THREADS * PER_THREAD
RUN_KW = dict(retries=10_000, backoff=(0.002, 0.2))

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]


@pytest.fixture(params=["memory", "file"])
def space(request, tmp_path):
    if request.param == "memory":
        storage = MemoryStorage()
    else:
        # "flush" keeps 6 400 commits within the time budget; durability modes are
        # covered by the crash suite, this suite is about interleavings.
        storage = FileStorage(str(tmp_path / "space"), durability="flush")
    s = ObjectSpace(storage)
    yield s
    s.close()


def _hammer(db, fn, attempts):
    def worker():
        for _ in range(PER_THREAD):
            attempts.run(db, fn, **RUN_KW)
    return run_threads([worker] * THREADS)


def _read(db, fn):
    tr = db.new_transaction()
    try:
        return fn(tr)
    finally:
        tr.abort()


def _check_versions(db, initial_version):
    assert db.history()[0].version == initial_version + TOTAL


def test_plain_int_counter(space, request):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("n", 0)
    attempts = Attempts()

    def increment(tr):
        tr.set_root_object("n", tr.get_root_object("n") + 1)

    elapsed = _hammer(db, increment, attempts)
    assert _read(db, lambda tr: tr.get_root_object("n")) == TOTAL
    assert attempts.commits == TOTAL
    _check_versions(db, 1)
    report(request.node.name, elapsed, attempts)


def test_dictionary_key_counter(space, request):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("c", tr.new_dictionary().set_at("n", 0).set_at("other", "kept"))
    attempts = Attempts()

    def increment(tr):
        d = tr.get_root_object("c")
        tr.set_root_object("c", d.set_at("n", d.get_at("n") + 1))

    elapsed = _hammer(db, increment, attempts)
    d = _read(db, lambda tr: tr.get_root_object("c"))
    assert dict(d.items()) == {"n": TOTAL, "other": "kept"}
    assert attempts.commits == TOTAL
    _check_versions(db, 1)
    report(request.node.name, elapsed, attempts)


def test_counted_set_blind_adds_merge_without_conflicts(space, request):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("hits", tr.new_counted_set().add("seed"))
    attempts = Attempts()

    def hit(tr):
        tr.set_root_object("hits", tr.new_counted_set().add("hit"))

    elapsed = _hammer(db, hit, attempts)
    hits = _read(db, lambda tr: tr.get_root_object("hits"))
    assert hits.count_of("hit") == TOTAL
    assert hits.count_of("seed") == 1
    assert hits.total_count == TOTAL + 1 and hits.count == 2
    assert attempts.commits == TOTAL
    # Nothing was read, so the read set is empty: every commit merges, none conflicts.
    assert attempts.conflicts == 0, attempts
    _check_versions(db, 1)
    report(request.node.name, elapsed, attempts)
