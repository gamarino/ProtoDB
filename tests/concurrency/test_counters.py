"""Counters under real parallelism: no lost update, through both commit paths (Task 5.2).

32 threads x 200 ``Database.run`` increments must end at exactly 6 400, on
``MemoryStorage`` and ``FileStorage``, for:

* a plain ``int`` root read and rewritten (read set -> conflicts -> retries);
* the key ``"n"`` of a ``Dictionary`` root, read-modify-write (same conflict path:
  the key's precondition);
* a ``CountedSet`` root receiving adds through its linked root (no precondition: the
  increments compose, D26): no attempt may conflict;
* one key of a ``RepeatedKeysDictionary`` root receiving adds of distinct and shared
  records through its linked root: the final bucket is the multiset union of every add.

Each test also checks the version chain (one ``DatabaseRoot`` per commit) and
reports runtime and conflict rate.

Catches: lost updates (a commit published over a concurrent one), a read set that
misses a change, a reapplication that drops or duplicates an intention, and deadlocks
between the root lock and atom stripes.
"""
from collections import Counter

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


def test_counted_set_linked_adds_compose_without_conflicts(space, request):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("hits", tr.new_counted_set().add("seed"))
    attempts = Attempts()

    def hit(tr):
        tr.set_root_object("hits", tr.get_root_object("hits").add("hit"))

    elapsed = _hammer(db, hit, attempts)
    hits = _read(db, lambda tr: tr.get_root_object("hits"))
    assert hits.count_of("hit") == TOTAL
    assert hits.count_of("seed") == 1
    assert hits.total_count == TOTAL + 1 and hits.count == 2
    assert attempts.commits == TOTAL
    # Only the root's presence and class were read: every commit composes its increment, none conflicts.
    assert attempts.conflicts == 0, attempts
    _check_versions(db, 1)
    report(request.node.name, elapsed, attempts)


def test_repeated_keys_linked_adds_compose(space, request):
    """Concurrent adds to ONE key of a RepeatedKeysDictionary all survive (0.1 test_102/097/098).

    Every thread commits its own distinct records plus one record shared by all, so the final
    bucket must be the multiset union of every add: each distinct record once, the shared one
    TOTAL times, and no other key touched.
    """
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("tags", tr.new_repeated_keys_dictionary().set_at("other", "seed"))
    attempts = Attempts()

    def worker_for(t):
        def worker():
            for i in range(PER_THREAD):
                def add(tr, i=i):
                    tr.set_root_object("tags", tr.get_root_object("tags")
                                       .set_at("k", (t, i)).set_at("k", "shared"))
                attempts.run(db, add, **RUN_KW)
        return worker

    elapsed = run_threads([worker_for(t) for t in range(THREADS)])
    tags = _read(db, lambda tr: tr.get_root_object("tags"))
    expected = Counter({(t, i): 1 for t in range(THREADS) for i in range(PER_THREAD)})
    expected["shared"] = TOTAL
    assert Counter(dict(tags.get_at("k").items())) == expected
    assert dict(tags.get_at("other").items()) == {"seed": 1}
    assert sorted(tags.keys()) == ["k", "other"]
    assert attempts.commits == TOTAL
    # No record was read: every commit composes its intentions (D26), none conflicts.
    assert attempts.conflicts == 0, attempts
    _check_versions(db, 1)
    report(request.node.name, elapsed, attempts)


FSYNC_THREADS = 8
FSYNC_PER_THREAD = 50


def test_plain_int_counter_with_fsync_durability(tmp_path, request):
    """A smaller counter on FileStorage's default durability (``"fsync"``): exactly 8 x 50."""
    space = ObjectSpace(FileStorage(str(tmp_path / "space")))
    try:
        assert space.storage.durability == "fsync"
        db = space.new_database("db")
        with db.new_transaction() as tr:
            tr.set_root_object("n", 0)
        attempts = Attempts()

        def increment(tr):
            tr.set_root_object("n", tr.get_root_object("n") + 1)

        def worker():
            for _ in range(FSYNC_PER_THREAD):
                attempts.run(db, increment, **RUN_KW)

        elapsed = run_threads([worker] * FSYNC_THREADS)
        total = FSYNC_THREADS * FSYNC_PER_THREAD
        assert _read(db, lambda tr: tr.get_root_object("n")) == total
        assert attempts.commits == total
        assert db.history()[0].version == 1 + total
        report(request.node.name, elapsed, attempts)
    finally:
        space.close()
