"""Lazy loads racing with readers and committers on a reopened FileStorage (Task 5.2).

A large indexed ``List`` of records (each record its own atom) and a ``meta`` root
``(version, count, checksum)`` are committed to a ``FileStorage``, which is then
closed and reopened, so every atom starts as an unloaded stub.  The main thread
takes one snapshot and hands the *same* stub objects to 8 reader threads, which
iterate them and query their (also lazily loaded) index at the same time, while 4
writer threads commit new versions (appending records and updating ``meta``
atomically with them).  After the shared snapshot each reader keeps taking fresh
snapshots of its own until the writers finish, spaced so that each reads a version at
least ``COMMITS_BETWEEN_SNAPSHOTS`` commits newer than the previous one, then one final
snapshot.  The reader work is thus bounded by the writers' progress, not by how fast a
snapshot is: readers re-reading in a tight loop would, with the GIL enabled, take the
interpreter lock away from the writers once snapshots are served from the shared atom
cache (D53), and re-reading an unchanged version adds no coverage.  The writers hold their last
commit until every reader has taken its first own snapshot, so each reader takes
at least two snapshots while writers are active, independently of timing.

Every reader must see a complete, consistent snapshot: the element count and the
checksum of the records it iterates equal the ``meta`` of the version it started
from, every record has all its fields, and the index answers exactly like a scan.
No thread may raise.

Catches: torn lazy loads (a stub observed half restored, or restored twice with
different state), a stub's content changing under a reader when a later version is
committed, page-cache races between reads and appends, and index stubs answering
from a partial load.
"""
import threading

import pytest

from proto_db.core.dbobject import DBObject
from proto_db.indexing.range import RangeIndex
from proto_db.query.terms import Term
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads

INITIAL = 6000
READERS = 8
WRITERS = 4
COMMITS_PER_WRITER = 40
APPEND_PER_COMMIT = 10
MOD = (1 << 61) - 1
#: Seconds a writer waits for each reader before its last commit.
READER_WAIT = 60.0
#: Spacing of a reader's own snapshots, in commits (about 10 snapshots per reader per run).
COMMITS_BETWEEN_SNAPSHOTS = WRITERS * COMMITS_PER_WRITER // 10
RUN_KW = dict(retries=10_000, backoff=(0.002, 0.2))

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]


def record(k: int, version: int) -> DBObject:
    return DBObject(k=k, v=(k * 7919 + version) % 10007, ver=version)


def record_sum(rec) -> int:
    return (rec.k * 1_000_003 + rec.v * 31 + rec.ver) % MOD


def check_snapshot(items, meta, where: str) -> int:
    """Assert ``items`` matches ``meta`` (count, checksum, complete records) and its index matches a scan."""
    version, count, checksum = meta
    total = n = 0
    ks = []
    for rec in items.elements():
        assert type(rec) is DBObject, f"{where}: element of type {type(rec).__name__}"
        assert rec.v == (rec.k * 7919 + rec.ver) % 10007, f"{where}: torn record {rec.k}"
        assert rec.ver <= version, f"{where}: record from version {rec.ver} in snapshot {version}"
        total = (total + record_sum(rec)) % MOD
        ks.append(rec.k)
        n += 1
    assert (n, items.count, total) == (count, count, checksum), f"{where}: snapshot of version {version} torn"
    index = items.get_index("by_k")
    for term in (Term("k", "between[]", (100, 140)), Term("k", ">=", count - 30), Term("k", "==", count // 2)):
        planned = sorted(r.k for r in index.plan(term).execute())
        assert planned == sorted(k for k in ks if term.matches(DBObject(k=k))), f"{where}: {term}"
    return version


def test_readers_share_lazy_stubs_while_writers_commit(tmp_path, request):
    path = str(tmp_path / "space")
    space = ObjectSpace(FileStorage(path, durability="flush"))
    db = space.new_database("db")
    with db.new_transaction() as tr:
        recs = [record(k, 0) for k in range(INITIAL)]
        items = tr.new_list().add_index(RangeIndex("by_k", "k")).extend(recs)
        tr.set_root_object("items", items)
        tr.set_root_object("meta", (0, INITIAL, sum(record_sum(r) for r in recs) % MOD))
    space.close()

    space = ObjectSpace(FileStorage(path, durability="flush"))
    try:
        db = space.open_database("db")
        tr0 = db.new_transaction()
        shared_items, shared_meta = tr0.get_root_object("items"), tr0.get_root_object("meta")
        tr0.abort()     # the stubs outlive the transaction that loaded them (D34)
        # Reading a root does not load it (D34 amendment): the readers race on the first load.
        assert object.__getattribute__(shared_items, "_loaded") is False, "the shared root must start as a stub"
        attempts = Attempts()
        state_lock = threading.Lock()
        # Signalled after every commit and when a writer ends; readers wait on it between snapshots.
        changed = threading.Condition(state_lock)
        writers_left = [WRITERS]
        commits_done = [0]
        snapshots = [0] * READERS
        first_versions = [None] * READERS
        # Set by each reader once it has checked the shared snapshot and taken one snapshot
        # of its own; the writers hold their last commit until every reader got there, so
        # every reader overlaps the writers by construction, whatever the machine's speed.
        reader_ready = [threading.Event() for _ in range(READERS)]

        def writer():
            def commit(tr):
                items, (version, count, checksum) = tr.get_root_object("items"), tr.get_root_object("meta")
                new = [record(count + j, version + 1) for j in range(APPEND_PER_COMMIT)]
                for rec in new:
                    items = items.append_last(rec)
                    checksum = (checksum + record_sum(rec)) % MOD
                tr.set_root_object("items", items)
                tr.set_root_object("meta", (version + 1, count + APPEND_PER_COMMIT, checksum))
            try:
                for i in range(COMMITS_PER_WRITER):
                    if i == COMMITS_PER_WRITER - 1:
                        for r, ready in enumerate(reader_ready):
                            assert ready.wait(READER_WAIT), f"reader {r} never reached its first snapshot"
                    attempts.run(db, commit, **RUN_KW)
                    with changed:
                        commits_done[0] += 1
                        changed.notify_all()
            finally:
                with changed:
                    writers_left[0] -= 1
                    changed.notify_all()

        def snapshot(r):
            tr = db.new_transaction()
            try:
                items, meta = tr.get_root_object("items"), tr.get_root_object("meta")
            finally:
                tr.abort()
            version = check_snapshot(items, meta, f"reader {r} snapshot")
            snapshots[r] += 1
            return version

        def reader(r):
            def body():
                try:
                    check_snapshot(shared_items, shared_meta, f"reader {r} shared snapshot")
                    first_versions[r] = snapshot(r)     # the writers' last commits are still pending
                finally:
                    reader_ready[r].set()               # also on failure: never stall the writers
                while True:
                    with changed:
                        done = writers_left[0] == 0
                        seen = commits_done[0]
                    snapshot(r)
                    if done:
                        return
                    with changed:   # the next snapshot reads a newer version (or the final one)
                        changed.wait_for(lambda: commits_done[0] >= seen + COMMITS_BETWEEN_SNAPSHOTS
                                         or writers_left[0] == 0, timeout=READER_WAIT)
            return body

        elapsed = run_threads([reader(r) for r in range(READERS)] + [writer] * WRITERS)
        # The shared snapshot is unchanged by everything committed since.
        check_snapshot(shared_items, shared_meta, "shared snapshot after the run")
        tr = db.new_transaction()
        final = check_snapshot(tr.get_root_object("items"), tr.get_root_object("meta"), "final")
        tr.abort()
        assert final == WRITERS * COMMITS_PER_WRITER
        assert attempts.commits == WRITERS * COMMITS_PER_WRITER
        assert min(snapshots) >= 2
        assert all(v < final for v in first_versions), "a reader's first snapshot did not overlap the writers"
        report(request.node.name, elapsed, attempts, f"reader snapshots {snapshots}")
    finally:
        space.close()
