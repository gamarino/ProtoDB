"""Lazy loads racing with readers and committers on a reopened FileStorage (Task 5.2).

A large indexed ``List`` of records (each record its own atom) and a ``meta`` root
``(version, count, checksum)`` are committed to a ``FileStorage``, which is then
closed and reopened, so every atom starts as an unloaded stub.  The main thread
takes one snapshot and hands the *same* stub objects to 8 reader threads, which
iterate them and query their (also lazily loaded) index at the same time, while 4
writer threads commit new versions (appending records and updating ``meta``
atomically with them).  After the shared snapshot each reader keeps taking fresh
snapshots of its own until the writers finish.

Every reader must see a complete, consistent snapshot: the element count and the
checksum of the records it iterates equal the ``meta`` of the version it started
from, every record has all its fields, and the index answers exactly like a scan.
No thread may raise.

Catches: torn lazy loads (a stub observed half restored, or restored twice with
different state), a stub's content changing under a reader when a later version is
committed, page-cache races between reads and appends, and index stubs answering
from a partial load.
"""
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
        writers_left = [WRITERS]
        snapshots = [0] * READERS

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
                for _ in range(COMMITS_PER_WRITER):
                    attempts.run(db, commit, **RUN_KW)
            finally:
                with attempts._lock:
                    writers_left[0] -= 1

        def reader(r):
            def body():
                check_snapshot(shared_items, shared_meta, f"reader {r} shared snapshot")
                while True:
                    with attempts._lock:
                        done = writers_left[0] == 0
                    tr = db.new_transaction()
                    try:
                        items, meta = tr.get_root_object("items"), tr.get_root_object("meta")
                    finally:
                        tr.abort()
                    check_snapshot(items, meta, f"reader {r} snapshot")
                    snapshots[r] += 1
                    if done:
                        return
            return body

        elapsed = run_threads([reader(r) for r in range(READERS)] + [writer] * WRITERS)
        # The shared snapshot is unchanged by everything committed since.
        check_snapshot(shared_items, shared_meta, "shared snapshot after the run")
        tr = db.new_transaction()
        final = check_snapshot(tr.get_root_object("items"), tr.get_root_object("meta"), "final")
        tr.abort()
        assert final == WRITERS * COMMITS_PER_WRITER
        assert attempts.commits == WRITERS * COMMITS_PER_WRITER
        assert min(snapshots) >= 2, "every reader must take snapshots while the writers commit"
        report(request.node.name, elapsed, attempts, f"reader snapshots {snapshots}")
    finally:
        space.close()
