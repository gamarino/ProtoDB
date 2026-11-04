"""Group commit on a standalone storage (Task 8.3f item 3, ruling R71).

Commits queued while another commit holds the root lock are decided together: one thread
builds each on the root the previous one produced and makes the last root durable with one
flush and one publish.  Checks: every acknowledged commit is in the published state and in
the version chain (one ``DatabaseRoot`` per commit); a group publishes fewer roots than it
commits (the slow first flush lets the others queue); a conflict inside a group is detected
as between sequential commits and ``Database.run`` retries it; an error of the round (the
publish) reaches every commit of the group, and nothing of it is visible.
"""
import threading
import time

import pytest

from proto_db.errors import ProtoConflictError, ProtoUnexpectedError
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

THREADS = 8


def _space(tmp_path):
    space = ObjectSpace(FileStorage(str(tmp_path / "s"), durability="fsync"))
    space.new_database("db")
    return space


def _slow_first_flush(storage, seconds=0.2):
    """Delay the next flush so that the other committers queue behind it."""
    original = storage.flush
    calls = []

    def flush():
        calls.append(1)
        if len(calls) == 1:
            time.sleep(seconds)
        original()
    storage.flush = flush
    return calls


def _together(fn):
    start = threading.Barrier(THREADS)
    errors = []

    def worker(i):
        start.wait()
        try:
            fn(i)
        except Exception as e:              # reported by the caller's assertion
            errors.append(e)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def test_queued_commits_share_one_flush_and_one_publish(tmp_path):
    space = _space(tmp_path)
    db = space.open_database("db")
    epoch_before = space.storage.read_root().epoch
    versions_before = len(db.history())
    flushes = _slow_first_flush(space.storage)
    errors = _together(lambda i: db.run(lambda tr: tr.set_root_object(f"k{i}", i)))
    assert errors == []
    published = space.storage.read_root().epoch - epoch_before
    assert 1 <= published < THREADS, (published, len(flushes))
    assert len(db.history()) - versions_before == THREADS       # one version per commit
    with db.new_transaction() as tr:
        assert [tr.get_root_object(f"k{i}") for i in range(THREADS)] == list(range(THREADS))
    space.close()
    reopened = ObjectSpace(FileStorage(str(tmp_path / "s")))
    with reopened.open_database("db").new_transaction() as tr:
        assert [tr.get_root_object(f"k{i}") for i in range(THREADS)] == list(range(THREADS))
    reopened.close()


def test_a_conflict_inside_a_group_is_detected_and_retried(tmp_path):
    space = _space(tmp_path)
    db = space.open_database("db")
    db.run(lambda tr: tr.set_root_object("n", 0))
    _slow_first_flush(space.storage)
    conflicts = []

    def increment(tr):
        tr.set_root_object("n", tr.get_root_object("n") + 1)

    def worker(i):
        while True:
            tr = db.new_transaction()
            increment(tr)
            try:
                tr.commit()
                return
            except ProtoConflictError:
                conflicts.append(i)
    assert _together(worker) == []
    with db.new_transaction() as tr:
        assert tr.get_root_object("n") == THREADS                    # no lost update
    assert conflicts, "the threads read one snapshot: all but one of a group must conflict"
    space.close()


def test_a_failed_publish_fails_every_commit_of_the_group(tmp_path):
    space = _space(tmp_path)
    db = space.open_database("db")
    storage = space.storage
    epoch_before = storage.read_root().epoch
    original_flush, original_publish = storage.flush, storage.publish_root
    gate = threading.Event()

    def flush():
        gate.wait(0.2)
        original_flush()

    def publish_root(ptr, expected_epoch):
        raise ProtoUnexpectedError("injected publish failure")
    storage.flush, storage.publish_root = flush, publish_root
    errors = _together(lambda i: db.run(lambda tr: tr.set_root_object(f"k{i}", i), retries=0))
    assert len(errors) == THREADS and all("injected publish failure" in str(e) for e in errors)
    storage.flush, storage.publish_root = original_flush, original_publish
    assert storage.read_root().epoch == epoch_before
    with db.new_transaction() as tr:
        assert all(tr.get_root_object(f"k{i}") is None for i in range(THREADS))
    db.run(lambda tr: tr.set_root_object("after", 1))           # the storage still commits
    space.close()


def test_an_unexpected_error_of_one_commit_fails_only_that_commit(tmp_path):
    """Review M2 (Task 8.3f-B fix round 1): a non-ProtoDB exception raised by one commit's
    build failed every undecided commit of the round with a copy of it.  Now only that
    commit gets it; the others are published together."""
    import proto_db.transactions as tx
    storage = FileStorage(str(tmp_path / "raw"), durability="none")
    group = tx._Group()
    pointers = []

    def good(current, stamp):
        ptr = storage.push_blob(b"commit")
        pointers.append(ptr)
        return ptr

    def bad(current, stamp):
        raise TypeError("a bug in one commit")

    entries = [tx._Entry(b, storage, group) for b in (good, bad, good)]
    tx._standalone_round(storage, None, entries)
    try:
        assert [e.result for e in entries] == [True, None, True]
        assert isinstance(entries[1].error, TypeError) and entries[0].error is None and entries[2].error is None
        root = storage.read_root()
        assert root.epoch == 1 and (root.segment, root.offset) == (pointers[-1].segment, pointers[-1].offset)
    finally:
        storage.close()
