"""Transactions committing under the cluster lease (Task 8.3c, D44, D45)."""
import threading

import pytest

from proto_db.errors import ProtoConflictError, ProtoUnexpectedError
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.cluster import Cluster
from tests.support.fake_object_store import FakeObjectStore

pytestmark = pytest.mark.timeout(60)

FAST = dict(lease_duration=0.4, drift_margin=0.04, request_timeout=0.1, startup_grace=0.0,
            backoff=(0.001, 0.02))
RUN_KW = dict(retries=200, backoff=(0.001, 0.01))


class RecordingStorage:
    """Delegates to a storage; logs calls; counts failed conditional root writes; can pause."""

    def __init__(self, inner, log=None):
        self._inner = inner
        self.log = log if log is not None else []
        self.read_roots = 0
        self.failed_cas = 0
        self.publishes = 0
        self.pause_publish = None      # (entered Event, resume Event) for the next publish
        self.pause_flush = None        # callable run inside the next flush that follows a lease

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def push_atom(self, payload):
        self.log.append("push")
        return self._inner.push_atom(payload)

    def flush(self):
        self.log.append("flush")
        hook, self.pause_flush = self.pause_flush, None
        if hook is not None and "acquire" in self.log:
            hook()
        else:
            self.pause_flush = hook
        return self._inner.flush()

    def read_root(self):
        self.read_roots += 1
        return self._inner.read_root()

    def publish_root(self, ptr, expected_epoch):
        self.log.append("publish")
        self.publishes += 1
        pause, self.pause_publish = self.pause_publish, None
        if pause is not None:
            entered, resume = pause
            entered.set()
            resume.wait(10)
        try:
            return self._inner.publish_root(ptr, expected_epoch)
        except ProtoConflictError:
            self.failed_cas += 1
            raise


def _record_acquires(node, log):
    original = node.acquire

    def acquire(timeout=None):
        lease = original(timeout)
        log.append("acquire")
        return lease

    node.acquire = acquire


def _incr(tr):
    tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)


def _value(space):
    tr = space.open_database("db").new_transaction()
    try:
        return tr.get_root_object("n")
    finally:
        tr.abort()


def test_space_without_coordinator_has_none():
    assert ObjectSpace(MemoryStorage()).coordinator is None


def test_commits_under_a_single_node_cluster():
    with Cluster(1, **FAST) as cl:
        space = ObjectSpace(MemoryStorage(), coordinator=cl["n0"])
        db = space.new_database("db")
        for _ in range(5):
            db.run(_incr)
        assert _value(space) == 5
        assert cl["n0"].stats()["acquired"] == 6      # new_database + 5 commits


def test_segments_are_flushed_before_the_lease_is_requested():
    with Cluster(1, **FAST) as cl:
        log = []
        storage = RecordingStorage(MemoryStorage(), log)
        space = ObjectSpace(storage, coordinator=cl["n0"])
        db = space.new_database("db")
        _record_acquires(cl["n0"], log)
        log.clear()

        def write(tr):
            d = tr.new_dictionary().set_at("k", tr.new_object(x=1))
            tr.set_root_object("d", d)

        db.run(write)
        before = log[:log.index("acquire")]
        assert "push" in before and before[-1] == "flush", log
        assert log.index("publish") > log.index("acquire")


def test_transactions_read_the_propagated_root_not_the_storage():
    store = FakeObjectStore()
    with Cluster(2, **FAST) as cl:
        storages = [RecordingStorage(CloudStorage(store, backoff=(0.0, 0.001))) for _ in range(2)]
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
        spaces[0].new_database("db")
        dbs = [s.open_database("db") for s in spaces]
        for i in range(10):
            dbs[i % 2].run(_incr, **RUN_KW)
        reads = sum(s.read_roots for s in storages)
        assert _value(spaces[1]) == 10
        # One read per node to learn the first root, plus open_database; never one per commit.
        assert reads <= 6, reads
        assert sum(s.failed_cas for s in storages) == 0


def test_a_stale_holders_publish_is_rejected_by_the_storage_cas():
    shared = MemoryStorage()
    with Cluster(3, **FAST) as cl:
        stale = RecordingStorage(shared)
        fresh = RecordingStorage(shared)
        s0 = ObjectSpace(stale, coordinator=cl["n0"])
        s1 = ObjectSpace(fresh, coordinator=cl["n1"])
        s0.new_database("db")
        entered, resume = threading.Event(), threading.Event()
        stale.pause_publish = (entered, resume)
        errors = []

        def paused_commit():
            try:
                s0.open_database("db").run(_incr, **RUN_KW)
            except BaseException as e:      # reported by the assertion below
                errors.append(e)
                raise

        t = threading.Thread(target=paused_commit)
        t.start()
        assert entered.wait(5)
        s1.open_database("db").run(_incr, **RUN_KW)   # possible only once n0's lease expired
        resume.set()
        t.join(10)
        assert not errors
        assert stale.failed_cas == 1                  # the stale publish was fenced
        assert cl["n0"].stats()["fencing_events"] == 1
        assert _value(s1) == 2                        # retried on the new root: nothing lost


def test_an_expired_lease_never_reaches_the_storage():
    with Cluster(1, **FAST) as cl:
        storage = RecordingStorage(MemoryStorage())
        space = ObjectSpace(storage, coordinator=cl["n0"])
        db = space.new_database("db")
        log = storage.log
        _record_acquires(cl["n0"], log)
        log.clear()
        storage.publishes = 0
        storage.pause_flush = lambda: threading.Event().wait(FAST["lease_duration"])
        db.run(_incr, **RUN_KW)
        assert _value(space) == 1
        assert storage.publishes == 1 and storage.failed_cas == 0   # first attempt stopped before publishing
        assert log.count("acquire") == 2


# -- fix round 1 (R39) ------------------------------------------------------------------------

def test_storage_root_reads_feed_the_coordinator_so_reads_never_go_backwards():
    shared = MemoryStorage()
    with Cluster(3, **FAST) as cl:
        s0 = ObjectSpace(shared, coordinator=cl["n0"])
        s1 = ObjectSpace(shared, coordinator=cl["n1"])
        db0 = s0.new_database("db")
        db0.run(_incr, **RUN_KW)
        cl.net.partition({"n0", "n2"}, {"n1"})          # n1 misses the next commits' releases
        for _ in range(3):
            db0.run(_incr, **RUN_KW)
        cl.net.heal()
        latest = shared.read_root()
        assert cl["n1"].known_root()[1] != latest
        entries = s1.open_database("db").history()       # a storage read through n1's space
        assert cl["n1"].known_root() == (True, latest)
        assert "n" in entries[0].root_names
        assert _value(s1) == 4                           # the snapshot is at least what history() saw


class _AppliedThenFails:
    """Storage proxy whose next publish is applied, then stalls, then raises (a lost reply)."""

    def __init__(self, inner):
        self._inner = inner
        self.entered = threading.Event()
        self.resume = threading.Event()
        self.armed = False

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def publish_root(self, ptr, expected_epoch):
        record = self._inner.publish_root(ptr, expected_epoch)
        if self.armed:
            self.armed = False
            self.entered.set()
            self.resume.wait(10)
            raise ProtoUnexpectedError("the connection was lost after the root write was applied")
        return record


def test_an_applied_publish_that_errors_after_the_lease_expired_is_never_applied_twice():
    shared = MemoryStorage()          # root_lock_is_global = True: the old path would infer a conflict
    with Cluster(3, **FAST) as cl:
        flaky = _AppliedThenFails(shared)
        s0 = ObjectSpace(flaky, coordinator=cl["n0"])
        s1 = ObjectSpace(shared, coordinator=cl["n1"])
        s0.new_database("db")
        flaky.armed = True
        errors = []

        def first():
            try:
                s0.open_database("db").run(_incr, **RUN_KW)
            except ProtoUnexpectedError as e:
                errors.append(e)

        t = threading.Thread(target=first)
        t.start()
        assert flaky.entered.wait(5)
        s1.open_database("db").run(_incr, **RUN_KW)     # only after n0's lease expired
        flaky.resume.set()
        t.join(10)
        assert errors, "an error after an applied publish must not be turned into a retried conflict"
        assert _value(s1) == 2                           # n0's increment applied once, n1's once
