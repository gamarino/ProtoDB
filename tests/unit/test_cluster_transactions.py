"""Transactions committing under the cluster lease (Tasks 8.3c and 8.3d, D44, D45, D46)."""
import threading
import time

import pytest

from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError, ProtoValidationError
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
        self.publishers = []
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
        if not threading.current_thread().name.startswith("proto-persister-"):
            self.read_roots += 1           # reads made for transactions, not by the persister
        return self._inner.read_root()

    def publish_root(self, ptr, expected_epoch):
        self.log.append("publish")
        self.publishers.append(threading.current_thread().name)
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


def _eventually(predicate, timeout=5.0):
    """Poll ``predicate``: another node learns a commit when its release arrives."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


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
        assert cl["n0"].stats()["critical_commits"] == 6      # new_database + 5 commits


def test_records_reach_the_storage_before_the_lease_and_no_root_is_published_under_it():
    with Cluster(1, **FAST) as cl:
        log = []
        storage = RecordingStorage(MemoryStorage(), log)
        space = ObjectSpace(storage, coordinator=cl["n0"])
        db = space.new_database("db")
        _record_acquires(cl["n0"], log)
        _value(space)                                  # the first snapshot confirms under a lease
        space.sync(timeout=5)
        log.clear()
        storage.publishers.clear()

        def write(tr):
            d = tr.new_dictionary().set_at("k", tr.new_object(x=1))
            tr.set_root_object("d", d)

        db.run(write)
        before = log[:log.index("acquire")]
        assert "push" in before and before[-1] == "flush", log
        space.sync(timeout=5)
        # The root reached the storage only through the persister thread, after the commit.
        assert storage.publishers and all(name.startswith("proto-persister-") for name in storage.publishers)
        assert space.persisted_epoch == space.committed_epoch == 2


def test_an_object_store_needs_the_tiered_storage_in_cluster_mode():
    with Cluster(1, **FAST) as cl:
        with pytest.raises(ProtoValidationError, match="TieredStorage"):
            ObjectSpace(CloudStorage(FakeObjectStore()), coordinator=cl["n0"])


def test_transactions_read_the_committed_root_not_the_storage():
    shared = MemoryStorage()
    with Cluster(2, **FAST) as cl:
        storages = [RecordingStorage(shared) for _ in range(2)]
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
        spaces[0].new_database("db")
        dbs = [s.open_database("db") for s in spaces]
        for i in range(10):
            dbs[i % 2].run(_incr, **RUN_KW)
        reads = sum(s.read_roots for s in storages)
        assert _value(spaces[1]) == 10
        # One read to learn the first root (the space was empty); never one per commit.
        assert reads <= 4, reads
        assert sum(s.failed_cas for s in storages) == 0


def test_the_commit_point_is_the_majority_acknowledgement():
    with Cluster(3, **FAST) as cl:
        shared = MemoryStorage()
        spaces = [ObjectSpace(shared, coordinator=cl[n], lock_timeout=1.0) for n in cl.ids]
        db = spaces[0].new_database("db")
        db.run(_incr, **RUN_KW)
        stats = [cl[n].stats() for n in cl.ids]
        assert sum(s["accepted"] for s in stats) >= 2 * 2        # both commits reached the two peers
        cl.crash("n2")                                           # a minority down: commits go on
        db.run(_incr, **RUN_KW)
        assert _value(spaces[1]) == 2
        cl.crash("n1")                                           # a majority down: no commit
        with pytest.raises(ProtoTimeoutError):              # no majority grants the lease
            spaces[0].open_database("db").run(_incr, retries=0)


def test_a_holder_paused_past_its_lease_never_replicates():
    with Cluster(3, **FAST) as cl:
        shared = MemoryStorage()
        s0 = ObjectSpace(shared, coordinator=cl["n0"])
        s1 = ObjectSpace(shared, coordinator=cl["n1"])
        s0.new_database("db")
        entered, resume = threading.Event(), threading.Event()
        original = cl["n0"].tier.outgoing
        armed = [True]

        def paused_outgoing():
            if armed[0]:
                armed[0] = False
                entered.set()
                resume.wait(10)
            return original()

        cl["n0"].tier.outgoing = paused_outgoing
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
        s1.open_database("db").run(_incr, **RUN_KW)     # possible only once n0's lease expired
        resume.set()
        t.join(10)
        assert not errors
        assert _eventually(lambda: _value(s1) == 2)     # n0 retried on the new root: nothing lost
        assert cl["n0"].stats()["replication_failures"] == 0


def test_a_late_replication_is_refused_by_peers_that_granted_a_newer_term():
    """The ballot rule: n0's replicate reaches the peers only after they granted n1's lease."""
    with Cluster(3, **FAST) as cl:
        shared = MemoryStorage()
        s0 = ObjectSpace(shared, coordinator=cl["n0"])
        s1 = ObjectSpace(shared, coordinator=cl["n1"])
        s0.new_database("db")
        _value(s1)
        late = FAST["lease_duration"] * 1.5
        original = cl["n0"].replicate

        def slow_replicate(lease, root):
            cl.net.set_link_delay("n0", "n1", late)
            cl.net.set_link_delay("n0", "n2", late)
            try:
                return original(lease, root)
            finally:
                cl.net.set_link_delay("n0", "n1", 0)
                cl.net.set_link_delay("n0", "n2", 0)

        cl["n0"].replicate = slow_replicate
        outcome = []

        def first():
            try:
                s0.open_database("db").run(_incr, retries=0)
                outcome.append("committed")
            except ProtoOutcomeUnknownError:
                outcome.append("unknown")

        t = threading.Thread(target=first)
        t.start()
        time.sleep(FAST["lease_duration"] * 1.1)
        s1.open_database("db").run(_incr, **RUN_KW)
        t.join(10)
        time.sleep(late)                                  # the delayed replicates arrive now
        assert outcome == ["unknown"]
        # n0's root may or may not have taken effect (n0 accepted it itself and may have
        # reported it when granting n1's lease), but never both increments lost or doubled.
        after = _value(s1)
        assert after in (1, 2)
        assert sum(cl[n].stats()["refused"] for n in ("n1", "n2")) >= 1   # the late copies
        cl["n0"].replicate = original
        s0.open_database("db").run(_incr, **RUN_KW)
        assert _value(s0) == after + 1
        assert _eventually(lambda: _value(s1) == after + 1)


def test_storage_root_reads_never_make_reads_go_backwards():
    shared = MemoryStorage()
    with Cluster(3, **FAST) as cl:
        s0 = ObjectSpace(shared, coordinator=cl["n0"])
        s1 = ObjectSpace(shared, coordinator=cl["n1"])
        db0 = s0.new_database("db")
        db0.run(_incr, **RUN_KW)
        assert _value(s1) == 1
        cl.net.partition({"n0", "n2"}, {"n1"})          # n1 misses the next commits
        for _ in range(3):
            db0.run(_incr, **RUN_KW)
        assert _value(s1) == 1                           # stale but committed: never backwards
        cl.net.heal()
        s0.sync(timeout=5)
        db0.run(_incr, **RUN_KW)                         # its release reaches n1
        deadline = time.monotonic() + 5
        while _value(s1) != 5 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert _value(s1) == 5
        entries = s1.open_database("db").history()
        assert entries[0].version == 5


def test_sync_waits_for_the_persisted_epoch_and_exposes_both_epochs():
    shared = MemoryStorage()
    with Cluster(3, **FAST) as cl:
        spaces = [ObjectSpace(shared, coordinator=cl[n]) for n in cl.ids]
        db = spaces[0].new_database("db")
        for i in range(5):
            spaces[i % 3].open_database("db").run(_incr, **RUN_KW)
        spaces[1].sync(timeout=5)                         # the last committer
        assert spaces[1].persisted_epoch >= spaces[1].committed_epoch == 6
        plain = ObjectSpace(shared)                       # a reader without the cluster
        assert _value(plain) == 5
        root = shared.read_root()
        assert _eventually(lambda: spaces[0].committed_epoch == 6)
        tr = db.new_transaction()
        assert tr._epoch == 6
        tr.abort()
        assert root is not None
