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
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, snapshot_of, write_token

pytestmark = pytest.mark.timeout(60)

FAST = dict(lease_duration=0.9, request_timeout=0.1, startup_grace=0.0,
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


def _record_replications(node, log):
    """Log ``"replicate"`` when the node sends a replication (D50: the commit's round)."""
    original = node._broadcast

    def broadcast(msg):
        if msg["type"] == "replicate":
            log.append("replicate")
        return original(msg)

    node._broadcast = broadcast


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


def test_records_reach_the_storage_before_the_replication_and_no_root_is_published_under_it():
    with Cluster(1, **FAST) as cl:
        log = []
        storage = RecordingStorage(MemoryStorage(), log)
        space = ObjectSpace(storage, coordinator=cl["n0"])
        db = space.new_database("db")
        _record_replications(cl["n0"], log)
        _value(space)                                  # the first snapshot confirms under a lease
        space.sync(timeout=5)
        log.clear()
        storage.publishers.clear()

        def write(tr):
            d = tr.new_dictionary().set_at("k", tr.new_object(x=1))
            tr.set_root_object("d", d)

        db.run(write)
        before = log[:log.index("replicate")]
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
        cl.wait_rejoined()      # every first start done first (a late member waits, R51)
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
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
        db = spaces[0].new_database("db")
        db.run(_incr, **RUN_KW)
        commits = cl["n0"].stats()["replications"] - cl["n0"].stats()["replication_failures"]
        assert commits >= 2
        # A commit returns once a majority acknowledged: the committer and at least one peer.
        # The other peer's acknowledgement may still be on its way.
        peers = ("n1", "n2")
        assert sum(cl[n].stats()["accepted"] for n in peers) >= commits * (cl["n0"].majority - 1)
        # Every replication reaches both peers eventually, but a straggler copy may be refused:
        # the peer may already have granted the committer's next lease (the ballot rule).
        assert _eventually(lambda: all(cl[n].stats()["accepted"] + cl[n].stats()["refused"] == commits
                                       for n in peers))
        cl.crash("n2")                                           # a minority down: commits go on
        db.run(_incr, **RUN_KW)
        assert _value(spaces[1]) == 2
        cl.crash("n1")                                           # a majority down: no commit
        # D50: n0 may still own its lease term (renewed while it had demand), so its first
        # attempt can replicate and find no majority (unknown outcome, never retried); the
        # term is then abandoned and no majority grants a new lease.
        try:
            spaces[0].open_database("db").run(_incr, retries=0)
        except (ProtoOutcomeUnknownError, ProtoTimeoutError):
            pass
        else:
            pytest.fail("a commit succeeded without a majority")
        with pytest.raises(ProtoTimeoutError):              # no majority grants the lease
            spaces[0].open_database("db").run(_incr, retries=0)


def test_a_holder_paused_past_its_lease_never_replicates():
    with Cluster(3, **FAST) as cl:
        shared = MemoryStorage()
        s0 = ObjectSpace(shared, coordinator=cl["n0"])
        s1 = ObjectSpace(shared, coordinator=cl["n1"])
        ObjectSpace(shared, coordinator=cl["n2"])        # a new cluster rejoins once every member answers
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
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
        ObjectSpace(shared, coordinator=cl["n2"])        # a new cluster rejoins once every member answers
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
        s0.new_database("db")
        _value(s1)
        late = FAST["lease_duration"] * 1.5
        original = cl["n0"].replicate

        def slow_replicate(lease, root, **kw):
            cl.net.set_link_delay("n0", "n1", late)
            cl.net.set_link_delay("n0", "n2", late)
            try:
                return original(lease, root, **kw)
            finally:
                cl.net.set_link_delay("n0", "n1", 0)
                cl.net.set_link_delay("n0", "n2", 0)

        cl["n0"].replicate = slow_replicate
        outcome = []
        tokens = CommitTokens()

        def token_incr(token):
            def fn(tr):
                _incr(tr)
                write_token(tr, token)
            return fn

        def first():
            try:
                s0.open_database("db").run(token_incr("n0-late"), retries=0)
                outcome.append("committed")
            except ProtoOutcomeUnknownError:
                outcome.append("unknown")

        t = threading.Thread(target=first)
        t.start()
        time.sleep(FAST["lease_duration"] * 1.1)
        s1.open_database("db").run(token_incr("n1-next"), **RUN_KW)
        tokens.acked("n1-next")
        t.join(10)
        time.sleep(late)                                  # the delayed replicates arrive now
        assert outcome == ["unknown"]
        tokens.unknown("n0-late")
        # n0's root may or may not have taken effect (n0 accepted it itself and may have
        # reported it when granting n1's lease).  A count of 1 alone could also be n1's
        # acknowledged increment lost and n0's applied (Task 8.3n): the tokens tell them apart,
        # on both members, with the count read in the same snapshot.
        res = assert_outcomes_resolved(
            {n: (lambda s=s: snapshot_of(s.open_database("db"), lambda tr: tr.get_root_object("n")))
             for n, s in (("n0", s0), ("n1", s1))}, tokens)
        after = len(res.present)
        assert set(res.values.values()) == {after}, res
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
        ObjectSpace(shared, coordinator=cl["n2"])        # n2 grants and acknowledges with a tier only
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
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
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
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


#: The message types that tell a member about another member's commit without its asking.
_UNPROMPTED = ("done", "release", "replicate", "store", "propose", "warm", "records")


def test_sync_promises_local_persistence_and_a_commit_on_the_reader_sees_every_acknowledged_commit(tmp_path):
    """Task 8.3n, ruling proposal on R113: ``sync()`` waits until what THIS member knows is
    persisted; it does not make another member's acknowledged commits visible here.  The
    public way to read every commit acknowledged anywhere before the read starts is a commit
    on the reading member first: its base is at or above every acknowledged commit (the chain
    rule, else that commit would be lost), and once it is acknowledged the member's committed
    root is its own.  Deterministic: the reader drops every message that would tell it of the
    other members' commits unprompted, and the object store is out (so sync() cannot learn a
    newer persisted root either)."""
    from tests.support.tiered_cluster import TieredCluster, incr, RUN_KW as TRUN_KW
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        tc.db("n0").run(incr, **TRUN_KW)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        assert tc.eventually("n2", 1)
        reader = tc["n2"]
        route = reader._route
        reader._route = lambda msg: None if msg["type"] in _UNPROMPTED else route(msg)
        tc.store.set_outage(True)
        try:
            tc.db("n0").run(incr, **TRUN_KW)                     # acknowledged by n0 and n1
            tc.spaces["n2"].sync(timeout=5)                   # returns: n2's knowledge is persisted
            assert tc.value("n2") == 1, "precondition: n2 has not learnt the commit"

            def barrier(tr):
                tr.set_root_object("barrier", (tr.get_root_object("barrier") or 0) + 1)
            tc.db("n2").run(barrier, **TRUN_KW)
            assert tc.value("n2") == 2                         # every acknowledged commit is visible
        finally:
            reader._route = route
            tc.store.set_outage(False)
