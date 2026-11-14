"""Transactions committing under the cluster lease (Tasks 8.3c and 8.3d, D44, D45, D46)."""
import threading
import time

import pytest

from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError, ProtoValidationError
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support import stress
from tests.support.cluster import Cluster
from tests.support.fake_object_store import FakeObjectStore
from tests.support.outcomes import (CommitTokens, assert_outcomes_resolved, commit_once, new_database_once, snapshot_of,
                                    write_token)

pytestmark = pytest.mark.timeout(stress.budget(60))

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
        self.failed_cas_by = []        # the thread names of the failed conditional writes
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
            self.failed_cas_by.append(threading.current_thread().name)
            raise


def _eventually(predicate, timeout=5.0):
    """Poll ``predicate``: another node learns a commit when its release arrives.  Under stress
    (R142) a wait past ``timeout`` is recorded and goes on up to the stress budget."""
    started = time.monotonic()
    while not predicate():
        if time.monotonic() - started > stress.budget(timeout):
            return False
        time.sleep(0.01)
    if time.monotonic() - started > timeout:
        stress.record(f"a member learnt the commit after more than {timeout} s")
    return True


def _run(db, fn, **kw):
    """``db.run(fn, **kw)``.  Under stress (R142) a step of the scenario must take effect once
    whatever its outcome: :func:`commit_once`, so an unknown outcome or a timeout under load is
    counted and becomes another attempt instead of ending the test before its checks."""
    if not stress.ACTIVE:
        return db.run(fn, **kw)
    unknown = commit_once(db, fn, attempts=10, **{**RUN_KW, **kw})
    if unknown:
        stress.record(f"{unknown} unknown outcomes before a scenario step took effect")


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
        space.sync(timeout=stress.budget(5))
        log.clear()
        storage.publishers.clear()

        def write(tr):
            d = tr.new_dictionary().set_at("k", tr.new_object(x=1))
            tr.set_root_object("d", d)

        _run(db, write)
        before = log[:log.index("replicate")]
        assert "push" in before and before[-1] == "flush", log
        space.sync(timeout=stress.budget(5))
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
        new_database_once(spaces[0])
        dbs = [s.open_database("db") for s in spaces]
        for i in range(10):
            _run(dbs[i % 2], _incr, **RUN_KW)
        reads = sum(s.read_roots for s in storages)
        assert _value(spaces[1]) == 10
        # One read to learn the first root (the space was empty); never one per commit.
        assert reads <= 4, reads
        # No commit writes the storage root: only the persisters do, after the commit point
        # (D46).  Two persisters may race on one publish -- n0's for its root while n1's
        # publishes the next one -- and the loser reads the root again (``PlainTier.persist``);
        # that is the compare-and-swap doing its job, not a commit's conflict (Task 8.3n fix
        # round 3: one such lost race in 80 runs under load).
        assert all(name.startswith("proto-persister-") for s in storages for name in s.publishers)
        assert not [name for s in storages for name in s.failed_cas_by
                    if not name.startswith("proto-persister-")]


def test_the_commit_point_is_the_majority_acknowledgement():
    with Cluster(3, **FAST) as cl:
        shared = MemoryStorage()
        spaces = [ObjectSpace(shared, coordinator=cl[n], lock_timeout=1.0) for n in cl.ids]
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
        db = new_database_once(spaces[0])
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
        new_database_once(s0)
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
        new_database_once(s0)
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
            except ProtoTimeoutError:
                outcome.append("timeout")              # no lease under load: nothing sent

        t = threading.Thread(target=first)
        t.start()
        time.sleep(FAST["lease_duration"] * 1.1)
        _run(s1.open_database("db"), token_incr("n1-next"), **RUN_KW)
        tokens.acked("n1-next")
        t.join(stress.budget(10))
        time.sleep(late)                                  # the delayed replicates arrive now
        assert "committed" not in outcome, outcome         # a late replication never commits
        stress.floor(outcome == ["unknown"], f"the late commit ended {outcome}, not unknown")
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
        stress.floor(sum(cl[n].stats()["refused"] for n in ("n1", "n2")) >= 1,   # the late copies
                     "no peer refused a late copy")
        cl["n0"].replicate = original
        _run(s0.open_database("db"), _incr, **RUN_KW)
        assert _value(s0) == after + 1
        assert _eventually(lambda: _value(s1) == after + 1)


def test_storage_root_reads_never_make_reads_go_backwards():
    shared = MemoryStorage()
    with Cluster(3, **FAST) as cl:
        s0 = ObjectSpace(shared, coordinator=cl["n0"])
        s1 = ObjectSpace(shared, coordinator=cl["n1"])
        ObjectSpace(shared, coordinator=cl["n2"])        # n2 grants and acknowledges with a tier only
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
        db0 = new_database_once(s0)
        _run(db0, _incr, **RUN_KW)
        assert _eventually(lambda: _value(s1) == 1) if stress.ACTIVE else _value(s1) == 1
        cl.net.partition({"n0", "n2"}, {"n1"})          # n1 misses the next commits
        for _ in range(3):
            _run(db0, _incr, **RUN_KW)
        assert _value(s1) == 1                           # stale but committed: never backwards
        cl.net.heal()
        s0.sync(timeout=stress.budget(5))
        _run(db0, _incr, **RUN_KW)                       # its release reaches n1
        stress.wait(lambda: _value(s1) == 5, 5, lambda: f"n1 reads {_value(s1)}, not 5", poll=0.01)
        entries = s1.open_database("db").history()
        assert entries[0].version == 5


def test_sync_waits_for_the_persisted_epoch_and_exposes_both_epochs():
    shared = MemoryStorage()
    with Cluster(3, **FAST) as cl:
        spaces = [ObjectSpace(shared, coordinator=cl[n]) for n in cl.ids]
        cl.wait_rejoined()      # every first start done before any fault (a late member waits, R51)
        db = new_database_once(spaces[0])
        for i in range(5):
            _run(spaces[i % 3].open_database("db"), _incr, **RUN_KW)
        spaces[1].sync(timeout=stress.budget(5))                         # the last committer
        # At least one epoch per commit (a base re-commit adds one with the same data; Task 11.1):
        # the plain reader's value counts the commits exactly.
        epoch = spaces[1].committed_epoch
        assert spaces[1].persisted_epoch >= epoch >= 6
        plain = ObjectSpace(shared)                       # a reader without the cluster
        assert _value(plain) == 5
        root = shared.read_root()
        assert _eventually(lambda: spaces[0].committed_epoch >= epoch)
        tr = db.new_transaction()
        assert tr._epoch == spaces[0].committed_epoch
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
        tc.create_database("n0")
        _run(tc.db("n0"), incr, **TRUN_KW)
        # n2 learns the commit first, then every member syncs: n2's sync below the outage must
        # find what n2 knows already persisted.  (Task 8.3n fix round 3: the syncs ran first,
        # and under load n2 learnt epoch 2 only after its own sync returned at epoch 1, so the
        # sync under the outage waited for a persisted root it could no longer read.)
        stress.wait(lambda: tc.value("n2") == 1, 5, "n2 did not learn the first commit", poll=0.01)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=stress.budget(10))
        # n2 owns no term, so n0's second commit needs no permit from n2 and nothing n2 drops
        # changes its shape (review M2: stated, not assumed).  n0's term may have ended
        # meanwhile (no demand): its next commit then asks for a new term, and the request
        # carries nothing n2 does not know.  n2 may own a term only through its own first
        # snapshot's confirmation; a term without demand ends within its lease, so wait for it
        # (Task 8.3n fix round 3: under load n2's confirmation took the term, run 12).
        stress.wait(lambda: tc["n2"]._ownership is None, 10, "n2's term never ended", poll=0.01)
        reader = tc["n2"]
        route = reader._route
        reader._route = lambda msg: None if msg["type"] in _UNPROMPTED else route(msg)
        tc.store.set_outage(True)
        try:
            _run(tc.db("n0"), incr, **TRUN_KW)                   # acknowledged by n0 and n1
            tc.spaces["n2"].sync(timeout=stress.budget(5))                   # returns: n2's knowledge is persisted
            assert tc.value("n2") == 1, "precondition: n2 has not learnt the commit"
            # Review I2: a commit that writes nothing is no barrier (it returns at once).
            tc.db("n2").run(lambda tr: None, **TRUN_KW)
            tc.db("n2").new_transaction().commit()
            assert tc.value("n2") == 1, "a commit that writes nothing taught n2 something"

            def barrier(tr):
                tr.set_root_object("barrier", (tr.get_root_object("barrier") or 0) + 1)
            _run(tc.db("n2"), barrier, **TRUN_KW)
            assert tc.value("n2") == 2                         # every acknowledged commit is visible
        finally:
            reader._route = route
            tc.store.set_outage(False)


def test_a_space_opened_while_no_majority_has_a_space_gets_no_permission_until_one_does(caplog):
    """Task 8.3n fix round 2, item 1 (concern 2 of fix round 1), reduced to its cause.  The
    first snapshot of a space confirms the committed root under a write permission
    (``needs_confirmation``).  A member with no space open has no acceptor log attached, so
    it neither grants nor acknowledges (D46 amendment, item 3: ``reject`` reason
    ``no_storage``; P5: no vote without a durable promise).  While a majority of the members
    has no space open, no permission can exist: the confirmation fails after the space's
    ``lock_timeout`` -- a timeout, the conservative failure P5 allows, logged -- and the read
    starts from the newest committed root this node knows.  As soon as a majority has a space
    open, every member commits.

    An early version of the TCP test's observer opened the members' spaces one after another
    in one thread: the first open waited out the 30 s default timeout because the opens that
    would have formed a majority came after it in the same thread."""
    from proto_db.cluster.rootlog import memory_log_for
    shared = MemoryStorage()
    for n in ("n0", "n1", "n2"):
        memory_log_for(shared, n).mark_rejoined()      # a cluster that ran before: no rejoin
    ObjectSpace(shared).new_database("db")
    with Cluster(3, **dict(FAST, startup_grace=None)) as cl:    # a restart: the default grace
        time.sleep(cl["n0"].restart_grace)              # the members grant nothing during it
        lonely = ObjectSpace(shared, coordinator=cl["n0"], lock_timeout=0.5)
        started = time.monotonic()
        with caplog.at_level("WARNING", logger="proto_db.transactions"):
            db0 = lonely.open_database("db")            # the first snapshot: the confirmation
        waited = time.monotonic() - started
        failed = [r for r in caplog.records if "Confirming the committed root" in r.getMessage()]
        assert failed and "could not obtain a write permission" in failed[0].getMessage(), caplog.text
        stress.floor(waited < 10, f"the confirmation waited {waited:.1f} s, not its lock_timeout of 0.5 s")
        assert all(cl[n].stats()["grants_given"] == 0 for n in cl.ids)     # no permission without a majority
        stress.floor(all(cl[n].stats()["grants_refused_no_storage"] >= 1 for n in ("n1", "n2")),
                     "a member was never asked during the confirmation")
        with pytest.raises(ProtoTimeoutError):          # still no majority: nothing commits
            db0.run(_incr, retries=0)
        caplog.clear()
        with caplog.at_level("WARNING", logger="proto_db.transactions"):
            db1 = ObjectSpace(shared, coordinator=cl["n1"], lock_timeout=5.0).open_database("db")
            _run(db1, _incr, **RUN_KW)                  # n0 and n1: a majority with a space open
            _run(db0, _incr, **RUN_KW)
        stress.floor(not [r for r in caplog.records if "Confirming the committed root" in r.getMessage()],
                     caplog.text)                       # the confirmation succeeded in time (liveness)
        assert _value(lonely) == 2


def test_a_confirmation_in_progress_succeeds_once_a_majority_opens_its_spaces(caplog):
    """Re-review 1, N5: the staggered start of a cluster.  n0 opens its space first: its
    confirmation waits for a permission while no majority has a space open (the others
    refuse, ``no_storage``).  n1 opens its space while n0's confirmation is still waiting;
    from then on a majority can grant, and n0's confirmation must succeed within its
    ``lock_timeout`` -- no "Confirming the committed root" warning -- having waited only until
    the majority formed."""
    from proto_db.cluster.rootlog import memory_log_for
    shared = MemoryStorage()
    for n in ("n0", "n1", "n2"):
        memory_log_for(shared, n).mark_rejoined()      # a cluster that ran before: no rejoin
    ObjectSpace(shared).new_database("db")
    with Cluster(3, **dict(FAST, startup_grace=None)) as cl:    # a restart: the default grace
        time.sleep(cl["n0"].restart_grace)              # the members grant nothing during it
        lonely = ObjectSpace(shared, coordinator=cl["n0"], lock_timeout=10.0)
        out = {}

        def open_first():
            started = time.monotonic()
            out["db"] = lonely.open_database("db")      # the first snapshot: the confirmation
            out["done"] = time.monotonic()
            out["started"] = started
        with caplog.at_level("WARNING", logger="proto_db.transactions"):
            first = threading.Thread(target=open_first)
            first.start()
            # n0's confirmation is in progress: its attempts were refused for want of storage.
            stress.wait(lambda: all(cl[n].stats()["grants_refused_no_storage"] >= 1 for n in ("n1", "n2")),
                        5, "n0's confirmation never asked the others")
            assert first.is_alive(), "n0's confirmation ended before any majority existed"
            formed = time.monotonic()
            db1 = ObjectSpace(shared, coordinator=cl["n1"], lock_timeout=10.0).open_database("db")
            first.join(stress.budget(15))
            assert not first.is_alive()
        stress.floor(not [r for r in caplog.records if "Confirming the committed root" in r.getMessage()],
                     caplog.text)                       # the confirmation succeeded in time (liveness)
        stress.floor(out["done"] - formed < 5, f"n0 confirmed {out['done'] - formed:.2f} s after the majority formed")
        _run(db1, _incr, **RUN_KW)
        _run(out["db"], _incr, **RUN_KW)
        assert _value(lonely) == 2


def test_a_setup_new_database_whose_recovery_query_goes_unanswered_conflicts_and_is_retried(tmp_path):
    """Task 8.3n fix round 2, item 2 (the pipelined-faults ``[1-5]`` failure of fix round 1),
    reduced.  A new cluster's first commit builds on an uncertain base, so its round asks the
    other voters for their roots and waits ``request_timeout`` for a majority.  Under a loaded
    suite the answers came late: ``recover_root`` raised a conflict ("only 1 of the 2
    witnesses needed answered the recovery query") -- retryable, nothing sent (P5) -- and
    ``new_database`` makes one attempt, so the test's setup failed.  Deterministic here: the
    first query's answers are lost.  ``TieredCluster.create_database`` retries the step and
    creates the database exactly once."""
    from tests.support.tiered_cluster import TieredCluster, incr, RUN_KW as TRUN_KW
    with TieredCluster(tmp_path) as tc:
        lost = threading.Lock()
        dropped = []
        for node in ("n1", "n2"):
            send = tc[node].transport.send

            def filtered(to, msg, _send=send):
                with lost:
                    drop = to == "n0" and msg["type"] == "answer" and len(dropped) < 2
                    if drop:
                        dropped.append(msg["from"])
                if not drop:
                    _send(to, msg)
            tc[node].transport.send = filtered
        with pytest.raises(ProtoConflictError, match="witnesses needed answered the recovery query"):
            tc.spaces["n0"].new_database("db")
        assert sorted(dropped) == ["n1", "n2"] and tc.spaces["n0"].list_databases() == []
        tc.create_database("n0")                           # the next queries are answered
        _run(tc.db("n0"), incr, **TRUN_KW)
        assert tc.spaces["n0"].list_databases() == ["db"] and tc.value("n0") == 1


def test_a_recovery_round_ends_once_a_majority_of_witnesses_answered(tmp_path):
    """Task 8.3n fix round 3 (the crash-test regression of the proof).  A commit on an
    uncertain base runs a recovery round inside its lease, before it replicates.  The round
    waited for every other voter: with one voter dead it lasted ``request_timeout`` every time,
    and a 1 s lease (belief 0.333 s) with a 0.25 s ``request_timeout`` had at most 83 ms left
    for the send -- under the ``send_reserve`` (R121) every commit was refused, no commit made
    the term's root certain, and the next commit recovered again: no commit after the kill.
    The round ends at a majority of witnesses.  Its answers promise nothing (ballot 0), so it
    may miss a root accepted after they were given; a commit's base is therefore floored at
    the lease's phase-1 root (re-review 2 of 8.3n, C1).  Here n2's answers never reach n0:
    before the fix the round lasted the full second.  The result is checked too (re-review 2,
    M7): the root every member committed and accepted."""
    from tests.support.tiered_cluster import TieredCluster, incr, RUN_KW as TRUN_KW
    with TieredCluster(tmp_path, lease_duration=4.0, request_timeout=1.0) as tc:
        tc.create_database("n0")
        _run(tc.db("n0"), incr, **TRUN_KW)
        send = tc["n2"].transport.send

        def silent_to_n0(to, msg, _send=send):
            if not (to == "n0" and msg["type"] == "answer"):
                _send(to, msg)
        tc["n2"].transport.send = silent_to_n0
        committed = tc["n0"].committed_root()[:3]
        assert committed[0] and committed[1] is not None
        stress.wait(lambda: all(tc[n].accepted_root()[:3] == committed for n in ("n0", "n1", "n2")),
                    10, lambda: [tc[n].accepted_root() for n in ("n0", "n1", "n2")])   # setup only
        started = time.monotonic()
        known, root, label = tc["n0"].recover_root()
        elapsed = time.monotonic() - started
        assert (known, root, label) == committed, ((known, root, label), committed)
        stress.floor(elapsed < 0.5, f"the round took {elapsed:.3f} s with a majority answering at once")
        with pytest.raises(ProtoConflictError, match="witnesses needed answered the recovery query"):
            tc["n1"].transport.send = lambda to, msg: None     # n1 silent too: no majority
            tc["n0"].recover_root()
