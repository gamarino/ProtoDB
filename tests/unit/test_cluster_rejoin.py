"""A node that lost its acceptor log becomes an acceptor and a witness again only once it has
learnt what it must know (8.3d re-review 2, ruling R49).  The reviewer's probes, as tests:

* X1 ``probe_lost``: ``b`` acknowledged R with ``a`` and is wiped; it rejoins and must report
  R again (the highest accepted root of ``N - majority + 1`` witnesses), so ``c``'s lease,
  recovered from ``b`` alone, builds on R.  Plus the hard guard: a base older than the
  committed root a node knows is refused, never committed over.
* X2 ``probe_unattached``: a coordinator whose space is not open (no acceptor log loaded,
  or still loading) answers ``unknown``, which never counts, and grants nothing.
* X3 ``probe_majority_loss``: with two of three disks wiped, the empty answers of the other
  wiped node do not count while an intact member knows a root: only ``force_rejoin()``
  completes the rejoin.  A new cluster (no root anywhere) still starts on its own.
* X4: a protocol violation found after the root was sent stays ``ProtoCorruptionError``.
* X5: every rejoin thread is joined by ``stop()``.
"""
import threading
import uuid

import pytest

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.cluster.lease import Lease
from proto_db.errors import ProtoCorruptionError, ProtoOutcomeUnknownError
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from proto_db.storage.memory import MemoryStorage
from tests.support.cluster import Cluster, MemoryTier
from tests.support.manual_cluster import D, Harness, rec

pytestmark = [pytest.mark.timeout(60)]


@pytest.fixture
def harness():
    made = []

    def make(names="abc"):
        h = Harness(names)
        made.append(h)
        return h

    yield make
    for h in made:
        h.close()


def test_x1_a_rejoined_acknowledger_reports_the_committed_root_it_lost(harness):
    h = harness()
    r1 = h.bootstrap("a")
    lease_a = h.attempt("a", {"b"})
    r = rec(2)
    assert not isinstance(h.replicate("a", lease_a, r, to={"b"}), Exception)    # committed: a + b
    h.release("a", lease_a, to=())                    # c never learns R
    h.restart("b", wipe=True)                         # b rejoins, answered by a and c
    assert h.nodes["b"].accepted_root()[1] == r       # learnt from the witnesses, and logged
    h.clock.t += D
    lease_c = h.attempt("c", {"b"})
    base, label = h.base("c", lease_c, responders={"b"})     # a's answer is late
    assert base == r and base != r1, "c would build on the bootstrap root and lose R"
    nxt = rec(base.epoch + 1)
    assert not isinstance(h.replicate("c", lease_c, nxt, to={"b"}), Exception)
    h.release("c", lease_c, to={"a", "b"})
    for n in "abc":
        assert h.nodes[n].committed_root()[1] == nxt
    h.restart("b")                                    # the learnt root survives a restart
    assert h.nodes["b"].accepted_root()[1] == nxt


class _Stub:
    """The coordinator surface ``_lease_base`` uses."""

    def __init__(self, committed):
        self._committed = committed

    def committed_root(self):
        return self._committed + (None, 0.0, 1)


class _NoPersisted:
    """A tier whose object store holds no persisted root."""

    def read_persisted(self):
        return None


def test_x1_guard_a_base_older_than_the_known_committed_root_is_refused():
    from proto_db.transactions import _lease_base
    old = RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 2)
    newer = RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 3)
    lease = Lease(term=3 * 4096, acquired_at=0.0, valid_until=1e9, root=old, root_certain=True,
                  clock=lambda: 0.0, root_term=4096)
    with pytest.raises(ProtoCorruptionError, match="below the committed root"):
        _lease_base(_Stub((True, newer, 8192)), _NoPersisted(), lease)
    assert _lease_base(_Stub((True, old, 4096)), _NoPersisted(), lease) == (old, 4096, True)


def test_x1_guard_a_holder_overtaken_by_a_later_term_acquires_again_instead_of_raising():
    """A holder paused between ``acquire`` and its base check (a scheduler or GC pause longer
    than the lease) may meanwhile have learnt a root committed under a later term.  That is a
    superseded lease, not a protocol violation: the base check asks for a new lease."""
    from proto_db.transactions import _lease_base
    old = RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 2)
    newer = RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 3)
    lease = Lease(term=3 * 4096, acquired_at=0.0, valid_until=1e9, root=old, root_certain=True,
                  clock=lambda: 0.0, root_term=4096)
    assert _lease_base(_Stub((True, newer, 5 * 4096)), None, lease) is None


def _unattached(h, name):
    """Restart ``name`` with its disk intact but its space not open yet (no tier attached)."""
    old = h.nodes[name]
    old.stop()
    h.retired.append(old)
    node = ClusterCoordinator(name, h.peers, transport=h.net.transport(), lease_duration=D, drift_margin=1.0,
                              request_timeout=0.5, startup_grace=D, clock=h.clock, seed=1,
                              persist_interval=60.0, persist_lag=60.0)
    node.start()
    h.nodes[name] = node
    return node


def test_x2_an_unattached_coordinator_is_never_a_rejoin_witness(harness):
    h = harness()
    h.bootstrap("a")
    last = None
    for i in range(3):                                # c leases with b only; a hears nothing
        h.clock.t += D
        lease_c = h.attempt("c", {"b"})
        r = rec(2 + i)
        assert not isinstance(h.replicate("c", lease_c, r, to={"b"}), Exception)
        h.release("c", lease_c, to={"b"})
        last = (lease_c.term, r)
    c = _unattached(h, "c")
    h.restart("b", wipe=True, responders=())
    assert not h.rejoin("b", {"a", "c"}, timeout=1.0), "the unattached c's answer was counted"
    c.attach(MemoryTier(h.disks["c"], "c"))
    assert h.rejoin("b", {"a", "c"})
    b = h.nodes["b"]
    assert b._grants.granted_top >= last[0]           # the promise it had made c
    assert b.accepted_root()[1] == last[1]            # and the root it had accepted
    h.clock.t += D
    lease_a = h.attempt("a", {"b"})
    assert lease_a.term > last[0]
    base, _ = h.base("a", lease_a, responders={"b"})  # c's answer is late
    assert base == last[1]


def test_x2_a_node_whose_log_is_still_loading_neither_answers_nor_grants():
    gate = threading.Event()

    class SlowTier(MemoryTier):
        def load_roots(self):
            gate.wait(10.0)
            return super().load_roots()

    with Cluster(3, lease_duration=2.0, drift_margin=0.2, request_timeout=0.2, startup_grace=0.0,
                 tiers=True) as cl:
        old = cl["n2"]
        old.stop()
        cl.retired.append(old)
        node = ClusterCoordinator("n2", cl.peers, transport=cl.net.endpoint(), lease_duration=2.0,
                                  drift_margin=0.2, request_timeout=0.2, startup_grace=0.2)
        node.start()
        cl.nodes["n2"] = node
        attaching = threading.Thread(target=node.attach, args=(SlowTier(cl.disks["n2"], "n2"),))
        attaching.start()
        try:
            for _ in range(1000):                     # the tier is set, its log still loading
                if node._tier is not None:
                    break
                threading.Event().wait(0.005)
            assert node._tier is not None
            with node._lock:
                assert node._witness_state() == "unknown" and node._refusal() == "no_storage"
                reply = node._decide("n0", 50 * 4096, 1, 0)
            assert reply["type"] == "reject" and reply["reason"] == "no_storage"
        finally:
            gate.set()
            attaching.join()
        with node._lock:
            assert node._witness_state() == "ok"


def test_x3_majority_disk_loss_needs_force_rejoin(harness):
    h = harness()
    h.bootstrap("a")
    lease = h.attempt("a", {"b"})
    r = rec(2)
    assert not isinstance(h.replicate("a", lease, r, to={"b"}), Exception)
    h.release("a", lease, to={"b", "c"})
    h.restart("b", wipe=True, responders=())
    h.restart("c", wipe=True, responders=())          # two of three disks lost, every node up
    assert not h.rejoin("b", {"a", "c"}, timeout=1.0)
    assert not h.rejoin("c", {"a", "b"}, timeout=1.0)
    h.nodes["b"].force_rejoin()                       # the operator's decision
    assert h.rejoin("b", {"a", "c"})
    assert h.nodes["b"].accepted_root()[1] == r       # a answered: nothing lost here
    h.nodes["c"].force_rejoin()
    assert h.rejoin("c", {"a", "b"})
    h.clock.t += D
    lease_c = h.attempt("c", {"b"})
    assert h.base("c", lease_c, responders={"b"})[0] == r


def test_x3_a_new_cluster_starts_even_when_members_finish_their_first_rejoin_at_different_times(harness):
    h = harness()                                     # Harness rejoins a, then b, then c
    assert all(h.nodes[n].stats()["rejoins"] == 1 for n in "abc")
    assert not isinstance(h.attempt("b", {"a", "c"}), Exception)


def test_x4_a_protocol_violation_after_the_root_was_sent_is_not_an_unknown_outcome(harness):
    h = harness()
    h.bootstrap("a")
    h.clock.t += D
    lease = h.attempt("a", {"b", "c"})
    node = h.nodes["a"]
    clash = rec(5)
    with node._lock:                                  # a corrupted state: same label and epoch
        node._accepted = (True, clash, lease.term)
    outcome = h.replicate("a", lease, rec(5), to={"b", "c"})
    assert isinstance(outcome, ProtoCorruptionError) and not isinstance(outcome, ProtoOutcomeUnknownError)
    assert "Protocol violation" in str(outcome)


def test_x5_every_rejoin_thread_is_joined_by_stop():
    with Cluster(3, lease_duration=2.0, drift_margin=0.2, request_timeout=0.5, startup_grace=0.0) as cl:
        cl.net.partition({"n0"}, {"n1", "n2"})        # n0 cannot rejoin
        node = cl["n0"]
        from proto_db.errors import ProtoValidationError

        def slow_ask(*args, **kwargs):                # a round that ignores the stop for a while
            threading.Event().wait(0.3)
            raise ProtoValidationError("stopped")

        node._ask = slow_ask
        first = MemoryTier(MemoryStorage(), "n0")
        node.attach(first)
        node.detach(first)
        node.attach(MemoryTier(MemoryStorage(), "n0"))
        threads = list(node._rejoiners)
        assert len(threads) >= 1
        started = [t for t in threading.enumerate() if t.name == "proto-cluster-rejoin-n0"]
        node.stop()
        assert all(not t.is_alive() for t in started + threads)


def test_x2_a_recovery_counts_only_witness_answers(harness):
    from proto_db.errors import ProtoConflictError
    h = harness()
    h.bootstrap("a")
    _unattached(h, "b")
    outcome = h.recover("c", {"b"})                   # b's answer is "unknown"
    assert isinstance(outcome, ProtoConflictError)
    h.nodes["b"].attach(MemoryTier(h.disks["b"], "b"))
    known, root, _ = h.recover("c", {"b"})
    assert known and root is not None


def test_probe_inflight_a_granter_wiped_during_a_lease(harness):
    """Reviewer probe ``probe_inflight`` (granter): ``b`` loses its disk while ``a`` holds a lease
    it granted; ``a``'s commit still reaches a majority, and the following leases have distinct
    terms and build on each other."""
    h = harness()
    h.bootstrap("a")
    lease_a = h.attempt("a", {"b"})
    h.restart("b", wipe=True)                         # rejoins from a and c, learns a's term
    r = rec(2)
    assert not isinstance(h.replicate("a", lease_a, r, to={"b", "c"}), Exception)
    h.release("a", lease_a, to={"b", "c"})
    h.clock.t += D
    terms, epoch = {lease_a.term}, r.epoch
    for name, granters in (("c", {"b"}), ("b", {"c"}), ("a", {"c"})):
        lease = h.attempt(name, granters)
        assert lease.term not in terms
        terms.add(lease.term)
        base, _ = h.base(name, lease, responders=set("abc") - {name})
        assert base.epoch == epoch                    # the previous commit, never an older root
        nxt = rec(epoch + 1)
        assert not isinstance(h.replicate(name, lease, nxt, to=granters), Exception)
        h.release(name, lease, to=set("abc") - {name})
        h.clock.t += D
        epoch += 1


def test_probe_inflight_a_holder_wiped_during_its_lease(harness):
    """Reviewer probe ``probe_inflight`` (proposer): ``a`` commits R with ``b`` and loses its disk
    while its lease runs; after its rejoin it proposes above its old ballot and builds on R."""
    h = harness()
    h.bootstrap("a")
    lease_a = h.attempt("a", {"b"})
    r = rec(2)
    assert not isinstance(h.replicate("a", lease_a, r, to={"b"}), Exception)
    h.restart("a", wipe=True)
    assert h.nodes["a"].accepted_root()[1] == r
    h.clock.t += D
    lease = h.attempt("a", {"c"})
    assert not isinstance(lease, Exception) and lease.term > lease_a.term
    assert h.base("a", lease, responders={"b", "c"})[0] == r
