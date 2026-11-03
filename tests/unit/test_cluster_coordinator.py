"""ClusterCoordinator over the in-process transport: lease, contention, faults, root propagation
(Tasks 8.3c and 8.3d).  Every node has an in-memory cluster tier (``Cluster(tiers=True)``): a
node without one neither grants nor acknowledges (D46)."""
import threading
import time
import uuid

import pytest

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.cluster.transport import InProcessNetwork
from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError, ProtoValidationError
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from tests.support.cluster import Cluster

pytestmark = pytest.mark.timeout(60)

FAST = dict(lease_duration=0.9, request_timeout=0.1, startup_grace=0.0,
            backoff=(0.001, 0.02))


def _rec(epoch):
    return RootRecord(FORMAT_VERSION, uuid.UUID(int=epoch), 64, epoch)


@pytest.fixture
def baseline_threads():
    before = threading.active_count()
    yield
    deadline = time.monotonic() + 5
    while threading.active_count() > before and time.monotonic() < deadline:
        time.sleep(0.01)
    assert threading.active_count() == before


def _wait(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.005)
    return True


# -- construction ------------------------------------------------------------------------------

def test_constructor_validates_membership_and_timing():
    net = InProcessNetwork()
    peers = [("a", "h", 1), ("b", "h", 2), ("c", "h", 3)]
    with pytest.raises(ProtoValidationError):
        ClusterCoordinator("z", peers, transport=net.endpoint())
    with pytest.raises(ProtoValidationError):
        ClusterCoordinator("a", peers + [("a", "h", 4)], transport=net.endpoint())
    with pytest.raises(ProtoValidationError):
        ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=1.0, drift_margin=1.0)
    with pytest.raises(ProtoValidationError, match="request_timeout"):
        ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=1.0, request_timeout=0.35)
    with pytest.raises(ProtoValidationError, match="at least"):       # ruling R78b
        ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=1.0, drift_margin=0.6)
    c = ClusterCoordinator("a", peers, transport=net.endpoint())
    assert c.cluster_size == 3 and c.majority == 2
    net.close()


def test_acquire_before_start_or_after_stop_raises(baseline_threads):
    with Cluster(1, tiers=True, **FAST) as cl:
        node = cl["n0"]
        node.stop()
        with pytest.raises(ProtoValidationError):
            node.acquire(timeout=0.1)


# -- lease basics ------------------------------------------------------------------------------

def test_single_node_cluster_acquires_and_releases(baseline_threads):
    with Cluster(1, tiers=True, **FAST) as cl:
        with cl["n0"].lease() as lease:
            assert lease.is_valid() and lease.term >= 1
        assert not lease.is_valid()


def test_three_nodes_never_hold_overlapping_leases_and_terms_increase(baseline_threads):
    intervals = []
    lock = threading.Lock()
    with Cluster(3, tiers=True, **FAST) as cl:
        def worker(node):
            for _ in range(15):
                with node.lease(timeout=10) as lease:
                    start = lease.acquired_at
                    time.sleep(0.001)
                with lock:
                    intervals.append((start, min(lease.valid_until, lease.released_at), lease.term, node.node_id))
        threads = [threading.Thread(target=worker, args=(cl[n],)) for n in cl.ids for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    intervals.sort()
    assert len(intervals) == 90
    for (s1, e1, t1, n1), (s2, e2, t2, n2) in zip(intervals, intervals[1:]):
        assert e1 <= s2, f"leases overlap: {n1} term {t1} and {n2} term {t2}"
        assert t2 > t1, "terms must strictly increase in lease order"


def test_a_minority_cannot_acquire_while_the_majority_can(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        cl.net.partition({"n0"}, {"n1", "n2"})
        with pytest.raises(ProtoTimeoutError):
            cl["n0"].acquire(timeout=0.5)
        with cl["n1"].lease(timeout=5) as lease:
            assert lease.is_valid()


def test_a_crashed_holder_blocks_others_until_its_lease_expires(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        held = cl["n0"].acquire(timeout=5)
        cl.crash("n0")            # never releases
        t0 = time.monotonic()
        lease = cl["n1"].acquire(timeout=5)
        waited = time.monotonic() - t0
        assert lease.acquired_at >= held.valid_until
        assert waited >= FAST["lease_duration"] * 0.5
        cl["n1"].release(lease)


def test_a_restarted_peer_honours_promises_through_its_startup_grace(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        cl.net.partition({"n0", "n1"}, {"n2"})     # n0 wins with n0 + n1 only
        held = cl["n0"].acquire(timeout=5)
        cl.net.heal()
        # n1 restarts (forgets its grant); with the grace it still refuses n2 during n0's lease.
        cl.restart("n1", startup_grace=None)        # the default: lease_duration + drift_margin
        lease = cl["n2"].acquire(timeout=5)
        assert lease.acquired_at >= held.valid_until
        cl["n2"].release(lease)
        cl["n0"].release(held)


def test_a_waiting_requester_is_woken_by_the_release(baseline_threads):
    with Cluster(3, tiers=True, lease_duration=2.0, request_timeout=0.1, startup_grace=0.0,
                 backoff=(0.5, 0.5)) as cl:
        held = cl["n0"].acquire(timeout=5)
        got = {}

        def waiter():
            got["lease"] = cl["n1"].acquire(timeout=5)
            got["at"] = time.monotonic()

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.2)
        released_at = time.monotonic()
        cl["n0"].release(held)
        t.join()
        # Woken by the release, not by the 0.5 s backoff nor the 2 s lease expiry.
        assert got["at"] - released_at < 0.3
        cl["n1"].release(got["lease"])


def test_stop_wakes_a_blocked_acquirer(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        cl.net.partition({"n0"}, {"n1"}, {"n2"})
        errors = []

        def blocked():
            try:
                cl["n0"].acquire(timeout=30)
            except ProtoValidationError as e:
                errors.append(e)

        t = threading.Thread(target=blocked)
        t.start()
        time.sleep(0.1)
        cl["n0"].stop()
        t.join(5)
        assert not t.is_alive() and errors


# -- root propagation --------------------------------------------------------------------------

def test_known_root_never_goes_backwards():
    net = InProcessNetwork()
    c = ClusterCoordinator("a", [("a", "h", 1)], transport=net.endpoint(), **FAST)
    assert c.known_root() == (False, None)
    c.observe_root(_rec(5), from_storage=True)
    c.observe_root(_rec(3), from_storage=True)
    assert c.known_root() == (True, _rec(5))
    net.close()


def test_release_propagates_the_replicated_root_to_every_peer(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        for node in cl.nodes.values():
            node.observe_root(None, from_storage=True)     # all know the empty space
        lease = cl["n0"].acquire(timeout=5)
        assert lease.root_certain and lease.root is None
        cl["n0"].replicate(lease, _rec(1))
        cl["n0"].release(lease)
        assert _wait(lambda: all(cl[n].known_root() == (True, _rec(1)) for n in cl.ids))
        nxt = cl["n2"].acquire(timeout=5)
        assert nxt.root == _rec(1) and nxt.root_certain
        cl["n2"].release(nxt)


def test_unknown_roots_make_the_holder_recover(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        lease = cl["n0"].acquire(timeout=5)
        assert not lease.root_certain     # nobody knows any root yet
        cl["n0"].release(lease)


def test_a_peer_that_missed_a_release_makes_the_next_holder_recover(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        for node in cl.nodes.values():
            node.observe_root(None, from_storage=True)
        lease = cl["n0"].acquire(timeout=5)
        assert _wait(lambda: cl["n2"].stats()["grants_given"] >= 1)
        cl.net.partition({"n0", "n1"}, {"n2"})      # n2 misses the replication and the release
        cl["n0"].replicate(lease, _rec(1))
        cl["n0"].release(lease)
        cl.net.heal()
        assert cl["n2"].known_root() == (True, None)
        time.sleep(FAST["lease_duration"])          # n2's grant to n0 expires
        nxt = cl["n2"].acquire(timeout=5)
        assert not nxt.root_certain                  # n2 may have missed a root: recover
        cl["n2"].release(nxt)


def test_a_failed_replication_makes_the_holders_next_lease_uncertain(baseline_threads):
    with Cluster(3, tiers=True, **FAST) as cl:
        for node in cl.nodes.values():
            node.observe_root(None, from_storage=True)
        lease = cl["n0"].acquire(timeout=5)
        cl.net.partition({"n0"}, {"n1", "n2"})
        with pytest.raises(ProtoOutcomeUnknownError):
            cl["n0"].replicate(lease, _rec(1))      # only n0 accepts: no majority
        cl["n0"].release(lease)
        assert cl["n0"].stats()["replication_failures"] == 1
        cl.net.heal()
        time.sleep(FAST["lease_duration"])
        nxt = cl["n0"].acquire(timeout=5)
        assert not nxt.root_certain
        cl["n0"].release(nxt)


def test_a_node_without_a_tier_neither_grants_nor_counts(baseline_threads):
    with Cluster(3, **FAST) as cl:                   # no tiers attached
        with pytest.raises(ProtoTimeoutError):
            cl["n0"].acquire(timeout=0.3)
        assert all(cl[n].stats()["grants_given"] == 0 for n in cl.ids)
        assert cl["n1"].stats()["grants_refused_no_storage"] >= 1


def test_fetch_timeout_must_leave_part_of_the_lease():
    net = InProcessNetwork()
    peers = [("a", "h", 1)]
    with pytest.raises(ProtoValidationError, match="fetch_timeout"):
        ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=2.0,
                           fetch_timeout=0.7)
    c = ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=2.0)
    assert c._fetch_timeout == pytest.approx(2.0 / 3 / 4)
    net.close()


def test_the_default_startup_grace_lasts_one_lease_plus_the_drift_margin(baseline_threads):
    kw = dict(FAST)
    del kw["startup_grace"]                        # default: lease_duration + drift_margin
    t0 = time.monotonic()
    with Cluster(3, tiers=True, **kw) as cl:
        grace = FAST["lease_duration"] * 5 / 3              # D + the default margin 2D/3 (ruling R78b)
        assert cl["n0"].restart_grace == pytest.approx(grace)
        lease = cl["n0"].acquire(timeout=5)
        waited = time.monotonic() - t0
        cl["n0"].release(lease)
    assert waited >= grace


def test_a_holder_never_replicates_under_a_term_below_its_rejoin_ballot(baseline_threads):
    """A node whose rejoin ballot (R78a) a quorum promised sends nothing under a lower term,
    which can no longer gain a majority: ``ProtoConflictError`` (retryable) instead of a
    replication the promising members refuse, whose outcome would be reported unknown.  Found under CPU load: a wiped node
    rejoined (its ballot ended the owner's term at every witness), then committed under a
    permission of that term and got ``ProtoOutcomeUnknownError`` (9/20 runs of
    ``test_a_fresh_peer_joining_during_an_outage_recovers_from_its_peers`` at 275a3b0)."""
    with Cluster(3, tiers=True, **FAST) as cl:
        for node in cl.nodes.values():
            node.observe_root(None, from_storage=True)
        n0 = cl["n0"]
        lease = n0.acquire(timeout=5)
        with n0._lock:
            n0._rejoin_ballot = lease.term + 100      # as _publish_rejoin records it
        sent = n0.stats()["replications"]
        with pytest.raises(ProtoConflictError, match="rejoin ballot"):
            n0.replicate(lease, _rec(1))
        assert n0.stats()["replications"] == sent and n0.stats()["replication_failures"] == 0
        n0.release(lease)
