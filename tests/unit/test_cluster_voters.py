"""Voters and learners (Task 8.3g, D54; maintainer rulings R70, R70a).

A fixed voter set of ``2f + 1`` members (default the first three member ids in sorted order,
configurable to any odd size of at least three; every member when the cluster has fewer
than three) orders roots: the lease, its renewals, ballots, recovery rounds and root
acceptance are decided by majorities of the voters only.  The other members are learners:
they never grant, never accept a root and never count in a quorum, but they may own a term
and commit like any member.
"""
import time

import pytest

from proto_db.cluster.coordinator import ClusterCoordinator as _Coordinator
from proto_db.cluster.transport import InProcessNetwork
from proto_db.errors import ProtoTimeoutError, ProtoValidationError
from tests.support.cluster import Cluster

pytestmark = [pytest.mark.timeout(120)]

KW = dict(lease_duration=1.2, request_timeout=0.1, startup_grace=0.0, backoff=(0.001, 0.01))
PEERS = [(f"n{i}", "127.0.0.1", 0) for i in range(8)]
_NET = InProcessNetwork()


def ClusterCoordinator(node_id, peers, **kw):
    """A coordinator that is never started (configuration only), on an in-process endpoint."""
    return _Coordinator(node_id, peers, transport=_NET.endpoint(), **kw)


def test_the_default_voter_set_is_the_first_three_members_in_id_order():
    node = ClusterCoordinator("n5", PEERS)
    assert node.voters == ("n0", "n1", "n2")
    assert not node.is_voter and node.majority == 2 and node.cluster_size == 8
    assert ClusterCoordinator("n1", PEERS).is_voter
    small = ClusterCoordinator("a", [("b", "127.0.0.1", 0), ("a", "127.0.0.1", 0)])
    assert small.voters == ("a", "b") and small.majority == 2


def test_the_voter_set_is_configurable_and_validated():
    node = ClusterCoordinator("n0", PEERS, voters=["n7", "n3", "n0", "n5", "n1"])
    assert node.voters == ("n0", "n1", "n3", "n5", "n7") and node.majority == 3
    for bad in (["n0", "n1"], ["n0", "n1", "n2", "n3"], ["n0", "n1", "x"], ["n0", "n0", "n1"], "n0n1n2", []):
        with pytest.raises(ProtoValidationError):
            ClusterCoordinator("n0", PEERS, voters=bad)
    with pytest.raises(ProtoValidationError):
        ClusterCoordinator("a", [("a", "127.0.0.1", 0), ("b", "127.0.0.1", 0)], voters=["a"])   # below three: all members


def test_learners_never_grant_and_two_voters_suffice():
    with Cluster(5, tiers=True, **KW) as cl:
        cl.crash("n3")
        cl.crash("n4")
        lease = cl["n0"].acquire(timeout=5)
        cl["n0"].release(lease)
        for node in ("n3", "n4"):
            assert cl[node].stats()["grants_given"] == 0


def test_learners_are_no_quorum_when_a_majority_of_voters_is_down():
    """On d6743ae every member is an acceptor: three of five (n0 and the two learners) grant."""
    with Cluster(5, tiers=True, **KW) as cl:
        cl.crash("n1")
        cl.crash("n2")
        started = time.monotonic()
        with pytest.raises(ProtoTimeoutError):
            cl["n0"].acquire(timeout=1.0)
        assert time.monotonic() - started >= 0.9
        assert cl["n3"].stats()["grants_given"] == 0 and cl["n4"].stats()["grants_given"] == 0


def test_a_learner_may_own_a_term():
    with Cluster(5, tiers=True, **KW) as cl:
        lease = cl["n4"].acquire(timeout=5)
        try:
            assert lease.is_valid()
            assert sum(cl[n].stats()["grants_given"] for n in ("n0", "n1", "n2")) >= 2
        finally:
            cl["n4"].release(lease)
        permission = cl["n3"].permit(timeout=5)
        cl["n3"].finish(permission)


def test_a_wiped_learner_needs_no_rejoin_and_a_recovery_counts_only_voters():
    with Cluster(5, tiers=True, **KW) as cl:
        node = cl.wipe("n4")
        assert node.stats()["rejoins"] == 0 and not node.stats()["rejoining"]
        cl.crash("n1")
        cl.crash("n2")
        with pytest.raises(Exception):
            cl["n3"].recover_root()         # only one voter answers: no majority of voters


def test_a_member_with_another_voter_set_is_ignored():
    """A misconfigured voter set must never form a quorum of its own: requests, renewals,
    ballots and replications carry a fingerprint of the sender's voter set and are dropped
    by members configured differently."""
    with Cluster(5, tiers=True, **KW) as cl:
        rogue = cl.restart("n4", voters=["n2", "n3", "n4"], startup_grace=None)
        with pytest.raises(ProtoTimeoutError):
            rogue.acquire(timeout=0.8)
        assert cl["n2"].stats()["config_mismatches"] > 0
        assert cl["n2"].stats()["grants_given"] == 0


def test_a_learners_refusal_never_decides_a_replication():
    """A data replica that is a learner stores the records and refuses the root (it accepts
    none): its refusal must not count toward the refusals that end a replication.  Counting
    it ended a commit as unknown with one voter refusing (8 TCP peers, three voters)."""
    from proto_db.cluster.protocol import make_message
    node = ClusterCoordinator("n5", PEERS)
    with node._lock:
        rid, waiter = node._new_waiter("replicate", 9)

    def answer(sender, ok, stored):
        with node._cond:            # as the dispatcher delivers it
            node._on_answer(sender, "ack", make_message("ack", sender, term=9, rid=rid, ok=ok, root=None,
                                                        stored=stored))

    for sender, ok, stored in (("n6", False, True), ("n7", False, False), ("n1", False, False)):
        answer(sender, ok, stored)
    assert waiter.nacks == {"n1"} and waiter.stored == {"n6"} and waiter.unstored == {"n7", "n1"}
    answer("n0", True, False)
    answer("n7", True, False)
    assert waiter.acks == {"n0"}
