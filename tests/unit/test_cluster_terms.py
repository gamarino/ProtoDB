"""A term is granted at most once, even across a restart (8.3d review C1, decision D46).

The reviewer's schedule: three nodes, one restart with the disk intact, two lost messages.

1. ``a`` wins term T with grants from ``a`` and ``b``; its request to ``c`` is lost.
2. ``a`` replicates R = (T, e+1); every copy is lost, so only ``a`` accepts it (outcome unknown).
3. ``b`` restarts with its disk intact; its grace passes.
4. ``c`` never saw T and asks for T.  Before the fix ``b`` had forgotten its promise and
   granted T again, so ``c`` committed R' = (T, e+1): two different roots with one label and
   one epoch, and ``a``'s next lease kept R (the first of two equal keys) and lost the
   committed R'.  Now ``b`` restored its promise from its log, refuses T, and ``c`` commits
   R' under a higher term.
5. ``a``'s next lease builds on R'.
"""
import pytest

from proto_db.errors import ProtoCorruptionError, ProtoOutcomeUnknownError, ProtoTimeoutError
from proto_db.cluster.lease import LeaseAttempt
from tests.support.manual_cluster import D, Harness, rec

pytestmark = [pytest.mark.timeout(30)]


def test_a_restarted_peer_never_grants_a_term_twice_and_the_committed_root_survives():
    h = Harness()
    try:
        h.bootstrap("a")
        lease_a = h.attempt("a", {"b"})                       # c's request is lost
        term = lease_a.term
        r = rec(2)
        outcome = h.replicate("a", lease_a, r, to=(), expire=True)   # every copy is lost
        assert isinstance(outcome, ProtoOutcomeUnknownError)
        h.release("a", lease_a, to=())                        # lost too
        h.restart("b")                                        # disk intact
        h.clock.t += D                                        # grace over, every grant expired
        lease_c = h.attempt("c", {"b"})
        assert lease_c.term > term, "b granted a term it had already granted before its restart"
        r_prime = rec(2)
        assert not isinstance(h.replicate("c", lease_c, r_prime, to={"b"}), Exception)
        h.release("c", lease_c, to={"a", "b"})
        h.clock.t += D
        lease_a2 = h.attempt("a", {"b", "c"})
        root, _ = h.base("a", lease_a2, responders={"b", "c"})
        assert root == r_prime                                # never the minority root r
    finally:
        h.close()


def test_equal_label_and_epoch_with_different_roots_is_a_protocol_violation():
    attempt = LeaseAttempt(term=9, rid=1, sent_at=0.0, cluster_size=3, lease_duration=1.0, drift_margin=0.1)
    attempt.on_grant("a", True, rec(5), True, 7)
    attempt.on_grant("b", True, rec(5), True, 7)              # same label and epoch, another root
    with pytest.raises(ProtoCorruptionError, match="Protocol violation"):
        attempt.best_root()


def test_ballots_are_proposer_qualified_so_two_proposers_never_share_a_term():
    """Ruling R47a: a term's low bits are its proposer's rank."""
    from proto_db.cluster.coordinator import MAX_MEMBERS, ballot_after, ballot_rank
    for floor in (0, 1, MAX_MEMBERS - 1, MAX_MEMBERS, 5 * MAX_MEMBERS + 3, 1 << 50):
        ballots = [ballot_after(floor, rank) for rank in range(3)]
        assert len(set(ballots)) == 3
        for rank, ballot in enumerate(ballots):
            assert ballot > floor and ballot_rank(ballot) == rank
            assert ballot - floor <= MAX_MEMBERS                 # the smallest such ballot


def test_a_request_for_another_proposers_ballot_is_refused():
    h = Harness()
    try:
        b = h.nodes["b"]
        with b._lock:
            reply = b._decide("c", 5 * 4096 + 0, 1, 0)          # rank 0 is a's, not c's
        assert reply["type"] == "reject" and reply["reason"] == "bad_ballot"
        with b._lock:
            reply = b._decide("c", 5 * 4096 + 2, 1, 0)
        assert reply["type"] == "grant"
    finally:
        h.close()


def test_a_wiped_granter_never_lets_two_roots_share_a_label():
    """Re-review N1: the C1 schedule with ``b``'s disk wiped (not merely restarted), and
    ``a``'s recovery answer to ``c`` lost.  Before R47 ``b`` restarted with no promise and
    granted ``a``'s term to ``c``; ``c`` committed R' with the label and epoch of ``a``'s
    unacknowledged R, and every later lease combining ``a`` with ``b`` or ``c`` raised
    ``ProtoCorruptionError`` (with ``b`` down, the cluster could not commit at all).  Now
    ``c``'s ballot carries its own rank, and ``b`` rejoins (learns the promised terms from the
    others) before it votes."""
    h = Harness()
    try:
        h.bootstrap("a")
        lease_a = h.attempt("a", {"b"})                       # c's request is lost
        r = rec(2)
        assert isinstance(h.replicate("a", lease_a, r, to=(), expire=True), ProtoOutcomeUnknownError)
        h.release("a", lease_a, to=())
        h.restart("b", wipe=True)                             # the disk is lost; b rejoins
        h.clock.t += D
        lease_c = h.attempt("c", {"b"})
        assert lease_c.term != lease_a.term
        root, _ = h.base("c", lease_c, responders={"b"})      # a's answer is lost
        r_prime = rec(root.epoch + 1)                         # b rejoined knowing a's R: c builds on it
        assert not isinstance(h.replicate("c", lease_c, r_prime, to={"b"}), Exception)
        h.release("c", lease_c, to={"a", "b"})
        h.clock.t += D
        for _ in range(3):                                    # every lease of a now works ...
            lease = h.attempt("a", {"b", "c"})
            assert not isinstance(lease, Exception), lease
            assert h.base("a", lease, responders={"b", "c"})[0] == r_prime
            h.release("a", lease, to={"b", "c"})
            h.clock.t += D
        for _ in range(3):                                    # ... and with b down, c and a commit
            lease = h.attempt("c", {"a"})
            assert not isinstance(lease, Exception), lease
            base, _ = h.base("c", lease, responders={"a"})
            assert base is not None and base.epoch >= r_prime.epoch
            nxt = rec(base.epoch + 1)
            assert not isinstance(h.replicate("c", lease, nxt, to={"a"}), Exception)
            h.release("c", lease, to={"a"})
            h.clock.t += D
        assert h.nodes["b"].stats()["rejoins"] == 1
    finally:
        h.close()


def test_a_wiped_proposer_never_wins_its_own_ballot_twice():
    """Ruling R47b: ``a`` wins ballot B with ``b`` and commits R (acknowledged by ``b``), then
    loses its disk.  Asked again, ``c`` (which never saw B) would grant B a second time, and
    ``a`` would commit another root with R's label and epoch.  A node with an empty log learns
    the promised terms from ``N - majority + 1`` others first: with ``c`` alone it may not vote
    or propose; once ``b`` answers it proposes above B and builds on R."""
    h = Harness()
    try:
        h.bootstrap("a")
        lease_a = h.attempt("a", {"b"})                       # c never sees B
        b_term = lease_a.term
        r = rec(2)
        assert not isinstance(h.replicate("a", lease_a, r, to={"b"}), Exception)   # committed: a + b
        h.release("a", lease_a, to=())                        # c never learns it
        h.restart("a", wipe=True, responders=())              # the disk is lost; b is unreachable
        h.clock.t += D
        h.rejoin("a", {"c"}, timeout=1.0)                     # c alone must not be enough
        outcome = h.attempt("a", {"c"}, timeout=1.0)
        assert getattr(outcome, "term", None) != b_term, "a won its ballot B a second time"
        assert isinstance(outcome, ProtoTimeoutError)         # it neither proposed nor voted
        assert h.rejoin("a", {"b", "c"})
        lease = h.attempt("a", {"c"})
        assert not isinstance(lease, Exception), lease
        assert lease.term > b_term
        base, _ = h.base("a", lease, responders={"b", "c"})
        assert base == r
        h.release("a", lease, to={"b", "c"})
    finally:
        h.close()
