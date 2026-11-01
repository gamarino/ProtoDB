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

from proto_db.errors import ProtoCorruptionError, ProtoOutcomeUnknownError
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
