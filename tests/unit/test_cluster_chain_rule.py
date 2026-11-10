"""The acceptor's chain rule (Task 8.3f, D50; Task 8.3k, D55 rule V2').

A root built on a root of its own term is taken only when it is the accepted root again (a
duplicate) or extends it (built on it); the accepted root includes a root the acceptor learnt
committed.  Nothing the message says about its predecessor counts (protocol 13 removed the
``pred_committed`` catch-up): this is Raft's log matching applied to roots.  Only the first
root of a term (built on a base of an older label, out of phase 1) may follow an accepted
root of an older term, or reach an acceptor that accepted nothing, under the ballot rule
alone.  A sibling -- another root built on the same base in the same term -- is
refused, never reported as a protocol violation; a permission sequence number below the
highest accepted in the term is refused.  The first root of a newer term is ruled by the
ballot rule alone.  These rules stop two holders of one term from both reaching a majority
whatever the permissions (D50, S3); that a committed root is never outranked by an
uncommitted sibling at the next phase 1 still rests on the owner's sequencing (D50, S2/S4,
tests/unit/test_cluster_permit_incarnations.py).
"""
import pytest

from tests.support.manual_cluster import Harness, rec

T = 5 << 12          # a ballot (rank 0)


@pytest.fixture
def node():
    h = Harness("abc")
    try:
        h.bootstrap("a")
        yield h.nodes["b"]
    finally:
        h.close()


OLD = 4 << 12        # an older ballot
BASE = rec(9)        # the phase-1 base the first root of term T is built on


def _accept(node, label, root, pred=(True, BASE, OLD), seq=0):
    with node._lock:
        ok = node._may_accept(label, root, label, pred, seq)
        if ok:
            node._accepted = (True, root, label)
            node._note_seq(label, seq)
        return ok


def test_a_root_extending_the_accepted_root_is_accepted(node):
    r1, r2 = rec(10), rec(11)
    assert _accept(node, T, r1, seq=1)
    assert _accept(node, T, r2, pred=(True, r1, T), seq=1)
    assert _accept(node, T, r2, pred=(True, r1, T), seq=1)          # a duplicate


def test_a_sibling_in_the_same_term_is_refused_without_a_protocol_violation(node):
    base, r1, sibling = rec(10), rec(11), rec(11)
    assert _accept(node, T, base, seq=1)
    assert _accept(node, T, r1, pred=(True, base, T), seq=2)
    assert not _accept(node, T, sibling, pred=(True, base, T), seq=3)
    assert not _accept(node, T, rec(12), pred=(True, sibling, T), seq=3)   # built on the sibling


def test_a_root_not_built_on_the_accepted_root_is_refused_until_its_base_is_learnt_committed(node):
    """V2': a same-term root whose ``pred`` orders above the accepted root is refused whatever
    the message says; once the acceptor learnt that ``pred`` committed (``done``, a release, a
    permit), it extends it (the design review's probe ``p_voter_local.py`` as a regression)."""
    r1, r2, r3 = rec(10), rec(11), rec(12)
    assert _accept(node, T, r1, seq=1)
    assert not _accept(node, T, r3, pred=(True, r2, T), seq=2)
    with node._lock:
        node._observe_committed(True, r2, T, "a")
    assert node.accepted_root() == (True, r2, T)
    assert _accept(node, T, r3, pred=(True, r2, T), seq=2)


def test_a_later_root_of_a_term_never_follows_an_accepted_root_of_an_older_term(node):
    """V2' (Task 8.3k probe ``p_newer_term.py``): root k is the FIRST root of term T and this
    acceptor never held it nor learnt it committed; root k+1 (built on k) is refused even though
    its term is newer than the accepted root's.  D55 revision 1's V2 constrained only roots of
    the accepted root's label, and the first-root-of-a-newer-term exemption accepted k+1."""
    old_root, k, k1 = rec(10), rec(11), rec(12)
    assert _accept(node, OLD, old_root, pred=(True, rec(8), OLD - (1 << 12)), seq=3)
    assert not _accept(node, T, k1, pred=(True, k, T), seq=2)
    assert _accept(node, T, k, pred=(True, old_root, OLD), seq=1)     # the first root of T
    assert _accept(node, T, k1, pred=(True, k, T), seq=2)             # then k+1 extends it


def test_a_node_that_accepted_nothing_takes_only_the_first_root_of_a_term(node):
    with node._lock:
        node._accepted = (False, None, 0)
    k, k1 = rec(11), rec(12)
    assert not _accept(node, T, k1, pred=(True, k, T), seq=2)
    assert not _accept(node, T, k1, pred=None, seq=2)
    assert _accept(node, T, k, pred=(True, None, 0), seq=1)            # built on the empty space


def test_a_stale_permission_sequence_is_refused(node):
    r1, r2, r3 = rec(10), rec(11), rec(12)
    assert _accept(node, T, r1, seq=1)
    assert _accept(node, T, r2, pred=(True, r1, T), seq=3)
    assert not _accept(node, T, r3, pred=(True, r2, T), seq=2)


def test_the_first_root_of_a_newer_term_follows_the_ballot_rule_only(node):
    r1 = rec(10)
    assert _accept(node, T, r1, seq=7)
    newer = T + (1 << 12)
    assert _accept(node, newer, rec(11), pred=(True, rec(10), T), seq=1)
    assert not _accept(node, newer + (1 << 12), rec(13), pred=(True, rec(12), newer + (1 << 12)), seq=1)
    assert not _accept(node, T, rec(12), pred=(True, r1, T), seq=8)          # an older term
