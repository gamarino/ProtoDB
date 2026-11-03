"""The acceptor's chain rule (Task 8.3f, D50).

Within one owner term an acceptor takes a root only when it is the accepted root again (a
duplicate), extends it (built on it), or catches it up along a committed predecessor that
orders above it; a sibling -- another root built on the same base in the same term -- is
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


def _accept(node, label, root, pred=None, pred_committed=False, seq=0):
    with node._lock:
        ok = node._may_accept(label, root, label, pred, pred_committed, seq)
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


def test_a_root_not_built_on_the_accepted_root_is_refused_unless_its_base_is_committed(node):
    r1, r2, r3 = rec(10), rec(11), rec(12)
    assert _accept(node, T, r1, seq=1)
    assert not _accept(node, T, r3, pred=(True, r2, T), pred_committed=False, seq=2)
    assert _accept(node, T, r3, pred=(True, r2, T), pred_committed=True, seq=2)   # catches up


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
    assert not _accept(node, T, rec(12), pred=(True, r1, T), seq=8)          # an older term
