"""A commit's base never orders below the lease's phase-1 root (Task 8.3n re-review 2, C1).

A recovery round's ``query`` carries ``ballot: 0``: it promises nothing, so a voter that
answered may still accept a root of an older term afterwards.  Ending the round at a majority
of witnesses (1b3a188) therefore misses a root that a majority accepted after the
intersecting member answered.  The lease's own granting majority (Paxos phase 1) cannot miss
it: each granter promised the term before it replied, so it reported every lower-term root it
had accepted and refuses every one it has not.  Before the fix, an uncertain lease took the
recovery result as its base and ignored ``lease.root``, so its first root (accepted over any
older term's root under the ballot rule alone) dropped an acknowledged root.

The reviewer's reproduction (``rr3-repro-test.py``), driven through the product's own
:func:`~proto_db.transactions._lease_base`.  Five voters a-e, :class:`ManualNet`, one fake
clock; e's lease reads a frozen clock (its timer runs slow, or its machine was suspended), a
rate fault the drift margin never makes a safety premise (D46, D50, R82).
"""
import threading
import time

import pytest

from proto_db.cluster.lease import Permission
from proto_db.cluster.rootlog import compare_roots
from proto_db.transactions import _lease_base
from tests.support.manual_cluster import D, MARGIN, Harness, rec

pytestmark = [pytest.mark.timeout(120)]


def _call(fn, *args):
    try:
        return fn(*args)
    except Exception as ex:     # the outcome under test, reported by the assertions
        return ex


def _lease_base_via(h, name, lease, responders):
    """``_lease_base`` for ``name`` under ``lease``; its recovery round (if any) is answered by
    ``responders`` only.  The result, or the ProtoDB error raised."""
    node = h.nodes[name]

    def pump():
        h.net.deliver(src=name, dst=set(responders), kinds=("query",))
        h.net.drop(src=name, kinds=("query",))
        h.net.deliver(src=None, dst={name}, kinds=("answer",))

    return h._run(lambda: _lease_base(node, node.tier, lease), pump)


def _acknowledged_root_out_of_phase_1(h):
    """Steps 1-4 of the reproduction.  Returns ``(r1, R', e's lease, e's replication thread,
    its result box, e's messages held back for c, a's lease)``: R' reached b only, a's lease
    was granted by b and d, so its phase 1 reported R' (uncertain)."""
    a, b, c, d, e = (h.nodes[n] for n in "abcde")
    r1 = h.bootstrap("a")
    h.clock.t += 2 * (D + MARGIN)
    # e owns T' from b and c (and itself).
    lease_e = h.attempt("e", {"b", "c"})
    assert not isinstance(lease_e, Exception), lease_e
    frozen = h.clock.t
    lease_e._clock = lambda: frozen             # e's timer stands still (rate fault / suspend)
    r_prime = rec(r1.epoch + 1)
    out = {}
    t = threading.Thread(target=lambda: out.setdefault("r", _call(e.replicate, lease_e, r_prime)))
    t.start()
    # e's replicate reaches b only; c's copy is held back (message delay).
    deadline = time.monotonic() + 10
    while b.accepted_root()[1] != r_prime:
        h.net.deliver(src="e", dst={"b"}, kinds=("records", "replicate"))
        assert time.monotonic() < deadline, "b never accepted R'"
        time.sleep(0.001)
    held_for_c = h.net.take("e", {"c"}, ("records", "replicate"))
    h.net.drop(src="e", dst={"a", "d"}, kinds=("records", "replicate"))
    assert held_for_c, "e sent nothing to c"
    # The granters' promises to e lapse on THEIR clocks.
    h.clock.t += D + MARGIN + 1
    # a owns T from b and d (and itself): phase 1 reports R' (b accepted it).
    lease_a = h.attempt("a", {"b", "d"})
    assert not isinstance(lease_a, Exception), lease_a
    assert lease_a.root == r_prime and lease_a.root_term == lease_e.term and not lease_a.root_certain
    return r1, r_prime, lease_e, t, out, held_for_c, lease_a


def test_c1_a_recovery_ended_at_a_majority_never_drops_the_phase_1_root():
    """Re-review 2, C1, end to end: the base a takes under its uncertain lease, with a recovery
    round answered by c and d only (they have not accepted R'), is R' -- not r1.  e then
    completes R' (acknowledged by b, c, e within its frozen lease) and a commits on its base:
    R' is in the history of a's committed root."""
    h = Harness("abcde")
    try:
        a, c = h.nodes["a"], h.nodes["c"]
        r1, r_prime, lease_e, t, out, held_for_c, lease_a = _acknowledged_root_out_of_phase_1(h)
        # a's recovery round is answered by c and d: a majority of witnesses with a.  It ends
        # at once (1b3a188) and reports r1, the highest root c and d accepted.
        base = _lease_base_via(h, "a", lease_a, {"c", "d"})
        assert isinstance(base, tuple), base
        current, label, replicated = base
        assert (current, label) == (r_prime, lease_e.term), \
            f"the base ordered below the lease's phase-1 root: {current} label {label}, not R' ({lease_e.term})"
        # e's delayed replicate reaches c, and c's ack reaches e (whose lease still looks valid).
        for src, dst, msg in held_for_c:
            h.net.handlers[dst](msg)
            h.nodes[dst].wait_idle(5.0)
        deadline = time.monotonic() + 10
        while t.is_alive():
            h.net.deliver(src=None, dst={"e"}, kinds=("ack",))
            assert time.monotonic() < deadline
            time.sleep(0.001)
        t.join()
        assert not isinstance(out["r"], Exception), f"R' was not acknowledged: {out['r']!r}"
        assert c.accepted_root()[1] == r_prime
        # a builds on its base, as _permit_round / _commit_under_lease do, and commits with c, d.
        lease_a.bind_root(current, label)
        r_t = rec(current.epoch + 1)
        res = h.replicate("a", lease_a, r_t, {"c", "d"})
        assert not isinstance(res, Exception), res
        known, committed, committed_label = a.committed_root()[:3]
        assert known and committed == r_t and committed_label == lease_a.term
        # R' was acknowledged; a's committed root is the first root of a's term, built on R'.
        assert compare_roots(label, current, lease_e.term, r_prime) >= 0, \
            "an acknowledged root (R') was superseded by a committed root not built on it"
    finally:
        h.close()


def test_c1_an_uncertain_permission_base_never_orders_below_its_issued_root():
    """The same floor for a permission (D50): an uncertain permit carries the owner's phase-1
    root as ``root``; a recovery round answered by voters that have not accepted it does not
    lower the base."""
    h = Harness("abcde")
    try:
        a = h.nodes["a"]
        r1, r_prime, lease_e, t, out, held_for_c, lease_a = _acknowledged_root_out_of_phase_1(h)
        permission = Permission(term=lease_a.term, seq=1, owner="a", acquired_at=h.clock.t,
                                valid_until=lease_a.valid_until, root=lease_a.root,
                                root_certain=lease_a.root_certain, clock=h.clock,
                                root_term=lease_a.root_term)
        assert not permission.root_certain and permission.after is None
        base = _lease_base_via(h, "a", permission, {"c", "d"})
        assert isinstance(base, tuple), base
        assert base[:2] == (r_prime, lease_e.term), \
            f"the base ordered below the permission's phase-1 root: {base[0]} label {base[1]}"
        h.clock.t = lease_e.valid_until + 1         # let e's replication end (unacknowledged)
        lease_e._clock = h.clock
        h.net.drop(src="e")
        t.join(10)
        assert a.committed_root()[1] == r1
    finally:
        h.close()


def test_c1_a_recovery_root_above_the_phase_1_root_is_still_adopted():
    """The floor never lowers what recovery finds: a root the answering majority accepted above
    the phase-1 root is the base (its proposer ran phase 1 for a higher ballot)."""
    h = Harness("abcde")
    try:
        r1 = h.bootstrap("a")
        h.clock.t += 2 * (D + MARGIN)
        lease_e = h.attempt("e", {"b", "c"})
        assert not isinstance(lease_e, Exception), lease_e
        r_prime = rec(r1.epoch + 1)
        h.replicate("e", lease_e, r_prime, {"c"}, expire=True)     # accepted by e and c only: unknown
        assert h.nodes["c"].accepted_root()[1] == r_prime
        h.clock.t += D + MARGIN + 1
        # a's phase 1 (b, d) reports r1 only; its recovery is answered by c, which holds R'.
        lease_a = h.attempt("a", {"b", "d"})
        assert not isinstance(lease_a, Exception), lease_a
        assert lease_a.root == r1 and not lease_a.root_certain
        base = _lease_base_via(h, "a", lease_a, {"c", "d"})
        assert isinstance(base, tuple), base
        assert base[:2] == (r_prime, lease_e.term), base
    finally:
        h.close()


def test_m6_a_recovery_with_every_waits_for_the_holder_that_knows_the_commit():
    """Re-review 2 of 8.3n, M6: ``open_database`` asks ``recover_root(every=True)`` before it
    reports a database missing.  Only committed roots are adopted from the answers, and the
    holder of a just-acknowledged commit (a create) may be the only node that knows it
    committed while its release is on its way.  A round ended at a majority answered by b
    alone (b accepted R, without knowing it committed) leaves c on r1; with ``every`` it waits
    for a's answer and learns R.  The answers are delivered b first, then a."""
    h = Harness("abc", request_timeout=3.0)
    try:
        a, b, c = (h.nodes[n] for n in "abc")
        r1 = h.bootstrap("a")
        h.clock.t += 2 * (D + MARGIN)
        lease = h.attempt("a", {"b"})
        assert not isinstance(lease, Exception), lease
        r = rec(r1.epoch + 1)
        assert not isinstance(h.replicate("a", lease, r, {"b"}), Exception)
        assert a.committed_root()[1] == r and b.accepted_root()[1] == r and b.committed_root()[1] == r1
        assert c.committed_root()[1] == r1          # a's release has not reached c
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("r", _call(c.recover_root, True)))
        t.start()
        deadline = time.monotonic() + 10
        answers = {}
        while set(answers) != {"a", "b"}:
            assert time.monotonic() < deadline, "the queries were never answered"
            h.net.deliver(src="c", dst={"a", "b"}, kinds=("query",))
            for src in ("a", "b"):
                answers.setdefault(src, []).extend(h.net.take(src, {"c"}, ("answer",)))
                if not answers[src]:
                    answers.pop(src)
            time.sleep(0.001)
        for _, dst, msg in answers["b"]:
            h.net.handlers[dst](msg)
        c.wait_idle(5.0)
        # A round that ends at a majority ends now; this one must not.  The wait only gives
        # such a round the time to end: the result checked below does not depend on it.
        t.join(0.2)
        assert t.is_alive(), f"the round ended without a's answer: {out}"
        for _, dst, msg in answers["a"]:
            h.net.handlers[dst](msg)
        t.join(10)
        assert not t.is_alive()
        assert out["r"][:2] == (True, r), out["r"]
        assert c.committed_root()[1] == r, "c did not learn the commit its holder knew"
    finally:
        h.close()
