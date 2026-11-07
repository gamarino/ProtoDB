"""A write permission ends only through the incarnation it was issued to (8.3f review, C1).

The 8.3f phase A review found that a restarted permission holder answered a duplicated copy of
a permit of its previous incarnation with ``done("none")``; the owner, which never checked the
incarnation, then issued the next permission on the same base while the previous incarnation's
root (same term, higher epoch) sat in the holder's acceptor log, and the next phase 1 chose it
over the committed root built on that base: an acknowledged commit was lost.

Since protocol 9 a ``permit`` names the requester's incarnation; any other incarnation drops it
unanswered, and the owner takes a ``done`` of another incarnation as an unknown outcome
(abandoning the term).  These tests replay the review's probe deterministically
(``tests/support/manual_cluster.py``: every message is delivered by hand, one fake clock).

``test_a_faulty_owner_issuing_one_base_twice_cannot_lose_a_commit`` plants the fault the
incarnation check removes -- an owner that issues a second permission on a base whose first
permission may have been used -- and is expected to FAIL (``xfail``, strict): with the
ranking by ``(label, epoch)`` of D46, S4 rests on the owner's sequencing code being correct
(D50 S2), as Raft rests on its leader.  Ruling R79 accepts that boundary and rules out the
alternatives (sibling resolution in phase 1, a ballot per permission); the test documents it.
"""
import threading
import time

import pytest

from proto_db.cluster.protocol import make_message
from tests.support.manual_cluster import GRACE, Harness, rec

ALL = ("request", "grant", "reject", "release", "renew", "permit_req", "permit", "permit_no", "done",
       "records", "replicate", "ack")


@pytest.fixture
def h():
    harness = Harness()
    yield harness
    harness.close()


def _owner_permit(h, name):
    """``name`` gets a permission from the owner ``a`` (its request and the permit delivered);
    returns ``(permission, copy of the permit message)``."""
    node, net, copies = h.nodes[name], h.net, []

    def pump():
        net.deliver(src=name, dst={"a"}, kinds=("permit_req",))
        for m in net.take("a", {name}, ("permit",)):
            copies.append(m)
            net.handlers[name](m[2])
    permission = h._run(lambda: node.permit(timeout=5), pump)
    assert not isinstance(permission, Exception), permission
    return permission, copies[0]


def _setup(h):
    """``a`` owns a term and commits R1 with everyone; returns ``(a, r1)``."""
    r0 = h.bootstrap("a")
    h.clock.t += 2 * GRACE
    a, net = h.nodes["a"], h.net

    def pump():
        net.deliver(src="a", dst={"b", "c"}, kinds=("request", "release"))
        net.deliver(src=None, dst={"a"}, kinds=("grant", "reject"))
    pa = h._run(lambda: a.permit(timeout=5), pump)
    r1 = rec(r0.epoch + 1)
    h.replicate("a", pa, r1, {"b", "c"})
    pa.outcome, pa.committed = "committed", (pa.term, r1)
    a.finish(pa)
    net.deliver(src="a", dst={"b", "c"}, kinds=("done",))
    return a, r1


def _replicate_alone_then_crash(h, name, permission, root):
    """``name`` replicates ``root`` under ``permission``, self-accepts, reaches nobody, and is
    restarted on its intact disk; returns the new incarnation."""
    node, out = h.nodes[name], {}

    def run():
        try:
            out["v"] = node.replicate(permission, root)
        except Exception as e:        # the outcome is unknown: reported by the caller's assertion
            out["e"] = e
    t = threading.Thread(target=run)
    t.start()
    deadline = time.monotonic() + 5
    while node.accepted_root()[1] != root and time.monotonic() < deadline:
        time.sleep(0.001)
    assert node.accepted_root()[1] == root
    h.net.drop(src=name, kinds=("records", "replicate"))
    restarted = h.restart(name)
    t.join(5)
    h.net.drop(src=name, kinds=ALL)            # the old incarnation's messages die with it
    assert "v" not in out
    return restarted


def test_a_restarted_holder_drops_a_permit_of_its_previous_incarnation(h):
    """The review's probe: the duplicate reaches the restarted holder, which no longer answers
    ``done("none")``; the owner never learns the outcome, abandons its term when the permission
    times out, and grants no permission on the base the crashed holder used.  The next lease's
    base is the crashed holder's root R2, never committed and never outranking a commit."""
    a, r1 = _setup(h)
    pb, dup = _owner_permit(h, "b")
    assert pb.root == r1
    r2 = rec(r1.epoch + 3)
    b2 = _replicate_alone_then_crash(h, "b", pb, r2)
    h.net.handlers["b"](dup[2])                # the duplicated permit reaches the new incarnation
    b2.wait_idle(5.0)
    assert [m for m in h.net.pending if m[0] == "b" and m[2]["type"] == "done"] == []
    assert b2.stats()["permits_foreign_dropped"] == 1
    # The permission times out on the owner's clock: the term is abandoned.
    h.clock.t += pb.remaining() + 1.0
    with a._cond:
        a._cond.notify_all()
    deadline = time.monotonic() + 5
    while a.stats()["terms_abandoned"] == 0 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert a.stats()["terms_abandoned"] == 1
    # c asks the owner for a permission: refused, never a second permission on base R1.
    c, net = h.nodes["c"], h.net

    def pump_c():
        net.deliver(src="c", dst={"a"}, kinds=("permit_req",))
        net.deliver(src="a", dst={"c"}, kinds=("permit", "permit_no"))
    issued = a.stats()["permits_issued"]
    got = h._run(lambda: c.permit(timeout=0.5), pump_c)
    assert a.stats()["permits_issued"] == issued
    if not isinstance(got, Exception):         # c became the owner of a new term itself
        assert got.term > pb.term
    net.drop(kinds=ALL)


def test_an_owner_counts_a_done_only_from_the_incarnation_it_issued_to(h):
    """A ``done`` naming another incarnation of the holder (even ``"none"``) ends the
    permission as an unknown outcome: the owner abandons the term."""
    a, r1 = _setup(h)
    pb, permit = _owner_permit(h, "b")
    assert permit[2]["inc"] == h.nodes["b"]._inc
    forged = make_message("done", "b", term=pb.term, seq=pb.seq, inc=h.nodes["b"]._inc + 1, outcome="none",
                          root=None, copies=[], ranges=None)
    h.net.handlers["a"](forged)
    a.wait_idle(5.0)
    assert a.stats()["terms_abandoned"] == 1
    h.net.drop(kinds=ALL)


def test_a_lost_permit_is_resent_on_a_retried_request(h):
    """Review M6: a permit lost on the wire is sent again when the holder retries its request
    after ``request_timeout``; the owner never issues a second permission for it."""
    a, r1 = _setup(h)
    b, net = h.nodes["b"], h.net
    lost = []

    def pump():
        net.deliver(src="b", dst={"a"}, kinds=("permit_req",))
        permits = net.take("a", {"b"}, ("permit",))
        if permits and not lost:
            lost.append(permits[0])                # the first permit is lost
        else:
            for m in permits:
                net.handlers["b"](m[2])
    pb = h._run(lambda: b.permit(timeout=5), pump)
    assert not isinstance(pb, Exception), pb
    assert lost and pb.seq == lost[0][2]["seq"]
    assert a.stats()["permits_issued"] == 2      # a's own first permission, and b's: one each
    assert b.stats()["permit_requests_retried"] >= 1
    net.drop(kinds=ALL)


@pytest.mark.xfail(strict=True, reason="ruling R79: S4 rests on the owner's sequencing code being correct; "
                                       "a planted double-issuing owner is outside the failure model")
def test_a_faulty_owner_issuing_one_base_twice_cannot_lose_a_commit(h):
    """Ruling R79 boundary.  Planted fault: the owner forgets the permission outstanding with b (as if it believed
    b's outcome were "none") and issues c a permission on the same base R1.  b's root R2 (its
    own acceptance only) and c's committed R3 are siblings; the chain rule keeps R2 from
    being committed, but phase 1 over {b, c} ranks R2 (higher epoch) above R3."""
    a, r1 = _setup(h)
    pb, _ = _owner_permit(h, "b")
    r2 = rec(r1.epoch + 3)
    _replicate_alone_then_crash(h, "b", pb, r2)
    with a._cond:                                  # the fault: the owner drops the outstanding permission
        a._ownership.pipeline.clear()
    pc, _ = _owner_permit(h, "c")
    assert pc.root == r1
    r3 = rec(r1.epoch + 1)
    h.replicate("c", pc, r3, {"a"})
    assert h.nodes["c"].committed_root()[1] == r3
    pc.outcome, pc.committed = "committed", (pc.term, r3)
    h.nodes["c"].finish(pc)
    h.net.drop(kinds=ALL)
    h.clock.t += 3 * GRACE
    lease = h.attempt("b", {"c"})
    assert not isinstance(lease, Exception), lease
    base = h.base("b", lease, {"c"})
    assert base[0] == r3, "the committed root R3 was lost: phase 1 chose the uncommitted R2"


def test_an_owner_whose_lease_expires_with_a_permission_outstanding_releases_as_uncertain(h):
    """Review I3: the outstanding permission's deadline runs past the lease's end; an owner
    whose lease expires first must not release its term as certain."""
    a, r1 = _setup(h)
    pb, _ = _owner_permit(h, "b")
    lease = a._ownership.lease
    h.clock.t += lease.remaining() + 0.001        # the lease ends; the permission's deadline has not
    assert a._ownership.pipeline
    with a._cond:
        a._cond.notify_all()
    deadline = time.monotonic() + 5
    while a._ownership is not None and time.monotonic() < deadline:
        time.sleep(0.005)
    assert a._ownership is None
    assert a.stats()["terms_abandoned"] == 1
    releases = [m for m in h.net.pending if m[0] == "a" and m[2]["type"] == "release" and m[2]["held"]]
    assert releases and all(m[2]["certain"] is False for m in releases)
    h.net.drop(kinds=ALL)


def test_a_permit_refusal_to_another_incarnation_is_dropped(h):
    """Re-review N1: ``permit_no`` echoes the requester's incarnation like every other reply.
    A refusal sent to a previous incarnation (request ids restart at every start) never
    refuses the current incarnation's request with the same id."""
    from proto_db.cluster.coordinator import _PermitWait
    b = h.nodes["b"]
    with b._cond:
        wait = b._permit_waits[1] = _PermitWait(1, time.monotonic())
    try:
        stale = make_message("permit_no", "a", term=1, rid=1, inc=b._inc + 1, reason="not_owner")
        b._on_permit_reply("a", stale)
        assert wait.reply is None, "a refusal to another incarnation refused this request"
        assert b.stats()["stale_replies"] == 1
        b._on_permit_reply("a", dict(stale, inc=b._inc))
        assert wait.reply == "refused"
    finally:
        with b._cond:
            b._permit_waits.pop(1, None)


def test_a_permit_arriving_after_its_request_gave_up_is_handed_back_at_once(h):
    """Task 8.3n: a permit that reaches its requester after ``permit()`` gave up (a delayed
    link) is handed back at once with ``done("none")``, so the owner issues the next
    permission without waiting for this one's validity.  Before, the hand-back message lacked
    the ``copies`` field that protocol 11 (Task 8.3g) added to ``done``: building it raised in
    the transport worker, nothing was sent, and the owner held the permission until it timed
    out (liveness only: an unanswered permission ends as an abandoned term, never a commit)."""
    a, r1 = _setup(h)
    b, c, net = h.nodes["b"], h.nodes["c"], h.net
    held = []

    def pump_b():
        net.deliver(src="b", dst={"a"}, kinds=("permit_req",))
        held.extend(net.take("a", {"b"}, ("permit",)))       # the permit is delayed past the wait
    got = h._run(lambda: b.permit(timeout=0.3), pump_b)
    assert isinstance(got, Exception), got
    assert held, "the owner issued no permit"
    for _, _, msg in held[:1]:
        net.handlers["b"](msg)                                # it arrives late: nobody waits
    b.wait_idle(5.0)
    back = net.take("b", {"a"}, ("done",))
    assert [m[2]["outcome"] for m in back] == ["none"], "the late permit was not handed back"
    for _, _, msg in back:
        net.handlers["a"](msg)
    a.wait_idle(5.0)
    assert not a._ownership.pipeline, "the owner still holds the handed-back permission"
    assert a.stats()["terms_abandoned"] == 0       # "none" of the only entry: the base is reused

    def pump_c():
        net.deliver(src="c", dst={"a"}, kinds=("permit_req",))
        net.deliver(src="a", dst={"c"}, kinds=("permit", "permit_no"))
    pc = h._run(lambda: c.permit(timeout=5), pump_c)   # the fake clock never moved
    assert not isinstance(pc, Exception), pc
    assert pc.root == r1
    net.drop(kinds=ALL)
