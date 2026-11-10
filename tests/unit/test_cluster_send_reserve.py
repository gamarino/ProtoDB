"""A holder sends nothing a voter may accept with less than ``send_reserve`` of its
permission left (Task 8.3n fix round 3, re-review finding N2, ruling R121).

Under contention, a remote holder's permission is measured from its request's first send (the
D45 argument), so the time the request waited in the owner's queue is charged to it.  A holder
then started replications with 5-93 ms left; each ran until the permission ended, so its
acknowledgements arrived too late and the outcome was unknown (P5).  An unknown outcome
abandons the owner's whole term (O6), and every member paid an election.  A send that cannot
finish turns what would be a truthful conflict (H7: nothing acceptable left the node) into an
unknown outcome plus a term abandonment.

The holder now needs ``send_reserve`` (default ``request_timeout``) of validity left before it
sends anything acceptable; with less, nothing leaves and the commit ends with a retryable
``ProtoConflictError``, and the ``done("none")`` that follows keeps the owner's term (O5).

Real coordinators over :class:`~tests.support.manual_cluster.ManualNet` and one fake clock.
"""
import pytest

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.errors import ProtoConflictError, ProtoValidationError
from tests.support.manual_cluster import Harness, rec

#: Every message that carries the commit's root or records to another member.
SENT = ("records", "replicate", "propose", "store", "warm")
ALL = ("request", "grant", "reject", "release", "renew", "permit_req", "permit", "permit_no", "done",
       "records", "replicate", "ack", "propose", "store", "warm")


@pytest.fixture
def h():
    harness = Harness()
    yield harness
    harness.close()


def _replicate(h, name, permission, root):
    """``name`` replicates ``root`` under ``permission``.  Every message it sends is recorded
    and delivered, and the acknowledgements come back; once a root has left, the clock is
    moved past the permission, as a send that cannot finish in what is left would see it.
    Returns ``(result or error, messages sent)``."""
    sent = []
    others = set(h.names) - {name}

    def pump():
        for src, dst, msg in h.net.take(name, None, SENT):
            sent.append(msg["type"])
            h.net.handlers[dst](msg)
            h.nodes[dst].wait_idle(5.0)
        if sent:
            h.clock.t = max(h.clock.t, permission.valid_until + 0.001)
        h.net.deliver(src=None, dst={name}, kinds=("ack",))
    out = h._run(lambda: h.nodes[name].replicate(permission, root), pump)
    h.net.drop(src=name, dst=others, kinds=("ack",))
    return out, sent


def test_a_holder_with_less_than_the_reserve_left_sends_nothing_and_gets_a_retryable_conflict(h):
    a, b = h.nodes["a"], h.nodes["b"]
    r1 = h.own("a")
    pb, _ = h.permit_from("b", "a")
    reserve = b._send_reserve
    assert reserve == b._request_timeout            # the default: request_timeout (below belief / 4)
    h.clock.t = pb.valid_until - reserve / 2        # valid, with half the reserve left
    assert pb.is_valid() and pb.remaining() < reserve
    accepted = b.accepted_root()
    r2 = rec(r1.epoch + 1)
    out, sent = _replicate(h, "b", pb, r2)
    assert isinstance(out, ProtoConflictError), out
    assert sent == []                               # nothing a voter may accept left b
    assert b.accepted_root() == accepted            # and b did not accept it itself
    assert not pb.sent_acceptable
    assert b.stats().get("sends_refused_reserve") == 1
    # The done that follows is "none": the owner keeps its term (O5), no election.
    b.finish(pb)
    h.net.deliver(src="b", dst={"a"}, kinds=("done",))
    assert a.stats()["terms_abandoned"] == 0
    assert a._ownership is not None and not a._ownership.abandoned
    h.net.drop(kinds=ALL)


def test_a_holder_with_the_reserve_left_replicates_as_before(h):
    b = h.nodes["b"]
    r1 = h.own("a")
    pb, _ = h.permit_from("b", "a")
    h.clock.t = pb.valid_until - b._send_reserve      # exactly the reserve left
    r2 = rec(r1.epoch + 1)
    sent = []

    def pump():
        for src, dst, msg in h.net.take("b", None, SENT):
            sent.append(msg["type"])
            h.net.handlers[dst](msg)
            h.nodes[dst].wait_idle(5.0)
        h.net.deliver(src=None, dst={"b"}, kinds=("ack",))
    out = h._run(lambda: b.replicate(pb, r2), pump)
    assert not isinstance(out, Exception), out
    assert "replicate" in sent
    assert b.stats().get("sends_refused_reserve", 0) == 0
    h.net.drop(kinds=ALL)


def test_the_reserve_is_configurable_and_must_leave_room_in_the_belief():
    peers = [(n, "127.0.0.1", 0) for n in "abc"]
    from proto_db.cluster.transport import InProcessNetwork
    net = InProcessNetwork()
    try:
        c = ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=2.0, request_timeout=0.1)
        assert c._send_reserve == 0.1              # request_timeout, below belief / 4 (0.167 s)
        c = ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=0.9, request_timeout=0.1)
        assert c._send_reserve == pytest.approx(0.075)     # capped at belief / 4 (0.3 s / 4)
        c = ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=0.9, request_timeout=0.1,
                               send_reserve=0.0)
        assert c._send_reserve == 0.0              # 0 restores the old rule (any validity left)
        with pytest.raises(ProtoValidationError):
            ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=0.9, request_timeout=0.1,
                               send_reserve=0.1)     # beyond belief / 4: the D/4 rule issues with 0.075 s
        with pytest.raises(ProtoValidationError):
            ClusterCoordinator("a", peers, transport=net.endpoint(), lease_duration=0.9, request_timeout=0.1,
                               send_reserve=-0.1)
    finally:
        net.close()


def test_a_permission_issued_at_the_d4_threshold_is_usable_whatever_the_request_timeout():
    """Fix round 3 proof: with ``request_timeout`` above a quarter of the belief (a 1 s lease
    and 0.25 s in the crash tests), a reserve of ``request_timeout`` refused permissions the
    owner is allowed to issue (D50's D/4 rule: with at least belief / 4 left), and the
    members of the crash tests committed nothing after one of three died.  The default
    reserve is capped at belief / 4: a permission issued at the threshold and received at
    once is used."""
    h = Harness(request_timeout=3.0)                  # belief 10 / 3 s: a quarter is 0.83 s < 3 s
    try:
        a, b = h.nodes["a"], h.nodes["b"]
        quarter = a._belief / 4
        r1 = h.own("a")
        own = a._ownership
        h.clock.t = own.lease.valid_until - quarter - 0.01     # the owner may still issue (D/4)
        pb, _ = h.permit_from("b", "a")
        assert pb.remaining() >= quarter
        sent = []

        def pump():
            for src, dst, msg in h.net.take("b", None, SENT):
                sent.append(msg["type"])
                h.net.handlers[dst](msg)
                h.nodes[dst].wait_idle(5.0)
            h.net.deliver(src=None, dst={"b"}, kinds=("ack",))
        out = h._run(lambda: b.replicate(pb, rec(r1.epoch + 1)), pump)
        assert not isinstance(out, Exception), out
        assert "replicate" in sent and b.stats()["sends_refused_reserve"] == 0
        h.net.drop(kinds=ALL)
    finally:
        h.close()
