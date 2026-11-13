"""The lease fairness mechanism, schedule by schedule (Task 8.3e, defect D-LIVE-1, D46).

Releases name the member seen waiting longest (``next``) and say whether the releaser still
wants the lease (``more``); a member that receives a release naming another member defers its
own requests to it; a member nobody queues says ``want`` before deferring.  These hints only
time requests: grants and acceptances never read them.  Each test drives real coordinators over
the manual network (:mod:`tests.support.manual_cluster`).
"""
import threading
import time

import pytest

from proto_db.cluster.lease import Lease
from proto_db.errors import ProtoTimeoutError, ProtoValidationError
from tests.support.manual_cluster import Harness

pytestmark = [pytest.mark.timeout(30)]


def _refused(h, name, others):
    """``name`` asks for the lease once while another member holds it: every request is
    delivered, refused, and the cancel delivered too."""
    outcome = h.attempt(name, set(others), timeout=0.05)
    assert not isinstance(outcome, Lease), outcome


def _released(h, name, lease):
    """``name`` releases ``lease``; return the release message (delivered to nobody yet)."""
    h.nodes[name].release(lease)
    [(_, _, msg)] = h.net.take(name, None, ("release",))[:1] or [(None, None, None)]
    return msg


def _deliver_release(h, msg, to):
    for node in to:
        h.net.handlers[node](msg)
        h.nodes[node].wait_idle(5.0)


def test_a_release_names_the_member_waiting_longest_then_the_next():
    h = Harness()
    try:
        lease = h.attempt("a", {"b", "c"})
        _refused(h, "b", {"a", "c"})            # b asks first ...
        time.sleep(0.002)
        _refused(h, "c", {"a", "b"})            # ... then c
        h.net.drop()
        msg = _released(h, "a", lease)
        assert msg["held"] and msg["next"] == "b", msg
        _deliver_release(h, msg, {"b", "c"})
        lease_b = h.attempt("b", {"a", "c"})
        assert isinstance(lease_b, Lease)
        msg = _released(h, "b", lease_b)
        assert msg["next"] == "c", msg          # b was served: c is next
        assert h.nodes["a"].stats()["turns_named"] == 1 and h.nodes["b"].stats()["turns_taken"] == 1
    finally:
        h.close()


def test_without_waiting_members_a_release_names_nobody_and_nobody_defers():
    h = Harness()
    try:
        lease = h.attempt("a", {"b", "c"})
        msg = _released(h, "a", lease)
        assert msg["next"] is None and msg["more"] is False
        _deliver_release(h, msg, {"b", "c"})
        started = time.monotonic()
        assert isinstance(h.attempt("a", {"b", "c"}), Lease)
        assert time.monotonic() - started < 1.0
        assert h.nodes["a"].stats()["deferrals"] == 0
    finally:
        h.close()


def _background_acquire(node, timeout):
    """Start ``node.acquire(timeout)`` in a thread; the outcome (a lease, the
    ``ProtoTimeoutError``, or the ``ProtoValidationError`` of a coordinator stopped by the test)
    lands in the returned dict under ``"out"``."""
    result = {}

    def run():
        try:
            result["out"] = node.acquire(timeout=timeout)
        except (ProtoTimeoutError, ProtoValidationError) as e:     # timed out, or the test stopped it
            result["out"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    result["thread"] = t
    return result


def _requests_from(h, name):
    with h.net.lock:
        return [m for m in h.net.pending if m[0] == name and m[2]["type"] == "request"]


def _releaser_defers_trial():
    """One trial of the test below; ``None`` when it was inconclusive (see there), else True."""
    h = Harness()
    t = None
    try:
        lease = h.attempt("a", {"b", "c"})
        _refused(h, "b", {"a", "c"})
        h.net.drop()
        msg = _released(h, "a", lease)
        assert msg["next"] == "b"
        _deliver_release(h, msg, {"b", "c"})
        t = _background_acquire(h.nodes["a"], 5.0)["thread"]
        time.sleep(0.02)
        assert not _requests_from(h, "a"), "a asked before b's turn"
        # b takes its turn: a sees b's request (its grant reply is lost), c grants.
        lease_b = h.attempt("b", {"c"}, informed={"a"})
        assert isinstance(lease_b, Lease), lease_b
        if h.nodes["a"].stats()["deferral_timeouts"]:
            # b's request reached a after the silence bound (50 ms of real time from the
            # release): a stopped deferring by design, so this trial says nothing about the
            # deferral it tests.  Measured: a 60 ms pause of the test thread here reproduces it.
            return None
        time.sleep(0.15)                         # well past the silence bound (50 ms)
        assert not _requests_from(h, "a"), "a asked during b's lease"
        msg_b = _released(h, "b", lease_b)
        _deliver_release(h, msg_b, {"a"})        # b was served: a asks now
        deadline = time.monotonic() + 5
        while not _requests_from(h, "a") and time.monotonic() < deadline:
            time.sleep(0.001)
        assert _requests_from(h, "a"), "a never asked after b's turn"
        stats = h.nodes["a"].stats()
        assert stats["deferrals"] == 1 and stats["deferral_timeouts"] == 0, stats
        return True
    finally:
        h.close()
        if t is not None:
            t.join(10)


def test_the_releaser_defers_to_the_member_it_named_until_that_member_has_been_served():
    """The schedule needs b's request to reach a within the real-time silence bound; a scheduler
    pause longer than that ends a's deferral by design (a timing bound, never a safety rule).
    Such a trial is inconclusive and repeated; one conclusive trial is required."""
    for _ in range(5):
        if _releaser_defers_trial():
            return
    pytest.fail("no conclusive trial in 5: b's request never reached a within the silence bound")


def test_a_named_member_that_does_not_ask_costs_one_silence_and_is_not_named_again():
    h = Harness()
    try:
        lease = h.attempt("a", {"b", "c"})
        _refused(h, "b", {"a", "c"})
        h.net.drop()
        msg = _released(h, "a", lease)
        assert msg["next"] == "b"
        _deliver_release(h, msg, {"c"})
        started = time.monotonic()
        lease_c = h.attempt("c", {"a", "b"})     # b never asks: c defers ~50 ms, then asks
        waited = time.monotonic() - started
        assert isinstance(lease_c, Lease)
        assert 0.04 <= waited < 2.0, waited
        assert h.nodes["c"].stats()["deferral_timeouts"] == 1
        msg = _released(h, "c", lease_c)
        assert msg["next"] is None, "b, silent since its turn was named, was named again"
    finally:
        h.close()


def test_a_member_nobody_queues_says_want_before_it_defers():
    h = Harness()
    try:
        lease = h.attempt("a", {"b", "c"})
        _refused(h, "b", {"a", "c"})
        h.net.drop()
        msg = _released(h, "a", lease)          # names b; c is queued nowhere
        _deliver_release(h, msg, {"b", "c"})
        t = _background_acquire(h.nodes["c"], 0.3)["thread"]
        deadline = time.monotonic() + 5
        wants = []
        while not wants and time.monotonic() < deadline:
            wants = h.net.take("c", None, ("want",))
            time.sleep(0.001)
        assert {dst for _, dst, _ in wants} == {"a", "b"}, wants
        assert not _requests_from(h, "c"), "c asked during b's turn"
        for _, dst, m in wants:
            h.net.handlers[dst](m)
            h.nodes[dst].wait_idle(5.0)
        # b takes its turn; its release names c, whom it now queues.
        lease_b = h.attempt("b", {"a"})
        assert isinstance(lease_b, Lease)
        assert _released(h, "b", lease_b)["next"] == "c"
        t.join(5)
    finally:
        h.close()


def test_fairness_hints_never_change_a_grant():
    """A member deferring to b still grants c's request (c never received the release): the
    hints time this node's own requests only; the grant rules are unchanged (D46 Amendment,
    Task 8.3e)."""
    h = Harness()
    try:
        lease = h.attempt("a", {"b", "c"})
        _refused(h, "b", {"a", "c"})
        h.net.drop()
        msg = _released(h, "a", lease)
        assert msg["next"] == "b"
        _deliver_release(h, msg, {"b"})          # a and b defer to b's turn ... c knows nothing
        lease_c = h.attempt("c", {"a", "b"})
        assert isinstance(lease_c, Lease), lease_c   # granted by a (and c), whatever the turn
        assert lease_c.term > lease.term
    finally:
        h.close()


def test_a_releaser_with_threads_still_waiting_says_more_and_is_queued_at_the_tail():
    """``a`` releases while another of its threads waits: its release says ``more``, so ``b``
    (named next) queues ``a`` behind itself and names it when it releases, without ``a``
    having to ask or say ``want``."""
    h = Harness()
    second = None
    try:
        lease = h.attempt("a", {"b", "c"})
        _refused(h, "b", {"a", "c"})
        h.net.drop()
        second = _background_acquire(h.nodes["a"], 5.0)
        deadline = time.monotonic() + 5
        while h.nodes["a"]._acquirers < 1 and time.monotonic() < deadline:
            time.sleep(0.001)
        msg = _released(h, "a", lease)
        assert msg["next"] == "b" and msg["more"] is True, msg
        _deliver_release(h, msg, {"b", "c"})
        time.sleep(0.02)
        assert not h.net.take("a", None, ("want",)), "a is queued already: no want needed"
        assert not _requests_from(h, "a"), "a asked before b's turn"
        lease_b = h.attempt("b", {"c"}, informed={"a"})
        assert isinstance(lease_b, Lease), lease_b
        assert _released(h, "b", lease_b)["next"] == "a"
    finally:
        h.close()
        if second is not None:
            second["thread"].join(10)


# -- wrong hints: safety unaffected, liveness bounded (8.3e review M6) -----------------------

def _named_release(h, holder="a", named="b"):
    """``holder`` wins a lease while ``named`` waits (so the release names it); return the
    holder's lease and its release message, delivered to nobody yet."""
    lease = h.attempt(holder, set("abc") - {holder})
    _refused(h, named, set("abc") - {named})
    h.net.drop()
    msg = _released(h, holder, lease)
    assert msg["next"] == named, msg
    return lease, msg


def _bounded_lease(h, name, granters, after_term):
    """``name`` asks for the lease: it must get it, with a term above ``after_term`` (the grant
    rules are untouched by any hint), after at most one silence bound plus slack."""
    started = time.monotonic()
    lease = h.attempt(name, set(granters))
    waited = time.monotonic() - started
    assert isinstance(lease, Lease), lease
    assert lease.term > after_term
    assert waited < 1.0, f"{name} waited {waited:.3f} s"
    return lease, waited


def test_a_duplicated_release_costs_no_more_than_one_bound():
    h = Harness()
    try:
        lease, msg = _named_release(h)
        _deliver_release(h, msg, {"c"})
        _deliver_release(h, msg, {"c"})                       # duplicate: same turn, same bound
        lease_c, waited = _bounded_lease(h, "c", {"a", "b"}, lease.term)
        assert waited >= 0.04, waited                         # it did defer to b (who is silent)
        stats = h.nodes["c"].stats()
        assert stats["deferrals"] == 1 and stats["deferral_timeouts"] == 1, stats
        h.nodes["c"].release(lease_c)
    finally:
        h.close()


def test_a_stale_release_delivered_after_the_named_member_was_served_costs_one_bound():
    h = Harness()
    try:
        lease, msg = _named_release(h)
        _deliver_release(h, msg, {"b", "c"})
        lease_b = h.attempt("b", {"a"}, informed={"c"})       # b takes its turn; c sees its request
        assert isinstance(lease_b, Lease), lease_b
        msg_b = _released(h, "b", lease_b)
        _deliver_release(h, msg_b, {"c"})                     # b served: c's deferral is over ...
        _deliver_release(h, msg, {"c"})                       # ... until a's old release arrives again
        lease_c, waited = _bounded_lease(h, "c", {"a", "b"}, lease_b.term)
        assert h.nodes["c"].stats()["deferral_timeouts"] == 1  # b does not ask again: one bound
    finally:
        h.close()


def test_releases_delivered_in_reverse_order_cost_one_bound():
    h = Harness()
    try:
        lease, msg = _named_release(h)
        _deliver_release(h, msg, {"b"})
        lease_b = h.attempt("b", {"a"})                       # c sees nothing of b's turn
        assert isinstance(lease_b, Lease), lease_b
        msg_b = _released(h, "b", lease_b)
        _deliver_release(h, msg_b, {"c"})                     # the newer release first,
        _deliver_release(h, msg, {"c"})                       # then the one naming b
        lease_c, waited = _bounded_lease(h, "c", {"a", "b"}, lease_b.term)
        assert waited >= 0.04 and h.nodes["c"].stats()["deferral_timeouts"] == 1
    finally:
        h.close()


@pytest.mark.parametrize("named", ["a", "zz", "c"])
def test_a_wrong_next_never_changes_a_grant_and_costs_at_most_one_bound(named):
    """A release naming its own sender (``a``; an honest member never does), a non-member
    (``zz``; ignored on receipt), or the receiver itself (``c``: it asks at once)."""
    h = Harness()
    try:
        lease, msg = _named_release(h)
        forged = dict(msg, next=named)
        _deliver_release(h, forged, {"c"})
        lease_c, waited = _bounded_lease(h, "c", {"a", "b"}, lease.term)
        stats = h.nodes["c"].stats()
        if named == "a":
            assert stats["deferral_timeouts"] == 1, stats     # a never asks: one silence bound
        else:
            assert stats["deferrals"] == 0, stats             # nobody else's turn: no deferral
    finally:
        h.close()
