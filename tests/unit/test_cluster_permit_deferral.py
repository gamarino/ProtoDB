"""A permission request deferred by the fairness turn asks a known owner (Task 8.3f-B).

Measured on 3 TCP peers: a member whose request was deferred to a named turn waited in
``_await_turn`` while the named member owned the term and renewed it under load -- up to
``lease_duration`` (4.6-4.7 s with a 6 s lease) without one commit, while the owner served
the other members.  A deferral is timing only; once another member is known to own a term,
the request goes to that owner for a permission instead.
"""
import threading
import time

import pytest

from proto_db.cluster.protocol import make_message
from tests.support.cluster import Cluster

pytestmark = [pytest.mark.timeout(60)]

LEASE = 6.0


def test_a_deferred_request_is_served_by_the_busy_owner():
    """n1 defers to a named turn (n2, whose request was seen: up to ``lease_duration``); then
    n0 takes a term and keeps using it.  Before the fix n1 waited the whole lease (measured
    6.0 s here); now its request goes to n0 as soon as n0's term is known."""
    with Cluster(3, tiers=True, lease_duration=LEASE, request_timeout=0.25, startup_grace=0.0) as cl:
        cl.wait_rejoined()
        n0, n1 = cl["n0"], cl["n1"]
        with n1._cond:
            now = time.monotonic()
            n1._turn = ("n2", now, now)              # a release named n2, whose request was seen
            n1._owner_hint = (0, None)               # n1 does not know the owner yet
        stop = threading.Event()
        errors = []

        def busy():                                  # the owner keeps working: it never idles
            try:
                while not stop.is_set():
                    permission = n0.permit(timeout=5)
                    time.sleep(0.005)
                    n0.finish(permission)
            except Exception as e:                   # reported by the assertion below
                errors.append(e)
        got = {}

        def ask():
            permission = n1.permit(timeout=LEASE * 2)
            got["at"], got["owner"] = time.monotonic(), permission.owner
            n1.finish(permission)
        asker = threading.Thread(target=ask)
        asker.start()
        deadline = time.monotonic() + 5
        while n1.stats()["deferrals"] == 0 and time.monotonic() < deadline:
            time.sleep(0.001)                        # no owner exists yet: n1 defers to the turn
        assert n1.stats()["deferrals"] == 1
        worker = threading.Thread(target=busy)
        start = time.monotonic()
        worker.start()
        try:
            asker.join(LEASE * 2)
        finally:
            stop.set()
            worker.join(10)
        assert errors == [] and not asker.is_alive()
        assert got["owner"] == "n0", got
        seconds = got["at"] - start
        assert seconds < LEASE / 4, f"the deferred request waited {seconds:.2f} s for the turn"


def test_a_refusal_of_an_older_terms_request_keeps_the_owner_learnt_since():
    """Task 8.3n fix round 2: n1 asks n0 for a permission under n0's term T1; n0's term ends
    and n0 owns a newer term T2, which n1 learns (a renewal) before n0's refusal of the T1
    request ("not_owner") arrives.  The refusal is about T1; it must not make n1 forget that
    n0 owns T2.  Before the fix it did, and nothing restored it: n0's renewals of T2 teach
    only a term above the hint's, so n1 asked nobody while n0 kept T2 -- in the proof of
    fix round 2 a member made no commit for 30 s under a 0.25 s owner-to-member delay
    (``test_duplicates_delayed_beyond_a_holder_restart``)."""
    lease = 2.4
    with Cluster(3, tiers=True, lease_duration=lease, request_timeout=0.1, startup_grace=0.0) as cl:
        cl.wait_rejoined()
        n0, n1 = cl["n0"], cl["n1"]
        stop = threading.Event()
        errors = []

        def busy():                                  # n0 keeps asking: its term never idles
            try:
                while not stop.is_set():
                    permission = n0.permit(timeout=5)
                    time.sleep(0.005)
                    n0.finish(permission)
            except Exception as e:                   # reported by the assertions below
                errors.append(e)

        def wait_for(cond, what, timeout=10):
            deadline = time.monotonic() + timeout
            while not cond():
                assert time.monotonic() < deadline, what
                time.sleep(0.005)
        worker = threading.Thread(target=busy)
        worker.start()
        wait_for(lambda: n0._ownership is not None and n1.owner == "n0", "n1 learns that n0 owns a term")
        t1 = n0._ownership.term
        cl.net.partition({"n1"}, {"n0", "n2"})       # n1's request under T1 stays unanswered
        got = {}

        def ask():
            try:
                permission = n1.permit(timeout=lease * 4)
                got["at"], got["owner"] = time.monotonic(), permission.owner
                n1.finish(permission)
            except Exception as e:
                got["error"] = e
        asker = threading.Thread(target=ask)
        asker.start()
        try:
            wait_for(lambda: len(n1._permit_waits) == 1, "n1 asks n0 under T1")
            rid = next(iter(n1._permit_waits))
            stop.set()
            worker.join(10)
            wait_for(lambda: n0._ownership is None, "n0's term T1 ends (idle)")
            stop.clear()
            worker = threading.Thread(target=busy)
            worker.start()
            wait_for(lambda: n0._ownership is not None, "n0 owns a new term")
            t2 = n0._ownership.term
            assert t2 > t1
            with n1._cond:
                n1._note_owner(t2)                   # a renewal of T2 reaches n1 first
            assert n1.owner == "n0"
            # Then n0's refusal of the T1 request (a retry reached n0 after T1 ended).
            n1._on_permit_reply("n0", make_message("permit_no", "n0", term=t2, rid=rid, inc=n1._inc,
                                                   reason="not_owner"))
            assert n1.owner == "n0", "a refusal about T1 made n1 forget that n0 owns T2"
            healed = time.monotonic()
            cl.net.heal()
            asker.join(lease * 4)
        finally:
            stop.set()
            worker.join(10)
            cl.net.heal()
        assert errors == [] and not asker.is_alive()
        assert got.get("owner") == "n0", got
        assert n0.stats()["ownerships"] >= 2
        seconds = got["at"] - healed
        assert seconds < lease / 2, f"n1 waited {seconds:.2f} s for a permission from n0 after the heal"


def _silent_owner_node(lease: float):
    """A coordinator that is never started (no threads), whose sends are recorded and never
    answered: an owner that stays silent (re-review 1, N4)."""
    from proto_db.cluster.coordinator import ClusterCoordinator, ballot_after
    from proto_db.cluster.transport import InProcessNetwork
    net = InProcessNetwork()
    peers = [(n, "127.0.0.1", 0) for n in ("n0", "n1", "n2")]
    c = ClusterCoordinator("n1", peers, transport=net.endpoint(), lease_duration=lease, request_timeout=lease / 10)
    c._state = "running"
    sent = []
    c._transport.send = lambda to, msg: sent.append((to, msg))
    t1 = ballot_after(0, c._ranks["n0"])
    t2 = ballot_after(t1, c._ranks["n0"])
    with c._cond:
        c._note_owner(t1)
    assert c.owner == "n0"
    return net, c, sent, t1, t2


@pytest.mark.parametrize("learnt", [False, True])
def test_a_silent_owner_is_forgotten_only_for_the_term_asked_under(learnt):
    """The silence branch of 40685bc (re-review 1, N4): n1 asks n0 under T1 and gets no
    answer for its wait.  Silence about T1 clears the hint (n1 then tries to own) -- unless
    n1 learnt meanwhile that n0 owns a newer term T2 (its renewals reached n1), which the
    silence says nothing about: then the hint stays (T2, n0).

    The order is enforced, not timed: n1's request is observed sent before T2 is learnt, and
    the wait ends at its deadline (one lease at most).  The plant ``silence_forgets`` reverts the
    branch and fails ``learnt=True``."""
    lease = 0.6
    net, c, sent, t1, t2 = _silent_owner_node(lease)
    try:
        out = {}
        worker = threading.Thread(target=lambda: out.setdefault("r", c._permission_from_owner(time.monotonic() + lease)))
        worker.start()
        deadline = time.monotonic() + lease / 2
        while not any(m["type"] == "permit_req" for _, m in sent):
            assert time.monotonic() < deadline, "n1 never asked n0"
            time.sleep(0.001)
        assert [(to, m["term"]) for to, m in sent if m["type"] == "permit_req"][0] == ("n0", t1)
        if learnt:
            with c._cond:
                c._note_owner(t2)                    # a renewal of T2 reaches n1 during the wait
        worker.join(lease * 4)
        assert not worker.is_alive() and out["r"] is None     # silence: no permission
        if learnt:
            assert c._owner_hint == (t2, "n0"), f"silence about T1 made n1 forget that n0 owns T2: {c._owner_hint}"
        else:
            assert c._owner_hint == (t1, None), c._owner_hint      # silence about T1 clears it
    finally:
        net.close()
