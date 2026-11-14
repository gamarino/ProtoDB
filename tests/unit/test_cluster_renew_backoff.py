"""A lost renewal no longer renews again once per round trip (Task RENEW-BACKOFF, ruling R149,
GitHub issue #1, D50 amendment).

Before the fix, the ownership thread renewed again at once after a lost renewal: a majority of
refusals ends a renewal in one round trip, so the owner sent one ``renew`` per round trip, and
every voter made one grant decision, until its lease ended.  Now:

* **fenced** -- when the permanent refusals (``stale_term``, ``term_taken``, or any reason
  with a highest term above the owner's) exceed ``size - needed``, the owner stops renewing,
  issues nothing new, refuses its queue with ``"ending"``, lets an outstanding permission end
  by its ``done`` (or its deadline), then releases with the end reason ``"fenced"``;
* **transient** -- any other lost renewal backs off until
  ``now + min(remaining, max(busy_remaining, request_timeout / 4, 0.001))`` on the
  coordinator's clock.

Every case runs real coordinators over a :class:`~tests.support.manual_cluster.ManualNet` with
one fake clock: every message is delivered or dropped by hand, and every deadline the
assertions read is on the fake clock.  Real time is only given for the ownership thread to act
(or, where the property is that it does nothing, to show that it does not)."""
import threading
import time

from proto_db.cluster.coordinator import _Ownership, ballot_after
from proto_db.cluster.lease import Lease
from proto_db.cluster.protocol import make_message
from tests.support.manual_cluster import Harness

#: Real seconds the ownership thread is given to act on a change of the fake clock.
SETTLE = 0.3


class _Pump:
    """Delivers ``a``'s renewals to the other members and their replies to ``a`` from a thread,
    recording the fake time of every renewal ``a`` sent; every other message from ``a`` is
    kept in ``sent`` (and taken off the net) for the test to inspect."""

    def __init__(self, h, owner="a"):
        self.h, self.owner = h, owner
        self.others = set(h.names) - {owner}
        self.renewals = []              # fake time of each renew broadcast (one per round)
        self.sent = []                  # (destination, message) of every other message of the owner
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        net = self.h.net
        while not self.stop.is_set():
            renews = net.take(self.owner, self.others, ("renew",))
            rids = {m[2]["rid"] for m in renews}
            with self.lock:
                self.renewals.extend(self.h.clock.t for _ in rids)
            for _, to, msg in renews:
                net.handlers[to](msg)
                net.handlers[to].__self__.wait_idle(5.0)
            net.deliver(src=None, dst={self.owner}, kinds=("grant", "reject"))
            other = net.take(self.owner, None, ("release", "permit_no", "permit", "done"))
            with self.lock:
                self.sent.extend((to, msg) for _, to, msg in other)
            time.sleep(0.001)

    def count(self):
        with self.lock:
            return len(self.renewals)

    def messages(self, kind):
        with self.lock:
            return [(to, m) for to, m in self.sent if m["type"] == kind]

    def close(self):
        self.stop.set()
        self.thread.join(5)


def _wait_for(cond, what, node, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, f"timed out waiting for {what}: {node.stats()}"
        time.sleep(0.002)


def _poke(node):
    with node._cond:
        node._cond.notify_all()


def _promise_higher(h, owner_term, names, proposer="c"):
    """``names`` promise a higher ballot of ``proposer`` (a rejoin's ballot, ruling R78a): the
    effect of :meth:`ClusterCoordinator._on_ballot_query` on their grant tables."""
    higher = ballot_after(owner_term, h.nodes["a"]._ranks[proposer])
    for n in names:
        node = h.nodes[n]
        with node._cond:
            node._grants.promise(higher)
    return higher


def _remote_request(h, owner, sender, term, rid):
    """``sender``'s permit request reaches ``owner`` (as the transport would hand it over)."""
    msg = make_message("permit_req", sender, term=term, rid=rid, inc=h.nodes[sender]._inc)
    h.net.handlers[owner](msg)
    h.nodes[owner].wait_idle(5.0)


def test_a_fenced_term_steps_down_after_one_renewal():
    """A majority (b and c) promised a higher ballot; the owner a renews once, sees the term
    fenced, refuses its queue with ``ending`` and releases: one renewal, never a second."""
    h = Harness("abc")
    pump = None
    try:
        a = h.nodes["a"]
        h.own("a")
        own = a._ownership
        assert own is not None
        pump = _Pump(h)
        before = a.stats()
        _promise_higher(h, own.term, ("b", "c"))
        # Too little lease for a new permission (D50's D/4 rule) and a renewal due: b's request
        # waits in the queue for the renewal.
        h.clock.t = own.lease.valid_until - a._belief / 8
        _remote_request(h, "a", "b", own.term, rid=50_001)
        _poke(a)
        _wait_for(lambda: a._ownership is None and own.lease.released_at is not None, "the fenced end", a)
        assert not own.abandoned, "the fenced term was abandoned while its lease is valid"
        time.sleep(SETTLE)                   # a second renewal, if any, would be sent now
        stats = a.stats()
        assert pump.count() == 1, f"renewals sent: {pump.renewals}"
        assert stats["renewal_failures"] - before["renewal_failures"] == 1, stats
        assert stats["terms_fenced"] - before["terms_fenced"] == 1, stats
        assert stats["terms_abandoned"] == before["terms_abandoned"], stats
        assert own.fenced and own.ended
        refused = [m for to, m in pump.messages("permit_no") if to == "b" and m["rid"] == 50_001]
        assert refused and refused[0]["reason"] == "ending", pump.sent
        assert any(m["term"] == own.term for _, m in pump.messages("release")), pump.sent
    finally:
        if pump is not None:
            pump.close()
        h.close()


def test_a_fenced_term_lets_its_outstanding_permission_end_by_its_done():
    """As above, with a's own permission outstanding at fencing time: it is never abandoned
    while its lease is valid, b's queued request is refused ``ending`` at once, and the release
    follows the permission's ``done``."""
    h = Harness("abc")
    pump = None
    try:
        a = h.nodes["a"]
        h.own("a")
        own = a._ownership
        pump = _Pump(h)
        before = a.stats()
        permission = a.permit(timeout=5)                # outstanding, never proposed
        _remote_request(h, "a", "b", own.term, rid=50_002)   # queued behind it (rule O1)
        assert [q[1] for q in own.queue] == [50_002]
        _promise_higher(h, own.term, ("b", "c"))
        h.clock.t = own.lease.valid_until - a._belief / 4   # a renewal due, the lease still valid
        _poke(a)
        _wait_for(lambda: own.fenced, "the fencing", a)
        assert not own.abandoned, "the fenced term was abandoned while its lease is valid"
        _wait_for(lambda: pump.messages("permit_no"), "b's refusal", a)
        time.sleep(SETTLE)
        assert own.lease.is_valid() and permission.is_valid()
        assert not own.abandoned, "the fenced term was abandoned while its lease is valid"
        assert a._ownership is own and own.lease.released_at is None
        assert not pump.messages("release"), pump.sent
        assert pump.count() == 1, f"renewals sent: {pump.renewals}"
        refused = [m for to, m in pump.messages("permit_no") if to == "b" and m["rid"] == 50_002]
        assert refused and refused[0]["reason"] == "ending", pump.sent
        # The fenced term issues nothing new: another request is refused, never queued.
        _remote_request(h, "a", "c", own.term, rid=50_003)
        assert not own.queue and not own.pipeline[1:]
        _wait_for(lambda: any(to == "c" for to, _ in pump.messages("permit_no")), "c's refusal", a)
        permission.outcome = "none"
        a.finish(permission)
        _wait_for(lambda: a._ownership is None and own.lease.released_at is not None, "the release", a)
        stats = a.stats()
        assert stats["terms_abandoned"] == before["terms_abandoned"], stats
        assert stats["terms_fenced"] - before["terms_fenced"] == 1, stats
        assert stats["renewal_failures"] - before["renewal_failures"] == 1, stats
        assert any(m["term"] == own.term for _, m in pump.messages("release")), pump.sent
    finally:
        if pump is not None:
            pump.close()
        h.close()


def test_a_transient_refusal_backs_off_until_the_refusals_remaining():
    """A majority (b and c) answers ``starting`` with ``remaining = r``: no second renewal is
    sent before ``r`` has passed on the fake clock, and the renewal after it wins."""
    h = Harness("abc")
    pump = None
    try:
        a = h.nodes["a"]
        h.own("a")
        own = a._ownership
        pump = _Pump(h)
        before = a.stats()
        r = a._belief / 8
        assert r > a._request_timeout / 4
        start = own.lease.valid_until - a._belief / 3          # a renewal due, r well inside the lease
        h.clock.t = start
        for n in ("b", "c"):
            g = h.nodes[n]._grants
            with h.nodes[n]._cond:
                g._quiet_until = start + r      # a startup grace ending at r (their grant to a kept)
        # Demand without a permission: the renewal is due while a's threads keep asking.
        own.last_demand = time.monotonic()
        _poke(a)
        _wait_for(lambda: a.stats()["renewal_failures"] > before["renewal_failures"], "the lost renewal", a)
        time.sleep(SETTLE)                          # a renewal sent at once would be counted now
        assert pump.count() == 1, f"renewals sent at {start} before r: {len(pump.renewals)}"
        assert abs(own.renew_after - (start + r)) < 1e-9, (own.renew_after, start + r)
        h.clock.t = start + r / 2                   # still before r
        own.last_demand = time.monotonic()
        _poke(a)
        time.sleep(SETTLE)
        assert pump.count() == 1, f"renewals sent before r: {pump.renewals}"
        h.clock.t = start + r + 1e-6                # r has passed: the grace is over
        own.last_demand = time.monotonic()
        _poke(a)
        _wait_for(lambda: a.stats()["renewals"] > before["renewals"], "the renewal after r", a)
        stats = a.stats()
        assert pump.count() == 2 and pump.renewals[1] >= start + r, pump.renewals
        assert stats["renewal_failures"] - before["renewal_failures"] == 1, stats
        assert stats["terms_fenced"] == before["terms_fenced"], stats
        assert own.renew_after is None and not own.fenced and a._ownership is own
        assert own.lease.valid_until > start + r
    finally:
        if pump is not None:
            pump.close()
        h.close()


def test_a_minority_busy_with_a_lower_term_never_fences_the_owner():
    """b never granted a's term and answers ``busy`` with a grant to c for a LOWER term (an
    attempt of c that lost).  While c grants, a's renewal wins.  When c also refuses for good
    (it promised a higher ballot), the round is lost with one permanent refusal (c) of the one
    allowed: a backs off, never fenced -- though the highest term seen is above its own --
    and once b's grant to c has ended, a's renewal is granted by b and wins."""
    h = Harness("abc")
    try:
        a, b, c = h.nodes["a"], h.nodes["b"], h.nodes["c"]
        h.bootstrap("a")
        h.clock.t += 10 * a._duration
        lease = h.attempt("a", {"c"})               # b never sees a's term
        assert isinstance(lease, Lease), lease
        own = _Ownership(lease, (lease.root is not None, lease.root, lease.root_term), lease.root_certain, 0.0)
        with a._cond:
            a._ownership = own                      # no ownership thread: the test drives renewals
        g = b._grants
        lower = lease.term - 1
        assert g.highest_term < lower and g._granted_max < lower
        first = own.lease.valid_until - a._belief / 3    # the first renewal is due here
        with b._cond:                               # b's grant to c of a lower term, still active
            g._active, g._holder, g._inc, g._term = True, "c", c._inc, lower
            g._expiry = first + a._belief * 0.8     # past the second renewal, before a's lease ends
            g._granted_max = g._granted_top = g._highest = lower
        other_ends = g._expiry

        def renew_once():
            h.clock.t = max(h.clock.t, own.lease.valid_until - a._belief / 3)

            def pump():
                h.net.deliver(src="a", dst={"b", "c"}, kinds=("renew",))
                h.net.deliver(src=None, dst={"a"}, kinds=("grant", "reject"))
            h._run(lambda: a._renew(own), pump)

        before = a.stats()
        renew_once()                                # b busy (lower term), c grants: won
        stats = a.stats()
        assert stats["renewals"] == before["renewals"] + 1 and stats["renewal_failures"] == 0, stats
        assert not own.fenced and own.renew_after is None
        _promise_higher(h, own.term, ("c",), proposer="b")
        renew_once()                                # b busy (lower term), c stale_term: lost
        stats = a.stats()
        assert stats["renewal_failures"] == 1, stats
        assert not own.fenced, "a minority's busy refusal of a lower term fenced the owner"
        assert stats["terms_fenced"] == 0 and a._ownership is own
        assert own.renew_after is not None and own.renew_after > h.clock.t
        h.clock.t = max(own.renew_after, other_ends)  # b's grant to c ended; the back-off too
        renew_once()                                # b grants (ordinary rules), a grants: won
        stats = a.stats()
        assert stats["renewals"] == before["renewals"] + 2 and stats["renewal_failures"] == 1, stats
        assert not own.fenced and own.renew_after is None and own.lease.is_valid()
    finally:
        h.close()
