"""An owner keeps its term while a contender's higher request was refused (Task 8.3f, D50).

Regression for the GIL-enabled benchmark failure of 4c8c949 (2 TCP peers, ``cluster_tcp``):
a member that found no owner asked for a higher ballot; the owner's granters refused it
("busy") but raised their highest term, after which every renewal of the owner's term was
refused as a stale term.  The owner's lease ran out while a holder was replicating under it,
the replication ended without a majority, and the commit's outcome was unknown (the
benchmark's peer thread died with ``ProtoOutcomeUnknownError``; the commit had in fact been
applied, so the persisted ids no longer matched the acknowledged ones).

Every test here runs on a fake clock with messages delivered by hand.  The end-to-end run on
real time (two TieredCluster members, a load thread committing continuously) is the probe
``benchmarks/cluster_renewal.py`` (final re-review): an owner's idle end is measured on the
monotonic clock by design (D50), so a stall of the load thread could end the term there, and
the suite holds no timing-dependent test.
"""
import time


def test_renewals_after_a_refused_higher_request_keep_the_term_deterministically():
    """The property of the test above with no timing (Task 8.3n fix round 2): real
    coordinators over a :class:`~tests.support.manual_cluster.ManualNet` and one fake clock,
    every message delivered by hand.  a owns its term; b's real acquire for a higher ballot
    is refused "busy" by a and by b's own grant table (both raise their highest term, as in
    4c8c949); then a renews its term three times, the clock advanced past each lease, and
    every renewal must be granted by both members and the term kept.  Under the 4c8c949
    defect the first renewal is refused as a stale term."""
    from proto_db.cluster.coordinator import _Ownership
    from proto_db.cluster.lease import Lease
    from tests.support.manual_cluster import D, Harness

    h = Harness("ab")
    try:
        a, b = h.nodes["a"], h.nodes["b"]
        lease = h.attempt("a", {"b"})
        assert isinstance(lease, Lease), lease
        own = _Ownership(lease, (lease.root is not None, lease.root, lease.root_term), lease.root_certain, 0.0)
        with a._cond:
            a._ownership = own                   # no ownership thread: the test drives renewals
        refused = h.attempt("b", {"a"}, timeout=0.5)
        assert not isinstance(refused, Lease), "b's higher ballot must be refused while a holds its term"
        assert b._grants.highest_term > lease.term and a._grants.highest_term > lease.term
        renewals = a.stats()["renewals"]
        first_expiry = own.lease.valid_until
        for i in range(3):
            h.clock.t = own.lease.valid_until - 0.25 * (D - h.nodes["a"]._margin)   # renewal due
            expiry = own.lease.valid_until

            def pump():
                h.net.deliver(src="a", dst={"b"}, kinds=("renew",))
                h.net.deliver(src="b", dst={"a"}, kinds=("grant", "reject"))
            h._run(lambda: a._renew(own), pump)
            stats = a.stats()
            assert stats["renewal_failures"] == 0, (i, stats)
            assert stats["renewals"] == renewals + i + 1, (i, stats)
            assert own.lease.valid_until > expiry, (i, own.lease.valid_until, expiry)
            assert a._ownership is own
        h.clock.t = first_expiry + 1.0               # past the lease of the acquire: the term lives on
        assert own.lease.is_valid(), (own.lease, first_expiry)
    finally:
        h.close()


def test_the_ownership_thread_keeps_its_term_after_a_refused_higher_request():
    """The property of the real-time end-to-end probe (``benchmarks/cluster_renewal.py``) with no
    timing: a's real ownership thread over a :class:`~tests.support.manual_cluster.ManualNet`
    and one fake clock.  b's higher request is refused "busy" (both grant tables raise their
    highest term, as in 4c8c949); then, with one of a's own permissions outstanding (the holder
    replicating under it), the fake clock reaches each renewal point and the thread renews: every
    renewal is granted and the term is kept.

    Deterministic by construction: an outstanding permission is demand, and it keeps the term from
    an idle end (D50 measures idle on the monotonic clock, only with an empty pipeline); its
    deadline (``valid + request_timeout`` past its issue, on the fake clock) is after both renewal
    points, so it is never abandoned as late.  Under the 4c8c949 defect (the stale-term rule before
    the renewal of an active grant) b refuses the first renewal.  The waits are liveness bounds,
    never asserted durations."""
    import threading as _threading
    from proto_db.cluster.lease import Lease
    from tests.support.manual_cluster import Harness

    h = Harness("ab")
    a, b = h.nodes["a"], h.nodes["b"]
    stop = _threading.Event()

    def pump_forever():
        while not stop.is_set():
            h.net.deliver(src="a", dst={"b"}, kinds=("renew", "request", "release"))
            h.net.deliver(src="b", dst={"a"}, kinds=("grant", "reject"))
            time.sleep(0.001)
    pumper = _threading.Thread(target=pump_forever, daemon=True)

    def wait_for(cond, what, timeout=10.0):
        deadline = time.monotonic() + timeout          # a liveness bound (R82), never a measure
        while not cond():
            assert time.monotonic() < deadline, f"timed out waiting for {what}: {a.stats()}"
            time.sleep(0.002)

    try:
        h.own("a")
        own = a._ownership
        assert own is not None
        pumper.start()
        refused = h.attempt("b", {"a"}, timeout=0.5)
        assert not isinstance(refused, Lease), "b's higher ballot must be refused while a holds its term"
        assert b._grants.highest_term > own.term and a._grants.highest_term > own.term
        before = a.stats()
        permission = a.permit(timeout=5)            # demand: the holder works under it
        issued = own.pipeline[-1]
        for i in range(2):
            expiry = own.lease.valid_until
            h.clock.t = expiry - a._belief / 2 + 0.001       # a renewal is due on the fake clock
            assert h.clock.t < issued.deadline, "the permission must stay outstanding, not late"
            with a._cond:
                a._cond.notify_all()
            wait_for(lambda: a.stats()["renewals"] > before["renewals"] + i or own.ended
                     or a.stats()["renewal_failures"] > before["renewal_failures"], f"renewal {i + 1}")
            assert a._ownership is own and not own.ended, f"the term ended while in use (renewal {i + 1})"
            stats = a.stats()
            assert stats["renewal_failures"] == before["renewal_failures"], (i, stats)
            assert own.lease.valid_until > expiry, (i, own.lease.valid_until, expiry)
        assert a._ownership is own and not own.ended
        assert a.stats()["terms_abandoned"] == before["terms_abandoned"]
        permission.outcome = "none"
        a.finish(permission)
    finally:
        stop.set()
        if pumper.is_alive():
            pumper.join(5)
        h.close()


def test_a_term_ends_by_idle_or_by_expiry_only_while_its_renewals_succeed():
    """Re-review 1, N6: the ownership thread itself (not ``_renew`` driven by hand), over a
    :class:`~tests.support.manual_cluster.ManualNet` with one fake clock for every lease.

    1. While a's own threads keep asking for permissions, every renewal falls due on the
       fake clock and is granted by b and c: the term is kept for five renewals.
    2. Demand stops: after ``lease_duration / 4`` without a request (D50 "idle", measured on
       the monotonic clock) the term ends, released, never abandoned, no renewal failed.
    3. a owns a new term with demand, and its renewals are lost: when the fake clock passes
       the lease the term ends as expired; it was never kept past its lease."""
    import threading as _threading
    from tests.support.manual_cluster import D, Harness

    h = Harness("abc")
    a = h.nodes["a"]
    deliver_renewals = _threading.Event()
    deliver_renewals.set()
    stop = _threading.Event()

    def pump_forever():
        while not stop.is_set():
            if deliver_renewals.is_set():
                h.net.deliver(src="a", dst={"b", "c"}, kinds=("renew", "request", "release"))
                h.net.deliver(src=None, dst={"a"}, kinds=("grant", "reject"))
            else:
                h.net.drop(src="a", kinds=("renew",))
                h.net.drop(src=None, dst={"a"}, kinds=("grant", "reject"))
            time.sleep(0.001)
    pumper = _threading.Thread(target=pump_forever, daemon=True)
    pumper.start()

    def wait_for(cond, what, timeout=10.0):
        deadline = time.monotonic() + timeout
        while not cond():
            assert time.monotonic() < deadline, f"timed out waiting for {what}: {a.stats()}"
            time.sleep(0.002)

    def demand():
        """One local permission, ended without a root (O5): a's own threads asking."""
        p = a.permit(timeout=5)
        p.outcome = "none"
        a.finish(p)

    try:
        h.own("a")
        own = a._ownership
        assert own is not None
        belief = a._belief
        before = a.stats()
        for i in range(5):
            demand()
            expiry = own.lease.valid_until
            h.clock.t = expiry - belief / 2 + 0.001        # a renewal is due on the fake clock
            with a._cond:
                a._cond.notify_all()
            wait_for(lambda: a.stats()["renewals"] > before["renewals"] + i, f"renewal {i + 1}")
            assert own.lease.valid_until > expiry
            assert a._ownership is own, f"the term ended while in use (renewal {i + 1})"
        assert a.stats()["renewal_failures"] == before["renewal_failures"]
        # 2. No demand: the term ends by idle, released and never abandoned.
        # The ownership is cleared first, then the lease released (outside the lock).
        wait_for(lambda: a._ownership is None and own.lease.released_at is not None, "the idle end of the term",
                 timeout=D)
        ended = time.monotonic()
        # Idle runs from the last request (``last_demand``, the monotonic clock), not from the
        # end of the renewal loop above (re-review 2 of 8.3n, M4): measured from there, with no
        # tolerance needed (the same clock, read after the decision).
        assert ended - own.last_demand >= D / 4, (ended - own.last_demand, D / 4)
        assert own.ended and not own.abandoned and own.lease.is_valid() is False
        stats = a.stats()
        assert stats["terms_abandoned"] == 0 and stats["renewal_failures"] == before["renewal_failures"]
        # 3. A new term with demand; its renewals are lost; the fake clock passes its lease.
        demand()
        own2 = a._ownership
        assert own2 is not None and own2 is not own and own2.term > own.term
        deliver_renewals.clear()
        demand()
        h.clock.t = own2.lease.valid_until + 0.001
        with a._cond:
            a._cond.notify_all()
        wait_for(lambda: a._ownership is None and own2.lease.released_at is not None, "the expiry of the second term")
        assert own2.ended and not own2.lease.is_valid()
        assert a.stats()["terms_abandoned"] == 0       # nothing outstanding when it expired
    finally:
        stop.set()
        pumper.join(5)
        h.close()
