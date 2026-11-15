"""An owner keeps its term while a contender's higher request was refused (Task 8.3f, D50).

Regression for the GIL-enabled benchmark failure of 4c8c949 (2 TCP peers, ``cluster_tcp``):
a member that found no owner asked for a higher ballot; the owner's granters refused it
("busy") but raised their highest term, after which every renewal of the owner's term was
refused as a stale term.  The owner's lease ran out while a holder was replicating under it,
the replication ended without a majority, and the commit's outcome was unknown (the
benchmark's peer thread died with ``ProtoOutcomeUnknownError``; the commit had in fact been
applied, so the persisted ids no longer matched the acknowledged ones).
"""
import time

from proto_db.cluster.coordinator import ballot_after
from proto_db.cluster.protocol import make_message
from proto_db.errors import ProtoOutcomeUnknownError
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, snapshot_of, write_token
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr


#: Longest a trial may be repeated for want of steady demand (a liveness detector, R82).
TRIALS_TIMEOUT = 60.0


def test_a_refused_higher_request_does_not_end_the_owners_term(tmp_path):
    """While n0 owns the term and commits continuously, n1's higher request is refused; every
    renewal of n0's term must still succeed and the term must not end.

    An owner also ends its term when nothing asked for a permission for ``lease_duration / 4``
    (D50, "idle", 0.225 s here), which is not the property under test: a pause that long
    between two commits of this one thread (a garbage collection, the scheduler) ended the
    term in 1 run of the fix round 2 proof (Task 8.3n, ownerships 3 == 2 with no renewal
    failure; reproduced by a 0.25 s pause between commits).  The owner counts that time from
    its last permission *request* (``last_demand``), so the gap measured here runs from n0's
    last request (timed by the test at the entry of n0's request path, never read from the
    term's state) to the end of the next commit: the time between two commit ends
    misses the part of a slow commit that follows its request (Task CI-FLAKES: a CI runner
    ended the term with commit ends at most 0.218 s apart; reproduced by a 0.12 s stall after
    one commit returned and another before the next one's request: commit ends at most 0.166 s
    apart, those two requests at least 0.24 s apart).  A stall inside a commit, longer
    than what its permission had left, ends that commit unknown although a majority
    acknowledged it (the committing thread observed the acknowledgements after its
    permission ended: re-review 4 M2, P5), with every renewal granted: 1 run of the
    restarted proof; reproduced by a 1 s stall in the commit's own acceptance.  A trial with
    either (or a renewal that timed out under the same stall) proves nothing either way and is
    repeated with a fresh refused request; a trial with steady demand, no unknown outcome and
    no failed renewal must keep the term.

    The defect this guards against is observed directly, in every trial: a granter refusing
    a renewal of n0's term while its grant to n0 is still active (the 4c8c949 failure: the
    refused higher request made every renewal stale).  A stall cannot produce that: a
    renewal reaching a granter whose grant lapsed during the stall may be refused, but then
    the grant was no longer active."""
    with TieredCluster(tmp_path, 2) as tc:
        tc.create_database("n0")
        db = tc.db("n0")
        # Every commit writes a token of its own (re-review 1, N1): the outcome of each is
        # checked on both members at the end, with the counter read in the same snapshot.
        tokens = CommitTokens()

        def setup_commit():
            token = tokens.new("setup")
            tc.commit("n0", lambda tr: (incr(tr), write_token(tr, token)))     # exactly once
            tokens.acked(token)

        def load_commit():
            token = tokens.new("n0")
            try:
                db.run(lambda tr: (incr(tr), write_token(tr, token)), **RUN_KW)
            except ProtoOutcomeUnknownError:
                tokens.unknown(token)                   # resolved by its token at the end
                raise
            tokens.acked(token)

        n0, n1 = tc["n0"], tc["n1"]
        # n0's permission requests, timed at the entry of its request path, independently of any
        # ownership object (fix round 1, review I2: ``own.last_demand`` stops moving when the
        # term ends, so a term that ended early looked like an idle gap and was retried).
        requests = [time.monotonic()]
        ask = n0._permission_from_owner

        def timed_ask(deadline):
            requests.append(time.monotonic())
            return ask(deadline)
        n0._permission_from_owner = timed_ask
        setup_commit()
        idle = n0._duration / 4
        refused = []                    # renewals of n0 refused under an active grant to n0

        def watch(node):
            grants = node._grants
            decide = grants.on_request

            def on_request(holder, term, inc=0, *, renew=False):
                active = grants._active and grants._holder == holder and grants._term == term \
                    and grants._inc == inc and grants._clock() < grants._expiry
                decision = decide(holder, term, inc, renew=renew)
                if renew and holder == "n0" and active and not decision.granted:
                    refused.append((node.node_id, decision.reason, term))
                return decision
            grants.on_request = on_request
        for node in (n0, n1):
            watch(node)
        give_up = time.monotonic() + TRIALS_TIMEOUT
        inconclusive = []
        rid = 10_000
        while True:
            setup_commit()                                      # n0 owns a term
            own = n0._ownership
            assert own is not None, "n0 owns the term of its own commits"
            before = n0.stats()
            higher = ballot_after(own.term, n0._ranks["n1"])
            # n1 asks n0 for a higher ballot while n0's grant to itself is active: refused, busy.
            rid += 1
            n0._on_request("n1", make_message("request", "n1", term=higher, rid=rid, inc=n1._inc, vs=0))
            deadline = time.monotonic() + n0._duration * 2.5      # several renewals are due
            widest, unknowns = 0.0, []
            # Every gap is measured from n0's last permission request (``requests``, timed at
            # entry, so no later than the owner's ``last_demand`` of the same request) to the end
            # of the next commit: an idle end needs more than ``idle`` from one request to the
            # next, and the next request comes before the end of the commit that makes it.  The
            # term's own state never enters this measure: a term that ends early is not idle.
            demand = requests[-1]
            while time.monotonic() < deadline:
                started = time.monotonic()
                try:
                    load_commit()
                except ProtoOutcomeUnknownError as e:
                    took = time.monotonic() - started
                    # Observed, not inferred: an owner issues a permission only with at least
                    # belief / 4 of its lease left (D50's D/4 rule), so a commit that ended
                    # unknown sooner than that was no stall -- a possible product defect.
                    assert took >= n0._belief / 4, f"an unknown outcome after only {took:.3f} s: {e}"
                    unknowns.append((round(took, 3), str(e)))
                widest = max(widest, time.monotonic() - demand)
                demand = requests[-1]
            stats = n0.stats()
            assert not refused, f"a renewal of the owner's term was refused under an active grant: {refused}"
            failed = stats["renewal_failures"] - before["renewal_failures"]
            kept = stats["ownerships"] == before["ownerships"] and n0._ownership is own
            if widest <= idle and not unknowns and not failed:
                assert stats["renewals"] - before["renewals"] >= 2, stats
                assert kept, (f"the owner's term ended while in use (at most {widest:.3f} s from a "
                              f"permission request to the end of the next commit)")
                break
            inconclusive.append((round(widest, 3), kept, failed, unknowns))
            assert time.monotonic() < give_up, f"no trial with steady demand: {inconclusive}"
        if inconclusive:
            print(f"\ntrials repeated (over {idle:g} s from a permission request to the end of the next commit, "
                  f"a failed renewal or an unknown outcome): "
                  f"{inconclusive}")
        # Every outcome resolved on both members, the counter read in the same snapshot as the
        # tokens (re-review 1, N1): a lost acknowledged commit, an unknown one resolved
        # differently on n1, or a counter that disagrees with the tokens fails here.
        res = assert_outcomes_resolved(
            {n: (lambda n=n: snapshot_of(tc.db(n), lambda tr: tr.get_root_object("n"))) for n in ("n0", "n1")},
            tokens)
        assert all(v == len(res.present) for v in res.values.values()), \
            f"the counter disagrees with the tokens: {res.values}, {len(res.present)} tokens present"


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
