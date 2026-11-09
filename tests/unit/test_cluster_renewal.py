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
    failure; reproduced by a 0.25 s pause between commits).  A stall inside a commit, longer
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
        tc.commit("n0", incr)                                   # exactly once
        commits, unknown = 1, 0
        n0, n1 = tc["n0"], tc["n1"]
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
            tc.commit("n0", incr)                               # exactly once: n0 owns a term
            commits += 1
            last = time.monotonic()                             # every gap from here is measured
            own = n0._ownership
            assert own is not None, "n0 owns the term of its own commits"
            before = n0.stats()
            higher = ballot_after(own.term, n0._ranks["n1"])
            # n1 asks n0 for a higher ballot while n0's grant to itself is active: refused, busy.
            rid += 1
            n0._on_request("n1", make_message("request", "n1", term=higher, rid=rid, inc=n1._inc, vs=0))
            deadline = time.monotonic() + n0._duration * 2.5      # several renewals are due
            widest, unknowns = 0.0, []
            while time.monotonic() < deadline:
                started = time.monotonic()
                try:
                    db.run(incr, **RUN_KW)
                    commits += 1
                except ProtoOutcomeUnknownError as e:
                    unknown += 1                                # resolved by the bound below
                    took = time.monotonic() - started
                    # Observed, not inferred: an owner issues a permission only with at least
                    # belief / 4 of its lease left (D50's D/4 rule), so a commit that ended
                    # unknown sooner than that was no stall -- a possible product defect.
                    assert took >= n0._belief / 4, f"an unknown outcome after only {took:.3f} s: {e}"
                    unknowns.append((round(took, 3), str(e)))
                now = time.monotonic()
                widest, last = max(widest, now - last), now
            stats = n0.stats()
            assert not refused, f"a renewal of the owner's term was refused under an active grant: {refused}"
            failed = stats["renewal_failures"] - before["renewal_failures"]
            kept = stats["ownerships"] == before["ownerships"] and n0._ownership is own
            if widest <= idle and not unknowns and not failed:
                assert stats["renewals"] - before["renewals"] >= 2, stats
                assert kept, f"the owner's term ended while in use (commits at most {widest:.3f} s apart)"
                break
            inconclusive.append((round(widest, 3), kept, failed, unknowns))
            assert time.monotonic() < give_up, f"no trial with steady demand: {inconclusive}"
        if inconclusive:
            print(f"\ntrials repeated (a pause over {idle:g} s between commits, a failed renewal or an unknown "
                  f"outcome): "
                  f"{inconclusive}")
        assert commits <= tc.value("n0") <= commits + unknown, (tc.value("n0"), commits, unknown)


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
