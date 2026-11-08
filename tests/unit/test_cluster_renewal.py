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
    failure; reproduced by a 0.25 s pause between commits).  A trial in which two
    consecutive commits ended more than that apart proves nothing either way and is
    repeated with a fresh refused request; a trial with steady demand must keep the term.
    The defect this guards against fails every trial: each renewal is refused."""
    with TieredCluster(tmp_path, 2) as tc:
        tc.create_database("n0")
        db = tc.db("n0")
        db.run(incr, **RUN_KW)
        commits = 1
        n0, n1 = tc["n0"], tc["n1"]
        idle = n0._duration / 4
        give_up = time.monotonic() + TRIALS_TIMEOUT
        inconclusive = []
        rid = 10_000
        while True:
            db.run(incr, **RUN_KW)
            commits += 1
            own = n0._ownership
            assert own is not None, "n0 owns the term of its own commits"
            before = n0.stats()
            higher = ballot_after(own.term, n0._ranks["n1"])
            # n1 asks n0 for a higher ballot while n0's grant to itself is active: refused, busy.
            rid += 1
            n0._on_request("n1", make_message("request", "n1", term=higher, rid=rid, inc=n1._inc, vs=0))
            deadline = time.monotonic() + n0._duration * 2.5      # several renewals are due
            last, widest = time.monotonic(), 0.0
            while time.monotonic() < deadline:
                db.run(incr, **RUN_KW)                           # an unknown outcome would raise
                commits += 1
                now = time.monotonic()
                widest, last = max(widest, now - last), now
            stats = n0.stats()
            assert stats["renewal_failures"] == before["renewal_failures"], stats
            kept = stats["ownerships"] == before["ownerships"] and n0._ownership is own
            if widest <= idle:
                assert stats["renewals"] - before["renewals"] >= 2, stats
                assert kept, f"the owner's term ended while in use (commits at most {widest:.3f} s apart)"
                break
            inconclusive.append((round(widest, 3), kept))
            assert time.monotonic() < give_up, f"no trial with steady demand: {inconclusive}"
        if inconclusive:
            print(f"\ntrials repeated for a pause over {idle:g} s between commits: {inconclusive}")
        assert tc.value("n0") == commits
