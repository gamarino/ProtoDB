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


def test_a_refused_higher_request_does_not_end_the_owners_term(tmp_path):
    with TieredCluster(tmp_path, 2) as tc:
        tc.spaces["n0"].new_database("db")
        db = tc.db("n0")
        db.run(incr, **RUN_KW)
        n0, n1 = tc["n0"], tc["n1"]
        own = n0._ownership
        owned_before = n0.stats()["ownerships"]
        assert own is not None, "n0 owns the term of its first commit"
        higher = ballot_after(own.term, n0._ranks["n1"])
        # n1 asks n0 for a higher ballot while n0's grant to itself is active: refused, busy.
        n0._on_request("n1", make_message("request", "n1", term=higher, rid=10_000, inc=n1._inc))
        deadline = time.monotonic() + n0._duration * 2.5      # several renewals are due
        commits = 1
        while time.monotonic() < deadline:
            db.run(incr, **RUN_KW)                           # an unknown outcome would raise
            commits += 1
        stats = n0.stats()
        assert stats["renewals"] >= 2, stats
        assert stats["renewal_failures"] == 0, stats
        assert stats["ownerships"] == owned_before and n0._ownership is own, "the owner's term ended while in use"
        assert tc.value("n0") == commits
