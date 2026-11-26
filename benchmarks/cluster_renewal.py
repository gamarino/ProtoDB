"""An owner keeps its term while a contender's higher request was refused, in real time (Task 8.3f,
D50): a probe.

Moved here from ``tests/unit/test_cluster_renewal.py`` (final re-review: a wall-clock flake of the
full suite on 3.12, "the owner's term ended while in use (at most 0.216 s ...)").  It runs two
real TieredCluster members on real time: an owner ends its term after ``lease_duration / 4``
without a permission request (D50 "idle", on ``time.monotonic``, by design), so a scheduler stall
between two commits of the load thread ends the term in a way no test can rule out, and the test
suite holds no timing-dependent test.  The property -- a refused higher request never makes a
renewal of the owner's term fail -- is proved with no timing by
``tests/unit/test_cluster_renewal.py::test_renewals_after_a_refused_higher_request_keep_the_term_deterministically``
(renewals driven by hand) and
``::test_the_ownership_thread_keeps_its_term_after_a_refused_higher_request`` (the real ownership
thread on a fake clock, demand held by an outstanding permission); this probe keeps the
end-to-end run, with every commit's outcome resolved on both members by its token.

The regression it guards (4c8c949, 2 TCP peers, ``cluster_tcp``): a member that found no owner
asked for a higher ballot; the owner's granters refused it ("busy") but raised their highest
term, after which every renewal of the owner's term was refused as a stale term, the lease ran
out under a replicating holder and the commit's outcome was unknown.

Self-reporting (benchmarks/README.md): ``python -m benchmarks.cluster_renewal [--runs N] [--json OUT]``
prints, per run, the renewals made, the widest gap from a permission request to the end of the
next commit, the trials repeated for want of steady demand and the commits resolved, then the
verdict; the exit status is 1 when a run fails (a renewal refused under an active grant, the term
ended with steady demand, or an outcome unresolved).
"""
import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

from proto_db.cluster.coordinator import ballot_after
from proto_db.cluster.protocol import make_message
from proto_db.errors import ProtoOutcomeUnknownError
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, snapshot_of, write_token
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr


#: Longest a run may repeat its trial for want of steady demand (a liveness detector, R82).
TRIALS_TIMEOUT = 60.0


def run_probe(tmp_path) -> dict:
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
        return {"renewals": stats["renewals"] - before["renewals"], "widest_gap_s": round(widest, 3),
                "idle_s": idle, "trials_repeated": inconclusive, "commits_present": len(res.present)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runs", type=int, default=3, help="independent runs (default 3)")
    parser.add_argument("--json", help="write every run's figures to this file")
    args = parser.parse_args(argv)
    results = []
    for run in range(args.runs):
        with tempfile.TemporaryDirectory(prefix="proto_db-renewal-") as tmp:
            try:
                figures = run_probe(Path(tmp))
                figures["ok"] = True
            except AssertionError as e:
                figures = {"ok": False, "failure": str(e)}
        results.append(figures)
        print(f"RENEWAL run {run + 1}: " + ("; ".join(f"{k} {v}" for k, v in figures.items() if k != "ok"))
              + f" -> {'ok' if figures['ok'] else 'FAILED'}", flush=True)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=1)
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
