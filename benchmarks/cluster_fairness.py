"""Lease fairness under contention in real time (Task 8.3e, defect D-LIVE-1, D46): a probe.

Moved here from ``tests/concurrency/test_cluster_fairness.py`` (final review F2): it measures
wall-clock gaps and lease ratios, and the mechanism's deferral bounds are real-time by design
(``_TURN_SILENCE``, one lease duration, on ``time.monotonic``), so no run of it can be
deterministic, and the test suite holds no timing-dependent test.  What it proved as a test --
no contending member starves, service is round robin -- is now proved on a logical clock (the
grant count) by ``tests/unit/test_cluster_fairness.py::
test_contending_members_are_served_in_turn_no_member_starves``; this probe keeps the real-time
measurement.

Every member runs several threads that loop ``acquire`` -> hold for a fixed critical section ->
``release`` for a few seconds.  Before Task 8.3e the process that released the lease re-asked
from its next queued thread within a fraction of a millisecond, long before a contender woken
by the release could, so one member held the lease almost always (measured with TCP peers in
``fsync`` mode: 178 commits against 4 in 3 s).  Releases now name the member seen waiting
longest and every other member, the releaser included, defers to it.

Bounds, with margin (the fair schedule is round-robin: each member waits about ``N - 1``
critical sections between two of its leases):

* ``_MAX_GAP``: no member waits longer than this between two of its leases (or before its
  first) -- a starved member waits the whole run (the bounds' data is beside them);
* ``_MIN_RATIO``: the fewest leases of a member over the most of any member;
* each case runs ``_TRIALS`` times; the median trial must meet both bounds, and every trial
  must give every member at least one lease (a starved member gets none in a trial).

Self-reporting (benchmarks/README.md): ``python -m benchmarks.cluster_fairness [--json OUT]``
prints every trial's leases, ratio, maximum gap, turns and deferral timeouts, then each case's
verdict; the exit status is 1 when a case misses a bound.
"""
import argparse
import json
import statistics
import sys
import threading
import time

from tests.support.cluster import Cluster

_HOLD = 0.01            # critical section, seconds (the fsync-mode TCP figure is 7-29 ms)
_THREADS = 4
_SECONDS = 1.5
_TRIALS = 3
#: Data behind the bounds (8.3e review M4).  Fair at rest: median-trial gaps 22-57 ms and
#: ratios 0.94-1.00; the review's 20 runs (10 per GIL mode) gave ratios 0.64-1.00 and a median
#: 5-member gap of about 68 ms over 48 trials, but single trials under light concurrent load
#: reached 491 and 518 ms, within 9 ms of the former 0.5 s bound.  Base ``ba3287d``
#: (median trial per case): gaps 1500, 1391, 421, 793 ms and ratios 0.03, 0.04, 0.32, 0.13
#: (2, 3, 3 with 0.5 ms delay, 5 members).  A 1.0 s gap bound keeps every base case failing --
#: the 2- and 3-member cases on the gap, the delayed and 5-member cases on the ratio -- with a
#: margin of about 2x over the worst loaded fair trial.
_MAX_GAP = 1.0          # fair: about (N - 1) x (hold + hand-off), under 0.1 s for N <= 5 at rest
_MIN_RATIO = 0.5        # fair: about 1.0; starved before the fix: 0.03 to 0.32 (median trials)
KW = dict(lease_duration=2.0, request_timeout=0.25, startup_grace=0.0,
          backoff=(0.001, 0.02))


def _trial(n: int, seed: int, delay: float, threads=None) -> dict:
    threads = threads or (_THREADS,) * n
    cl = Cluster(n, seed=seed, tiers=True, **KW)
    try:
        if delay:
            cl.net.set_faults(delay=(delay, delay))
        wins = {node: [] for node in cl.ids}
        lock = threading.Lock()
        start = threading.Barrier(sum(threads) + 1)
        stop_at = []

        def worker(node):
            coordinator = cl[node]
            start.wait()
            while time.monotonic() < stop_at[0]:
                lease = coordinator.acquire(timeout=10.0)
                try:
                    with lock:
                        wins[node].append(time.monotonic())
                    time.sleep(_HOLD)
                finally:
                    coordinator.release(lease)

        workers = [threading.Thread(target=worker, args=(node,))
                   for node, k in zip(cl.ids, threads) for _ in range(k)]
        for t in workers:
            t.start()
        began = time.monotonic()
        stop_at.append(began + _SECONDS)
        start.wait()
        for t in workers:
            t.join()
        counts = [len(wins[node]) for node in cl.ids]
        gaps = []
        for node in cl.ids:
            times = [began] + [t for t in wins[node] if t < stop_at[0]] + [stop_at[0]]
            gaps.append(max(b - a for a, b in zip(times, times[1:])))
        stats = {node: cl[node].stats() for node in cl.ids}
        return {"counts": counts, "ratio": min(counts) / max(counts) if max(counts) else 0.0,
                "max_gap": max(gaps), "turns": sum(s.get("turns_taken", 0) for s in stats.values()),
                "timeouts": sum(s.get("deferral_timeouts", 0) for s in stats.values())}
    finally:
        cl.close()


#: The cases: (members, link delay in seconds, threads per member or None for _THREADS each).
#: ``(1, 4, 4)``: a member with one thread releases without another thread waiting (``more`` is
#: False), so nobody queues it when its thread asks again: it must say ``want`` before deferring,
#: or no release would ever name it.
CASES = [(2, 0.0, None), (3, 0.0, None), (3, 0.0005, None), (5, 0.0, None), (3, 0.0, (1, 4, 4))]


def run_case(n: int, delay: float, threads) -> dict:
    """Run one case's trials; return the trials and whether the case met every bound."""
    trials = [_trial(n, seed, delay, threads) for seed in range(_TRIALS)]
    starved = not all(min(t["counts"]) > 0 for t in trials)
    gap = statistics.median(t["max_gap"] for t in trials)
    ratio = statistics.median(t["ratio"] for t in trials)
    return {"members": n, "delay": delay, "threads": list(threads or (_THREADS,) * n), "trials": trials,
            "starved": starved, "median_max_gap": gap, "median_ratio": ratio,
            "ok": not starved and gap <= _MAX_GAP and ratio >= _MIN_RATIO}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", help="write every case's figures to this file")
    args = parser.parse_args(argv)
    results = []
    for n, delay, threads in CASES:
        case = run_case(n, delay, threads)
        results.append(case)
        print(f"FAIRNESS {n} members x {threads or _THREADS} threads, hold {_HOLD * 1e3:g} ms, link delay "
              f"{delay * 1e3:g} ms: " + "; ".join(
                  f"leases {t['counts']} ratio {t['ratio']:.2f} max gap {t['max_gap'] * 1e3:.0f} ms "
                  f"turns {t['turns']} timeouts {t['timeouts']}" for t in case["trials"])
              + f" -> median gap {case['median_max_gap'] * 1e3:.0f} ms (bound {_MAX_GAP * 1e3:g}), median ratio "
                f"{case['median_ratio']:.2f} (bound {_MIN_RATIO}), starved {case['starved']}: "
                f"{'ok' if case['ok'] else 'FAILED'}", flush=True)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=1)
    return 0 if all(case["ok"] for case in results) else 1


if __name__ == "__main__":
    sys.exit(main())
