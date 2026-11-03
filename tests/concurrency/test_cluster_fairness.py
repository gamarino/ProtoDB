"""Lease fairness under contention (Task 8.3e, defect D-LIVE-1, D46).

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
* each size runs ``_TRIALS`` times; the median trial must meet both bounds, and every trial
  must give every member at least one lease (a starved member gets none in a trial).
"""
import statistics
import threading
import time

import pytest

from tests.support.cluster import Cluster

pytestmark = [pytest.mark.timeout(240)]

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


@pytest.mark.parametrize("n,delay,threads", [(2, 0.0, None), (3, 0.0, None), (3, 0.0005, None), (5, 0.0, None),
                                             (3, 0.0, (1, 4, 4))])
def test_every_contending_member_gets_the_lease_regularly(n, delay, threads, capsys):
    """``threads=(1, 4, 4)``: a member with one thread releases without another thread waiting
    (``more`` is False), so nobody queues it when its thread asks again: it must say ``want``
    before deferring, or no release would ever name it."""
    trials = [_trial(n, seed, delay, threads) for seed in range(_TRIALS)]
    with capsys.disabled():
        print(f"\nFAIRNESS {n} members x {threads or _THREADS} threads, hold {_HOLD * 1e3:g} ms, link delay "
              f"{delay * 1e3:g} ms: " + "; ".join(
                  f"leases {t['counts']} ratio {t['ratio']:.2f} max gap {t['max_gap'] * 1e3:.0f} ms "
                  f"turns {t['turns']} timeouts {t['timeouts']}" for t in trials))
    assert all(min(t["counts"]) > 0 for t in trials), trials                  # nobody starved outright
    assert statistics.median(t["max_gap"] for t in trials) <= _MAX_GAP, trials
    assert statistics.median(t["ratio"] for t in trials) >= _MIN_RATIO, trials
