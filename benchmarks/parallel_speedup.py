"""Informational speedup of proto_db.runtime.parallel.parallel_scan for a CPU-bound function (Task 8.1, D40).

Run with the free-threaded interpreter and again with the GIL enabled::

    .venv314t/bin/python benchmarks/parallel_speedup.py
    .venv314t/bin/python -X gil=1 benchmarks/parallel_speedup.py

The script prints the work done and verifies every computed result against a
sequential run, so a silent failure cannot pass for a speedup.  Nothing is asserted
about the speed itself.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db.runtime.parallel import ParallelPool  # noqa: E402

ITEMS = 2_000
SPIN = 4_000


def work(x: int) -> int:
    """A CPU-bound pure-Python function: an integer hash loop."""
    h = x
    for i in range(SPIN):
        h = (h * 1_000_003 + i) & 0xFFFFFFFF
    return h


def best_of(runs: int, fn) -> float:
    """The best wall time of ``runs`` calls of ``fn``."""
    best = float("inf")
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def main() -> int:
    gil = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    items = list(range(ITEMS))
    expected = [work(x) for x in items]
    sequential = best_of(3, lambda: [work(x) for x in items])
    print(f"python {sys.version.split()[0]} gil={'on' if gil else 'off'} cpus={os.cpu_count()} "
          f"items={ITEMS} spin={SPIN}")
    print(f"sequential: {sequential * 1000:.1f} ms")
    for workers in (1, 2, 4, 8):
        with ParallelPool(workers) as pool:
            got = pool.scan(items, work)
            if got != expected:
                print(f"workers={workers}: WRONG RESULT")
                return 1
            elapsed = best_of(3, lambda: pool.scan(items, work))
        print(f"workers={workers}: {elapsed * 1000:.1f} ms  speedup x{sequential / elapsed:.2f}  "
              f"(verified {len(got)} results)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
