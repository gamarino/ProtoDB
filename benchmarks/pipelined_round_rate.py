"""Pipelined permits: round rate against the serial base (Task 8.3n, moved from the unit suite).

Runs the measurement of ``tests/concurrency/test_cluster_pipelined_permits.py``
(``_rounds_per_second``: three tiered members, two threads each, records-bearing acceptor
writes slowed to 20 ms) with ``DEPTH = 0`` (serial: no permit while one is outstanding) and
``DEPTH = 1`` (pipelined), alternating, and prints both rates and their ratio per run.  Each run
verifies its work before it reports a figure: every commit outcome is resolved by a token on
every member and each thread's counter equals its tokens present (an assertion error exits 1).

The ratio is a throughput figure of this machine under its current load, not a correctness
property, so it is not asserted in the unit suite; ``--min-ratio`` makes this script fail
(exit 1) when the median ratio is below it.

Usage, from the repository root::

    .venv314t/bin/python -X gil=1 benchmarks/pipelined_round_rate.py --runs 20 --dir <scratch dir>

``--dir`` must be on a real disk (the members fsync): never a tmpfs such as ``/tmp``.
"""
import argparse
import json
import os
import pathlib
import shutil
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

from tests.concurrency import test_cluster_pipelined_permits as pipelined  # noqa: E402


def _sample(base: pathlib.Path, depth: int) -> dict:
    mp = pytest.MonkeyPatch()
    try:
        return pipelined._rounds_per_second(base, mp, depth)
    finally:
        mp.undo()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--dir", required=True, help="scratch directory on a real disk")
    ap.add_argument("--min-ratio", type=float, default=None)
    ap.add_argument("--json", default=None, help="write the samples to this file")
    args = ap.parse_args(argv)
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    samples = []
    for i in range(args.runs):
        base = pathlib.Path(args.dir) / f"run-{i}"
        shutil.rmtree(base, ignore_errors=True)
        base.mkdir(parents=True)
        try:
            serial = _sample(base, 0)
            piped = _sample(base, 1)
        finally:
            shutil.rmtree(base, ignore_errors=True)
        ratio = piped["rate"] / serial["rate"]
        samples.append({"serial": serial, "pipelined": piped, "ratio": ratio})
        print(f"run {i + 1}/{args.runs}: serial {serial['rate']:.1f} rounds/s ({serial['rounds']} rounds, "
              f"{serial['acked']} acked, {serial['unknown']} unknown); pipelined {piped['rate']:.1f} "
              f"({piped['rounds']} rounds, {piped['acked']} acked, {piped['unknown']} unknown, "
              f"{piped['pipelined']:.0%} on a proposal); ratio {ratio:.2f}", flush=True)
    ratios = sorted(s["ratio"] for s in samples)
    median = statistics.median(ratios)
    print(f"GIL {'on' if gil else 'off'}: ratio median {median:.2f}, min {ratios[0]:.2f}, max {ratios[-1]:.2f} "
          f"over {len(ratios)} runs")
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"gil": gil, "samples": samples}, f, indent=1)
    if args.min_ratio is not None and median < args.min_ratio:
        print(f"FAIL: median ratio {median:.2f} below {args.min_ratio}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
