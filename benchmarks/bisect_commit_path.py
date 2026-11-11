"""Interleaved paired bisection of the cluster commit path over clean exports (Task R136).

Usage::

    python -m benchmarks.bisect_commit_path --rounds N --json OUT --workdir DIR EXPORT [EXPORT ...]

Each ``EXPORT`` is a directory written by :mod:`benchmarks.export` (a ``git archive`` of one
commit).  Every run is one fresh child interpreter started with the export as its working
directory, so ``proto_db``, ``benchmarks.cluster_peer`` and ``run_tcp_cluster`` all come from
that export (the cluster benchmark code is unchanged from 4aab0f2 to this commit except the
reader figures, which these runs do not use).  The child runs one cell of ``cluster_tcp``
(:func:`benchmarks.workloads.cluster.run_tcp_cluster`) and prints its figures as one JSON line.

Cells (``--cells``): ``blind`` (3 TCP peers x 4 threads, fsync, a blind add of one id) and
``p100`` (the same plus one 100 B value per commit, the ``payload 100 B`` cell).  One round runs
every cell at every export once; the export order alternates direction each round (A B C, C B A,
...) so that a drift of the machine over the session falls on every export alike, and an
export's figure is always compared with its neighbours run minutes apart.  A run whose persisted
state does not hold exactly the acknowledged commits is recorded as failed and never averaged.

The machine is shared: before each run the driver waits until other processes keep fewer than
``--quiet-below`` busy CPUs (measured over 1 s from ``/proc/stat`` while it runs nothing), and it
records, per run, the CPU other processes used during it (machine busy time minus this run's
process tree, from ``RUSAGE_CHILDREN``); a run at or above ``--foreign-max`` busy CPUs is kept in
the JSON with ``discarded`` and repeated.

The JSON holds every run (export, commit, cell, round, start time, rate, latency, critical
section, syncs and frame bytes per commit) and, per cell, each export's median rate and the
paired ratios of consecutive exports (per round, then their median).
"""
import argparse
import json
import resource
import os
import statistics
import subprocess
import sys
import time

CELLS = {"blind": {"payload": 0}, "p100": {"payload": 100}}

CHILD = r"""
import json, os, sys
sys.path.insert(0, os.getcwd())
from benchmarks.harness import Context
from benchmarks.workloads.cluster import run_tcp_cluster
ctx = Context(quick=False, threads=[4], repeat=1, warmup=0, workdir=sys.argv[1])
try:
    r = run_tcp_cluster(ctx, int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), payload=int(sys.argv[5]))
finally:
    ctx.cleanup()
n = max(1, r["commits"])
print(json.dumps({"ok": r["ok"], "rate": r["rate"], "commits": r["commits"], "latency_ms": r["latency_ms"],
                  "critical_ms": r["critical_ms"], "lease_attempts_per_commit": r["lease_attempts_per_commit"],
                  "syncs_per_commit": r["fsync"]["total_per_commit"], "syncs_by_role": r["fsync"]["per_commit"],
                  "wire_bytes_per_commit": {k: round(v / n) for k, v in r["wire_by_type"].items()},
                  "pipeline": r["pipeline"], "commits_by_peer": r["commits_by_peer"],
                  "replication_failures_by_peer": r["replication_failures_by_peer"],
                  "gil": sys._is_gil_enabled()}))
"""


def _busy_jiffies() -> tuple:
    """(busy, total) jiffies of the whole machine from ``/proc/stat``."""
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    idle = v[3] + v[4]
    return sum(v) - idle, sum(v)


def _children_cpu() -> float:
    """CPU seconds used by this process's finished children (user + system)."""
    ru = resource.getrusage(resource.RUSAGE_CHILDREN)
    return ru.ru_utime + ru.ru_stime


def foreign_cpus(seconds: float = 1.0) -> float:
    """Busy CPUs of the machine over ``seconds`` while this driver runs nothing (all foreign)."""
    b0, _ = _busy_jiffies()
    time.sleep(seconds)
    b1, _ = _busy_jiffies()
    return (b1 - b0) / os.sysconf("SC_CLK_TCK") / seconds


def await_quiet(limit: float, log) -> dict:
    """Wait until the foreign load is below ``limit`` busy CPUs; return what was seen."""
    waited, t0 = [], time.time()
    while True:
        busy = foreign_cpus()
        if busy < limit:
            return {"idle_busy_cpus": round(busy, 2), "waited_s": round(time.time() - t0, 1)}
        if not waited or time.time() - waited[-1] > 60:
            waited.append(time.time())
            log(f"waiting: {busy:.1f} foreign busy CPUs >= {limit}")


def commit_of(export: str) -> str:
    """The commit recorded in an export's ``BENCH_SOURCE.json``."""
    with open(os.path.join(export, "BENCH_SOURCE.json"), encoding="utf-8") as f:
        return json.load(f)["commit"]


def run_one(export: str, workdir: str, cell: str, args) -> dict:
    """One run of ``cell`` in a fresh interpreter rooted at ``export``."""
    os.makedirs(workdir, exist_ok=True)
    cmd = [sys.executable, "-X", f"gil={args.gil}", "-c", CHILD, workdir, str(args.peers), str(args.threads),
           str(args.seconds), str(CELLS[cell]["payload"])]
    started = time.time()
    b0, mine0 = _busy_jiffies()[0], _children_cpu()
    proc = subprocess.run(cmd, cwd=export, capture_output=True, text=True, timeout=600)
    wall = time.time() - started
    b1, mine1 = _busy_jiffies()[0], _children_cpu()
    # Machine CPU used during the run by anything but this run's process tree.
    foreign = max(0.0, (b1 - b0) / os.sysconf("SC_CLK_TCK") - (mine1 - mine0)) / wall
    out = {"export": export, "commit": commit_of(export), "cell": cell, "started": started,
           "wall_s": round(wall, 2), "foreign_cpus_during": round(foreign, 2)}
    lines = [x for x in proc.stdout.splitlines() if x.startswith("{")]
    if proc.returncode != 0 or not lines:
        out.update(ok=False, error=(proc.stderr or proc.stdout)[-3000:])
        return out
    out.update(json.loads(lines[-1]))
    return out


def summary(runs: list, exports: list, cells: list) -> dict:
    """Per cell: each export's median rate over verified runs and paired ratios of neighbours."""
    res = {}
    for cell in cells:
        by = {e: {r["round"]: r["rate"] for r in runs
                  if r["export"] == e and r["cell"] == cell and r.get("ok") and not r.get("discarded")}
              for e in exports}
        med = {commit_of(e)[:7]: (round(statistics.median(v.values()), 1) if v else None) for e, v in by.items()}
        pairs = {}
        for a, b in zip(exports, exports[1:]):
            ratios = [by[b][k] / by[a][k] for k in by[a] if k in by[b]]
            if ratios:
                ratios.sort()
                pairs[f"{commit_of(a)[:7]}->{commit_of(b)[:7]}"] = {
                    "median_ratio": round(statistics.median(ratios), 3), "n": len(ratios),
                    "below_1": sum(1 for x in ratios if x < 1), "min": round(ratios[0], 3),
                    "max": round(ratios[-1], 3)}
        first = exports[0]
        to_first = {}
        for e in exports[1:]:
            ratios = [by[e][k] / by[first][k] for k in by[first] if k in by[e]]
            if ratios:
                to_first[commit_of(e)[:7]] = round(statistics.median(ratios), 3)
        res[cell] = {"median_rate": med, "paired_neighbours": pairs, f"paired_vs_{commit_of(first)[:7]}": to_first,
                     "failed": sum(1 for r in runs if r["cell"] == cell and not r.get("ok"))}
    return res


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("exports", nargs="+")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--cells", default="blind,p100")
    ap.add_argument("--peers", type=int, default=3)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--seconds", type=float, default=3.0)
    ap.add_argument("--gil", type=int, default=0)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--json", required=True)
    ap.add_argument("--quiet-below", type=float, default=1.0,
                    help="start a run only when other processes keep fewer busy CPUs than this")
    ap.add_argument("--foreign-max", type=float, default=1.5,
                    help="a run during which other processes averaged this many busy CPUs or more is "
                         "discarded and repeated (recorded with discarded=True)")
    args = ap.parse_args()
    exports = [os.path.abspath(e) for e in args.exports]
    cells = args.cells.split(",")
    runs = []
    t0 = time.time()
    for rnd in range(args.rounds):
        order = exports if rnd % 2 == 0 else exports[::-1]
        for e in order:
            for cell in (cells if rnd % 2 == 0 else cells[::-1]):
                for _attempt in range(20):
                    gate = await_quiet(args.quiet_below, lambda m: print(m, flush=True))
                    r = run_one(e, args.workdir, cell, args)
                    r["round"], r["gate"] = rnd, gate
                    r["discarded"] = r["foreign_cpus_during"] >= args.foreign_max
                    runs.append(r)
                    if not r["discarded"]:
                        break
                    print(f"discarded: {r['foreign_cpus_during']} foreign CPUs during the run", flush=True)
                print(f"[{time.time() - t0:7.0f}s] r{rnd} {r['commit'][:7]} {cell:6s} "
                      f"{'ok' if r.get('ok') else 'FAILED'} {r.get('rate', 0):7.1f}/s "
                      f"p50 {r.get('latency_ms', {}).get('p50')} ms", flush=True)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({"args": vars(args), "exports": [{"dir": e, "commit": commit_of(e)} for e in exports],
                       "rounds_done": rnd + 1, "elapsed_s": round(time.time() - t0, 1),
                       "summary": summary(runs, exports, cells), "runs": runs}, f, indent=1)
    print(json.dumps(summary(runs, exports, cells), indent=1))


if __name__ == "__main__":
    main()
