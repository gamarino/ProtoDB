"""Interleaved measurement sessions: the cluster headline and the mixed read/write cells (Task 9c).

``python -m benchmarks.session --reps 5 --json OUT [--rqlite PATH] [--workdir DIR] [--only GROUP ...]``

The suite (``python -m benchmarks``) measures each variant's repetitions back to back.  This
driver instead runs every cell once per repetition, in a fixed order, alternating the GIL mode
(``-X gil=0`` then ``-X gil=1``) for each cell, so that a slow period of the machine (frequency
scaling, another process) spreads over every cell and both modes instead of biasing one.  Each
cell runs in a child interpreter of the requested mode (``python -X gil=N -m benchmarks.session
--cell ...``), is verified there exactly as in the suite, and returns one JSON record; a cell
that fails verification or crashes is recorded as failed and the session goes on.

Groups (``--only``):

* ``headline``: 3 TCP peers x 4 threads, one 100-byte value per commit; the same with 50 ids per
  commit; 5 peers x 4 threads at 100 B (``run_tcp_cluster``, ``fsync``); rqlite (3 nodes) with
  one 100-byte row per request and with 50 rows plus one 100-byte row per request;
* ``mixed``: the cells of :mod:`benchmarks.workloads.mixed` (R97).

Before the session the driver records the stray-process check (no ``rqlited`` and no other
benchmark process may be running) and refuses to start when it fails; it records the free
space of the work directory and the environment of each mode.  The output JSON holds every
cell record and, per cell and mode, the median and range of each figure.
"""
import argparse
import http.client
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time

from benchmarks.harness import REPO, latency_ms

PAYLOAD = 100


class _Ctx:
    """The part of :class:`benchmarks.harness.Context` the cluster helpers use."""

    def __init__(self, workdir: str, rqlite=None):
        self.workdir = tempfile.mkdtemp(prefix="cell-", dir=workdir)
        self.rqlite = rqlite

    def tempdir(self, name: str) -> str:
        return tempfile.mkdtemp(prefix=f"{name}-", dir=self.workdir)


def headline_cells(with_rqlite: bool) -> list:
    """``(kind, args)`` of the headline group, in run order."""
    out = [("protodb", {"peers": 3, "threads": 4, "batch": 1}),
           ("protodb", {"peers": 3, "threads": 4, "batch": 50}),
           ("protodb", {"peers": 5, "threads": 4, "batch": 1})]
    if with_rqlite:
        out += [("rqlite", {"batch": 1}), ("rqlite", {"batch": 50})]
    return out


def mixed_cells(with_rqlite: bool) -> list:
    from benchmarks.workloads.mixed import RQLITE_LEVELS, cells
    return [("mixed", {"cell": list(c)}) for c in cells([1, 4], RQLITE_LEVELS, with_rqlite)]


def label_of(kind: str, args: dict) -> str:
    """A cell's label."""
    if kind == "protodb":
        return f"protodb {args['peers']} peers x {args['threads']} threads, {args['batch']} id(s) + 100 B per commit"
    if kind == "rqlite":
        return f"rqlite 3 nodes x 4 clients, {args['batch']} row(s) + 100 B per request"
    from benchmarks.workloads.mixed import cell_label
    return "mixed: " + cell_label(*args["cell"])


# -- cells (run in the child) ----------------------------------------------------------------

def _protodb(ctx, a, seconds):
    from benchmarks.workloads.cluster import run_tcp_cluster
    r = run_tcp_cluster(ctx, a["peers"], a["threads"], seconds, batch=a["batch"], payload=PAYLOAD)
    return {"ok": r["ok"], "commits": r["commits"], "rate": r["rate"], "row_rate": r["row_rate"],
            "latency_ms": r["latency_ms"], "syncs_per_commit": r["fsync"]["total_per_commit"],
            "syncs_by_role": r["fsync"]["per_commit"], "critical_ms": r["critical_ms"],
            "commits_by_peer": r["commits_by_peer"], "ids_missing": r["ids_missing"], "ids_extra": r["ids_extra"],
            "thread_errors": r["thread_errors_by_peer"], "kept_run_dir": r["kept_run_dir"],
            "wire_bytes_per_commit": round(sum(r["wire_by_type"].values()) / max(1, r["commits"]))}


def _rqlite(ctx, a, seconds):
    """3 rqlite nodes, 12 clients: ``batch`` id rows and one 100-byte row per request (one
    ``?transaction`` request when ``batch`` > 1), the same work as a ProtoDB commit."""
    from benchmarks.support import run_threads
    from benchmarks.workloads.rqlite import RqliteCluster
    batch = a["batch"]
    cl = RqliteCluster(ctx.rqlite, ctx.tempdir("rq"))
    try:
        conn = http.client.HTTPConnection("127.0.0.1", cl.http[0], timeout=30)
        cl.execute(conn, ["CREATE TABLE t (id INTEGER PRIMARY KEY)", "CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)"])
        conn.close()
        counts, lat_all, lock = [0] * 12, [], threading.Lock()

        def worker_for(t):
            def worker():
                c = http.client.HTTPConnection("127.0.0.1", cl.http[t % 3], timeout=30)
                lat, j = [], 0
                stop_at = time.monotonic() + seconds
                try:
                    while time.monotonic() < stop_at:
                        first = t * 10**7 + j
                        unit = f"{first}:"
                        value = (unit * (PAYLOAD // len(unit) + 1))[:PAYLOAD]
                        stmts = [["INSERT INTO t(id) VALUES(?)", first + x] for x in range(batch)]
                        stmts.append(["INSERT INTO b(id, v) VALUES(?, ?)", first, value])
                        t0 = time.perf_counter()
                        res = cl.execute(c, stmts, transaction=batch > 1)
                        lat.append(time.perf_counter() - t0)
                        if sum(x.get("rows_affected", 0) for x in res) != batch + 1:
                            raise RuntimeError(f"rqlite inserted fewer rows than requested: {res}")
                        j += batch
                        counts[t] += 1
                finally:
                    c.close()
                with lock:
                    lat_all.extend(lat)
            return worker

        elapsed, _ = run_threads([worker_for(t) for t in range(12)])
        ids = [t * 10**7 + j for t in range(12) for j in range(counts[t] * batch)]
        got_t = cl.query_strong("SELECT COUNT(*), SUM(id) FROM t")[0]
        got_b = cl.query_strong("SELECT COUNT(*), SUM(length(v)) FROM b")[0]
    finally:
        cl.close()
    commits = sum(counts)
    ok = (got_t[0], got_t[1] or 0) == (len(ids), sum(ids)) and (got_b[0], got_b[1] or 0) == (commits, commits * PAYLOAD)
    return {"ok": ok, "commits": commits, "rate": commits / elapsed, "row_rate": len(ids) / elapsed,
            "latency_ms": latency_ms(lat_all), "check": [got_t, got_b, [len(ids), sum(ids), commits]]}


def _mixed(ctx, a, seconds):
    from benchmarks.workloads.mixed import run_cell
    return run_cell(ctx, tuple(a["cell"]), seconds, 10_000)


def run_child(kind: str, args: dict, seconds: float, workdir: str, rqlite) -> dict:
    """Run one cell in this interpreter and return its record (never raises)."""
    import traceback
    ctx = _Ctx(workdir, rqlite)
    started = time.perf_counter()
    try:
        record = {"protodb": _protodb, "rqlite": _rqlite, "mixed": _mixed}[kind](ctx, args, seconds)
    except Exception:            # recorded as a failed cell; the session continues
        record = {"ok": False, "error": traceback.format_exc()[-3000:]}
    finally:
        shutil.rmtree(ctx.workdir, ignore_errors=True)
    record["elapsed_s"] = round(time.perf_counter() - started, 2)
    record["gil_enabled"] = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    return record


# -- session (the parent) ----------------------------------------------------------------------

def stray_processes() -> list:
    """``rqlited`` and benchmark processes other than this one (``pgrep -af``)."""
    mine, pid = set(), os.getpid()
    while pid > 1:                        # this process and its ancestors (the shell that started it)
        mine.add(str(pid))
        try:
            with open(f"/proc/{pid}/stat") as f:
                pid = int(f.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    out = subprocess.run(["pgrep", "-af", "rqlited|cluster_peer|benchmarks|pytest"], capture_output=True,
                         text=True).stdout.splitlines()
    return [line for line in out if line.split()[0] not in mine and "pgrep" not in line]


def _summary(records: list) -> dict:
    keys = ("rate", "row_rate", "commit_rate", "read_rate", "read_p50_us", "read_p99_us", "read_steady_p99_us",
            "syncs_per_commit")
    out = {"runs": len(records), "failed": sum(1 for r in records if not r.get("ok"))}
    for k in keys:
        xs = [r[k] for r in records if r.get(k) is not None]
        if xs:
            out[k] = {"median": statistics.median(xs), "min": min(xs), "max": max(xs)}
    lat = [r["latency_ms"]["p50"] for r in records if r.get("latency_ms")]
    if lat:
        out["latency_p50_ms"] = {"median": statistics.median(lat), "min": min(lat), "max": max(lat)}
    lat = [r["commit_latency_ms"]["p50"] for r in records if r.get("commit_latency_ms")]
    if lat:
        out["commit_latency_p50_ms"] = {"median": statistics.median(lat), "min": min(lat), "max": max(lat)}
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m benchmarks.session")
    p.add_argument("--reps", type=int, default=5)
    p.add_argument("--seconds", type=float, default=3.0)
    p.add_argument("--json")
    p.add_argument("--workdir", default=os.path.join(REPO, "benchmarks", ".work"))
    p.add_argument("--rqlite", default=os.environ.get("PROTODB_BENCH_RQLITED"))
    p.add_argument("--only", nargs="+", default=["headline", "mixed"], choices=["headline", "mixed"])
    p.add_argument("--modes", default="0,1", help="GIL modes to alternate (default 0,1)")
    p.add_argument("--cell", help=argparse.SUPPRESS)
    p.add_argument("--cell-out", help=argparse.SUPPRESS)
    a = p.parse_args(argv)
    workdir = os.path.abspath(a.workdir)
    os.makedirs(workdir, exist_ok=True)
    rqlite = os.path.abspath(a.rqlite) if a.rqlite else None
    if a.cell:
        kind, args = json.loads(a.cell)
        record = run_child(kind, args, a.seconds, workdir, rqlite)
        with open(a.cell_out, "w") as f:
            json.dump(record, f)
        return 0
    stray = stray_processes()
    usage = shutil.disk_usage(workdir)
    check = {"stray_processes": stray, "checked_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
             "workdir_free_bytes": usage.free}
    print(f"stray-process check: {stray or 'none'}; free {usage.free / 2**30:.0f} GiB in {workdir}")
    if stray:
        print("refusing to start: stop the processes above first")
        return 2
    with_rqlite = bool(rqlite and os.access(rqlite, os.X_OK))
    plan = []
    if "headline" in a.only:
        plan += headline_cells(with_rqlite)
    if "mixed" in a.only:
        plan += mixed_cells(with_rqlite)
    modes = [int(x) for x in a.modes.split(",")]
    envs = {}
    for mode in modes:
        out = subprocess.run([sys.executable, "-X", f"gil={mode}", "-c",
                              "import json,sys;sys.path.insert(0,'.');from benchmarks.harness import "
                              "capture_environment,Context;print(json.dumps(capture_environment(Context("
                              f"False,[1],1,0,{workdir!r},{rqlite!r}))))"],
                             cwd=REPO, capture_output=True, text=True, check=True)
        envs[f"gil={mode}"] = json.loads(out.stdout)
    records = []
    started = time.perf_counter()
    for rep in range(1, a.reps + 1):
        for kind, args in plan:
            for mode in modes:
                with tempfile.NamedTemporaryFile(suffix=".json", dir=workdir, delete=False) as tmp:
                    out_path = tmp.name
                cmd = [sys.executable, "-X", f"gil={mode}", "-m", "benchmarks.session", "--cell",
                       json.dumps([kind, args]), "--cell-out", out_path, "--seconds", str(a.seconds),
                       "--workdir", workdir] + (["--rqlite", rqlite] if rqlite else [])
                try:
                    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=900)
                    with open(out_path) as f:
                        rec = json.load(f)
                except (subprocess.TimeoutExpired, OSError, ValueError) as e:
                    rec = {"ok": False, "error": f"cell child failed: {e!r}"}
                finally:
                    if os.path.exists(out_path):
                        os.unlink(out_path)
                rec.update({"rep": rep, "mode": f"gil={mode}", "kind": kind, "args": args,
                            "label": label_of(kind, args)})
                records.append(rec)
                shown = {k: (round(v, 1) if isinstance(v, float) else v) for k, v in rec.items()
                         if k in ("ok", "rate", "commit_rate", "read_rate", "read_p50_us", "read_p99_us")}
                print(f"rep {rep} {rec['mode']} {rec['label']}: {shown}", flush=True)
    summary = {}
    for rec in records:
        summary.setdefault(rec["label"], {}).setdefault(rec["mode"], []).append(rec)
    summary = {lab: {m: _summary(rs) for m, rs in modes_.items()} for lab, modes_ in summary.items()}
    failed = sum(1 for r in records if not r.get("ok"))
    print(f"{len(records)} cell runs in {time.perf_counter() - started:.0f} s, {failed} failed")
    if a.json:
        doc = {"schema": 1, "kind": "session", "check": check, "environments": envs, "reps": a.reps,
               "seconds": a.seconds, "order": "for each repetition, every cell in plan order, each in every GIL "
               "mode in turn", "plan": [[k, v] for k, v in plan], "summary": summary, "records": records,
               "total_s": time.perf_counter() - started}
        with open(a.json, "w") as f:
            json.dump(doc, f, indent=1, default=str)
        print(f"written {a.json}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
