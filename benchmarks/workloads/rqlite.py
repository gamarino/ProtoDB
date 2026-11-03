"""rqlite as the external, functionally comparable cluster reference (ruling R45).

rqlite is a replicated SQLite on Raft: a local consensus cluster that works offline, the
closest functional match to a ProtoDB cluster (single coherent state, majority-replicated
commits).  The workload starts three ``rqlited`` nodes on loopback (data directories in the
work directory), creates ``t(id INTEGER PRIMARY KEY)``, and drives it from
``3 x threads`` client threads (each bound to one node; followers forward writes to the
leader) for ``seconds``:

* per request: each HTTP request inserts one row (one Raft entry);
* batched: each request inserts ``batch`` rows in one transaction (``?transaction``);
* payload (Task 8.3f): each request inserts one row carrying a TEXT value of 100 B, 10 KB or
  100 KB into ``b(id, v)``; the ProtoDB side writes one value of the same size per commit.

The same work runs on a 3-peer ProtoDB TCP cluster (:func:`~benchmarks.workloads.cluster.run_tcp_cluster`):
one commit adds one id, or ``batch`` ids, to a CountedSet.  Both sides are verified: rqlite
by a strongly consistent ``SELECT COUNT(*), SUM(id)`` against the acknowledged inserts,
ProtoDB by reading the persisted root.

The binary is located by ``--rqlite PATH`` or the environment variable
``PROTODB_BENCH_RQLITED``.  Without it the workload is reported as pending: rqlite numbers
are never quoted from memory.  Settings are rqlite's defaults; the client is Python's
``http.client`` with one keep-alive connection per thread, so the rqlite figures include an
HTTP round trip per request that the embedded ProtoDB does not pay.
"""
import http.client
import json
import os
import shlex
import socket
import subprocess
import threading
import time

from benchmarks.harness import BenchResult, benchmark, latency_ms
from benchmarks.support import run_threads


def _free_ports(n: int) -> list:
    socks = []
    try:
        for _ in range(n):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


class RqliteCluster:
    """Three ``rqlited`` processes on loopback (see the module docstring)."""

    def __init__(self, binary: str, workdir: str, nodes: int = 3):
        ports = _free_ports(2 * nodes)
        self.http = [ports[2 * i] for i in range(nodes)]
        raft = [ports[2 * i + 1] for i in range(nodes)]
        join = ",".join(f"127.0.0.1:{p}" for p in raft)
        self.logs, self.procs = [], []
        try:
            for i in range(nodes):
                log = open(os.path.join(workdir, f"rqlited-{i}.log"), "w")
                self.logs.append(log)
                wrapper = [w.replace("{node}", f"n{i}")      # e.g. strace for a sync count
                           for w in shlex.split(os.environ.get("PROTODB_BENCH_RQLITE_WRAPPER", ""))]
                self.procs.append(subprocess.Popen(
                    wrapper + [binary, "-node-id", f"n{i}", "-http-addr", f"127.0.0.1:{self.http[i]}",
                     "-raft-addr", f"127.0.0.1:{raft[i]}", "-bootstrap-expect", str(nodes), "-join", join,
                     os.path.join(workdir, f"data{i}")], stdout=log, stderr=subprocess.STDOUT))
            self._wait_ready()
        except BaseException:
            self.close()
            raise

    def _get(self, port: int, path: str):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            return resp.status, resp.read().decode()
        finally:
            conn.close()

    def _wait_ready(self, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        while True:
            ready = 0
            for port in self.http:
                try:
                    status, body = self._get(port, "/readyz")
                    ready += status == 200 and "leader ok" in body
                except OSError:
                    pass                      # not listening yet: polled again below
            if ready == len(self.http):
                return
            if time.monotonic() > deadline or any(p.poll() is not None for p in self.procs):
                raise RuntimeError("rqlite cluster did not become ready")
            time.sleep(0.2)

    def execute(self, conn: http.client.HTTPConnection, statements: list, transaction: bool = False) -> list:
        """POST statements to ``/db/execute``; raise on any error; return the results."""
        path = "/db/execute?transaction" if transaction else "/db/execute"
        conn.request("POST", path, body=json.dumps(statements), headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        if resp.status != 200 or "error" in body or any("error" in r for r in body.get("results", [])):
            raise RuntimeError(f"rqlite error {resp.status}: {body}")
        return body["results"]

    def query_strong(self, sql: str) -> list:
        """A strongly consistent query (read through the Raft log) on node 0; returns the rows."""
        from urllib.parse import quote
        status, body = self._get(self.http[0], f"/db/query?level=strong&q={quote(sql)}")
        data = json.loads(body)
        if status != 200 or "error" in data["results"][0]:
            raise RuntimeError(f"rqlite query error: {body}")
        return data["results"][0]["values"]

    def close(self) -> None:
        """Terminate every node and wait for it."""
        for p in self.procs:
            if p.poll() is None:
                p.terminate()
        for p in self.procs:
            try:
                p.wait(15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for log in self.logs:
            log.close()


def _rqlite_version(binary: str) -> str:
    out = subprocess.run([binary, "-version"], capture_output=True, text=True, timeout=10)
    return (out.stdout or out.stderr).strip()


def _drive_rqlite(cl: RqliteCluster, threads: int, seconds: float, batch: int, base: int) -> dict:
    counts = [0] * threads
    latencies = []
    lock = threading.Lock()

    def worker_for(t):
        def worker():
            conn = http.client.HTTPConnection("127.0.0.1", cl.http[t % len(cl.http)], timeout=30)
            lat, j = [], 0
            first = base + t * 10**7
            stop_at = time.monotonic() + seconds
            try:
                while time.monotonic() < stop_at:
                    stmts = [["INSERT INTO t(id) VALUES(?)", first + j + x] for x in range(batch)]
                    t0 = time.perf_counter()
                    results = cl.execute(conn, stmts, transaction=batch > 1)
                    lat.append(time.perf_counter() - t0)
                    if sum(r.get("rows_affected", 0) for r in results) != batch:
                        raise RuntimeError(f"rqlite inserted fewer rows than requested: {results}")
                    j += batch
                    counts[t] += 1
            finally:
                conn.close()
            with lock:
                latencies.extend(lat)
        return worker

    elapsed, _ = run_threads([worker_for(t) for t in range(threads)])
    ids = [base + t * 10**7 + j for t in range(threads) for j in range(counts[t] * batch)]
    return {"requests": sum(counts), "rows": len(ids), "id_sum": sum(ids), "elapsed": elapsed,
            "latency_ms": latency_ms(latencies)}


def _drive_rqlite_payload(cl: RqliteCluster, threads: int, seconds: float, size: int) -> dict:
    """One row per request carrying a TEXT value of ``size`` bytes (table ``b``)."""
    counts = [0] * threads
    latencies = []
    lock = threading.Lock()

    def worker_for(t):
        def worker():
            conn = http.client.HTTPConnection("127.0.0.1", cl.http[t % len(cl.http)], timeout=30)
            lat, j = [], 0
            stop_at = time.monotonic() + seconds
            try:
                while time.monotonic() < stop_at:
                    key = t * 10**7 + j
                    unit = f"{key}:"
                    value = (unit * (size // len(unit) + 1))[:size]
                    t0 = time.perf_counter()
                    results = cl.execute(conn, [["INSERT INTO b(id, v) VALUES(?, ?)", key, value]])
                    lat.append(time.perf_counter() - t0)
                    if sum(r.get("rows_affected", 0) for r in results) != 1:
                        raise RuntimeError(f"rqlite inserted no row: {results}")
                    j += 1
                    counts[t] += 1
            finally:
                conn.close()
            with lock:
                latencies.extend(lat)
        return worker

    elapsed, _ = run_threads([worker_for(t) for t in range(threads)])
    return {"requests": sum(counts), "elapsed": elapsed, "latency_ms": latency_ms(latencies)}


def _payload_comparison(ctx, result, checksum, expected, p) -> None:
    """The payload dimension (Task 8.3f): the same values, one per commit or request, on rqlite
    and on a 3-peer ProtoDB TCP cluster in fsync mode (:func:`run_tcp_cluster` with ``payload``).
    Both are verified (rqlite: row count and total value length read strongly; ProtoDB: every
    persisted value).  Wire bytes are loopback bytes per commit (clients included)."""
    from benchmarks.workloads.cluster import loopback_bytes, payload_figures, run_tcp_cluster
    nodes, threads = 3, p["threads"]
    for size in p["payloads"]:
        rq_rates, rq_figs, runs = [], [], []
        for rep in range(p["repeat"]):
            cl = RqliteCluster(p["binary"], ctx.tempdir("rqlite"))
            try:
                conn = http.client.HTTPConnection("127.0.0.1", cl.http[0], timeout=30)
                cl.execute(conn, ["CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)"])
                conn.close()
                before = loopback_bytes()
                r = _drive_rqlite_payload(cl, nodes * threads, p["seconds"], size)
                after = loopback_bytes()
                count, total = cl.query_strong("SELECT COUNT(*), SUM(length(v)) FROM b")[0]
            finally:
                cl.close()
            key = f"rqlite payload {size} B, run {rep + 1}"
            ok = (count, total or 0) == (r["requests"], r["requests"] * size)
            result.check(f"{key}: rows and value bytes equal the acknowledged inserts", ok,
                         f"{(count, total)} != {(r['requests'], r['requests'] * size)}")
            checksum[key], expected[key] = (count, total or 0), (r["requests"], r["requests"] * size)
            rq_rates.append(r["requests"] / r["elapsed"])
            rq_figs.append({"rate": r["requests"] / r["elapsed"], "latency ms": r["latency_ms"],
                            "loopback bytes per commit": round((after - before) / max(1, r["requests"]))
                            if before is not None and after is not None else None})
            t = run_tcp_cluster(ctx, nodes, threads, p["seconds"], payload=size)
            key = f"protodb payload {size} B, run {rep + 1}"
            result.check(f"{key}: persisted state holds exactly the acknowledged ids and values", t["ok"], "")
            checksum[key], expected[key] = t["ok"], True
            runs.append(t)
        result.add_rate(f"rqlite payload {size} B: requests (commits)/s", rq_rates, "commits/s")
        result.add_rate(f"protodb payload {size} B: commits/s", [t["rate"] for t in runs], "commits/s")
        mid = sorted(rq_figs, key=lambda f: f["rate"])[len(rq_figs) // 2]
        result.metrics[f"rqlite payload {size} B: payload MB/s"] = round(mid["rate"] * size / 1e6, 3)
        result.metrics[f"rqlite payload {size} B: latency ms"] = mid["latency ms"]
        result.metrics[f"rqlite payload {size} B: loopback bytes per commit"] = mid["loopback bytes per commit"]
        for name, value in payload_figures(runs).items():
            result.metrics[f"protodb payload {size} B: {name}"] = value


@benchmark("rqlite_reference", quick={"threads": 2, "seconds": 0.5, "batch": 10, "repeat": 1, "payloads": [100]},
           full={"threads": 4, "seconds": 3.0, "batch": 50, "repeat": 2, "payloads": [100, 10240, 102400]})
def rqlite_reference(ctx, p):
    """3-node rqlite on loopback vs a 3-peer ProtoDB TCP cluster: small commits, per request and batched."""
    binary = ctx.rqlite
    if not binary or not os.access(binary, os.X_OK):
        return BenchResult.pending("rqlite not available (pass --rqlite PATH or set PROTODB_BENCH_RQLITED)")
    from benchmarks.workloads.cluster import run_tcp_cluster
    nodes, threads = 3, p["threads"]
    result = BenchResult({"nodes": nodes, "client_threads": nodes * threads, "seconds": p["seconds"],
                          "batch": p["batch"], "rqlite": _rqlite_version(binary),
                          "protodb": "3 TCP subprocess peers, TieredStorage durability=fsync, blind CountedSet adds"},
                         None, None)
    checksum, expected = {}, {}
    for label, batch in (("per request", 1), (f"batched x{p['batch']}", p["batch"])):
        rq_rates, rq_rows, pd_rates, pd_rows = [], [], [], []
        for rep in range(p["repeat"]):
            cl = RqliteCluster(binary, ctx.tempdir("rqlite"))
            try:
                conn = http.client.HTTPConnection("127.0.0.1", cl.http[0], timeout=30)
                cl.execute(conn, ["CREATE TABLE t (id INTEGER PRIMARY KEY)"])
                conn.close()
                r = _drive_rqlite(cl, nodes * threads, p["seconds"], batch, base=0)
                count, total = cl.query_strong("SELECT COUNT(*), SUM(id) FROM t")[0]
            finally:
                cl.close()
            key = f"rqlite {label}, run {rep + 1}"
            ok = (count, total or 0) == (r["rows"], r["id_sum"])
            result.check(f"{key}: rows in the table equal the acknowledged inserts", ok,
                         f"{(count, total)} != {(r['rows'], r['id_sum'])}")
            checksum[key], expected[key] = (count, total or 0), (r["rows"], r["id_sum"])
            rq_rates.append(r["requests"] / r["elapsed"])
            rq_rows.append(r["rows"] / r["elapsed"])
            result.metrics[f"rqlite {label}: request latency ms (run {rep + 1})"] = r["latency_ms"]

            t = run_tcp_cluster(ctx, nodes, threads, p["seconds"], batch=batch)
            key = f"protodb {label}, run {rep + 1}"
            result.check(f"{key}: persisted state holds exactly the acknowledged ids", t["ok"], "")
            checksum[key], expected[key] = t["ok"], True
            pd_rates.append(t["rate"])
            pd_rows.append(t["row_rate"])
            result.metrics[f"protodb {label}: commit latency ms (run {rep + 1})"] = t["latency_ms"]
            result.metrics[f"protodb {label}: critical section ms per commit (run {rep + 1})"] = t["critical_ms"]
        result.add_rate(f"rqlite {label}: requests (commits)/s", rq_rates, "commits/s")
        result.add_rate(f"protodb {label}: commits/s", pd_rates, "commits/s")
        result.add_rate(f"rqlite {label}: rows/s", rq_rows, "rows/s")
        result.add_rate(f"protodb {label}: rows/s", pd_rows, "rows/s")
    _payload_comparison(ctx, result, checksum, expected, dict(p, binary=binary))
    result.checksum, result.expected = checksum, expected
    return result
