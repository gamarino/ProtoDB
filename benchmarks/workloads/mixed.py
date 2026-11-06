"""Mixed read/write load on a 3-member cluster: readers that never wait (Task 9c, ruling R97).

ProtoDB: three TCP subprocess peers (:func:`~benchmarks.workloads.cluster.run_tcp_cluster`,
``durability="fsync"``).  Node ``n0`` first commits a ``catalog`` Dictionary of ``catalog``
entries; then, on every node, ``writers`` threads commit (one id and one 100-byte value per
commit, the headline commit) while ``readers`` threads read random catalog entries, each in a
new transaction: a snapshot of the node's last known committed root, with no lock and no
message to any other node.

rqlite: three ``rqlited`` nodes on loopback (:class:`~benchmarks.workloads.rqlite.RqliteCluster`)
with the same catalog in ``catalog(id, v)``; ``writers`` client threads per node insert one row
with a 100-byte value per request, and ``readers`` client threads per node run
``SELECT v FROM catalog WHERE id = ?`` at one read consistency level per cell: ``none``,
``weak`` (rqlite's default), ``linearizable`` and ``strong``.  The level semantics are those of
rqlite's documentation (https://rqlite.io/docs/api/read-consistency/), summarised in
``docs/source/performance.rst``.  The clients are Python ``http.client`` threads in the
benchmark process, one keep-alive connection each, so every rqlite read pays an HTTP round
trip that an embedded ProtoDB read does not.

Before the window, every node of either system must see the whole catalog in a local read
(ProtoDB: a new transaction on that node; rqlite: a ``level=none`` count on that node): without
that wait, ``level=none`` reads on a follower that had not applied the catalog yet returned no
row in the first run of Task 9c (1-5 reads per run), as rqlite documents for that level.

Cells, each run separately: writers only, readers only, writers and readers together.  Every
read is checked against the catalog's value; every write is verified as in the write workloads
(ProtoDB: the persisted state holds exactly the acknowledged ids and values; rqlite: a strong
``COUNT(*), SUM(length(v))`` equals the acknowledged inserts).

Caveats that the figures carry (R97):
* a ProtoDB read whose records are not in the node's caches fetches them (from the local tier
  or, on a node that is not a data replica of them, from a peer: D54): that read waits for I/O;
* a ProtoDB read is a snapshot of the root the node last learned, not a linearizable read;
  only a query to the owner first would make it one (not measured here).
"""
import http.client
import json
import random
import threading
import time

from benchmarks.harness import BenchResult, benchmark, percentile
from benchmarks.support import run_threads

#: rqlite read consistency levels measured (https://rqlite.io/docs/api/read-consistency/).
RQLITE_LEVELS = ("none", "weak", "linearizable", "strong")
PAYLOAD = 100


def catalog_value(key: int) -> str:
    """The catalog value of ``key``, the same on both systems."""
    from benchmarks.cluster_peer import catalog_value as value
    return value(key)


def protodb_cell(ctx, writers: int, readers: int, seconds: float, catalog: int) -> dict:
    """One verified ProtoDB cell; returns its figures (``ok`` False when any check failed)."""
    from benchmarks.workloads.cluster import run_tcp_cluster
    r = run_tcp_cluster(ctx, 3, writers, seconds, payload=PAYLOAD if writers else 0, readers=readers,
                        catalog=catalog)
    out = {"system": "protodb", "writers": writers, "readers": readers, "ok": r["ok"],
           "commits": r["commits"], "commit_rate": r["rate"] if writers else None,
           "commit_latency_ms": r["latency_ms"] if writers else None,
           "syncs_per_commit": r["fsync"]["total_per_commit"] if writers else None,
           # why a failed run failed: lost or extra ids, writer threads that died (an unknown outcome)
           "ids_missing": r["ids_missing"], "ids_extra": r["ids_extra"], "ids_duplicated": r["ids_duplicated"],
           "thread_errors": [[e[:200] for e in errs] for errs in r["thread_errors_by_peer"]],
           "commits_by_peer": r["commits_by_peer"], "kept_run_dir": r["kept_run_dir"]}
    if readers:
        rd = r["reads"]
        out.update({"reads": rd["reads"], "bad_reads": rd["bad"], "read_rate": rd["reads_per_s"],
                    "read_p50_us": rd["p50_us"], "read_p99_us": rd["p99_us"], "read_max_us": rd["max_us"],
                    "read_steady_p50_us": rd["steady_p50_us"], "read_steady_p99_us": rd["steady_p99_us"],
                    "read_by_node": rd["by_node"], "versions_seen": rd["versions_seen"]})
    return out


def _rqlite_query(conn, level: str, key: int):
    body = json.dumps([["SELECT v FROM catalog WHERE id = ?", key]])
    conn.request("POST", f"/db/query?level={level}", body=body, headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    data = json.loads(resp.read().decode())
    if resp.status != 200 or "error" in data or "error" in data["results"][0]:
        raise RuntimeError(f"rqlite query error {resp.status}: {data}")
    values = data["results"][0].get("values") or []
    return values[0][0] if values else None


def _await_catalog(port: int, catalog: int, timeout: float = 60.0) -> None:
    """Wait until the node at ``port`` answers a local (``level=none``) count of the whole catalog."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    deadline = time.monotonic() + timeout
    try:
        while True:
            body = json.dumps([["SELECT COUNT(*) FROM catalog"]])
            conn.request("POST", "/db/query?level=none", body=body, headers={"Content-Type": "application/json"})
            data = json.loads(conn.getresponse().read().decode())
            values = data["results"][0].get("values") if "error" not in data["results"][0] else None
            if values and values[0][0] == catalog:
                return
            if time.monotonic() > deadline:
                raise RuntimeError(f"rqlite node on port {port} did not apply the catalog: {data}")
            time.sleep(0.05)
    finally:
        conn.close()


def rqlite_cell(ctx, binary: str, writers: int, readers: int, level, seconds: float, catalog: int) -> dict:
    """One verified rqlite cell (``level`` None when there are no readers)."""
    from benchmarks.workloads.rqlite import RqliteCluster
    cl = RqliteCluster(binary, ctx.tempdir("rqmixed"))
    try:
        conn = http.client.HTTPConnection("127.0.0.1", cl.http[0], timeout=30)
        cl.execute(conn, ["CREATE TABLE catalog (id INTEGER PRIMARY KEY, v TEXT)",
                          "CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)"])
        for lo in range(0, catalog, 1000):
            cl.execute(conn, [["INSERT INTO catalog(id, v) VALUES(?, ?)", k, catalog_value(k)]
                              for k in range(lo, min(catalog, lo + 1000))], transaction=True)
        conn.close()
        loaded = cl.query_strong("SELECT COUNT(*) FROM catalog")[0][0]
        if loaded != catalog:
            raise RuntimeError(f"rqlite catalog holds {loaded} rows, not {catalog}")
        nodes = len(cl.http)
        # Every node must have applied the whole catalog before the window, as every ProtoDB peer
        # waits until its own reads see it: a level=none read on a follower that has not applied
        # the catalog yet returns no row (rqlite's documented semantics for none, Task 9c).
        for port in cl.http:
            _await_catalog(port, catalog)
        w_counts = [0] * (writers * nodes)
        w_lat, r_lat, r_stats = [], [], []
        lock = threading.Lock()

        def writer_for(t):
            def worker():
                conn = http.client.HTTPConnection("127.0.0.1", cl.http[t % nodes], timeout=30)
                lat, j = [], 0
                stop_at = time.monotonic() + seconds
                try:
                    while time.monotonic() < stop_at:
                        key = t * 10**7 + j
                        unit = f"{key}:"
                        value = (unit * (PAYLOAD // len(unit) + 1))[:PAYLOAD]
                        t0 = time.perf_counter()
                        res = cl.execute(conn, [["INSERT INTO b(id, v) VALUES(?, ?)", key, value]])
                        lat.append(time.perf_counter() - t0)
                        if sum(x.get("rows_affected", 0) for x in res) != 1:
                            raise RuntimeError(f"rqlite inserted no row: {res}")
                        j += 1
                        w_counts[t] += 1
                finally:
                    conn.close()
                with lock:
                    w_lat.extend(lat)
            return worker

        def reader_for(r):
            def worker():
                conn = http.client.HTTPConnection("127.0.0.1", cl.http[r % nodes], timeout=30)
                rng = random.Random(r)
                lat, bad = [], 0
                start = time.monotonic()
                stop_at = start + seconds
                steady_at, steady_from = start + min(0.5, seconds / 4), None
                try:
                    while True:
                        now = time.monotonic()
                        if now >= stop_at:
                            break
                        if steady_from is None and now >= steady_at:
                            steady_from = len(lat)
                        key = rng.randrange(catalog)
                        t0 = time.perf_counter()
                        value = _rqlite_query(conn, level, key)
                        lat.append(round((time.perf_counter() - t0) * 1e6))
                        if value != catalog_value(key):
                            bad += 1
                finally:
                    conn.close()
                with lock:
                    r_lat.append((r % nodes, lat, len(lat) if steady_from is None else steady_from))
                    r_stats.append(bad)
            return worker

        fns = [writer_for(t) for t in range(writers * nodes)] + [reader_for(r) for r in range(readers * nodes)]
        elapsed, _ = run_threads(fns)
        count, total = cl.query_strong("SELECT COUNT(*), SUM(length(v)) FROM b")[0]
    finally:
        cl.close()
    commits = sum(w_counts)
    ok_w = (count, total or 0) == (commits, commits * PAYLOAD)
    out = {"system": "rqlite", "level": level, "writers": writers, "readers": readers,
           "commits": commits, "commit_rate": commits / elapsed if writers else None,
           "commit_latency_ms": _ms(w_lat) if writers else None,
           "write_check": [count, total or 0, commits * PAYLOAD]}
    ok_r = True
    if readers:
        every = [x for _, lat, _ in r_lat for x in lat]
        steady = [x for _, lat, s in r_lat for x in lat[s:]]
        bad = sum(r_stats)
        ok_r = bad == 0 and bool(every) and len(r_lat) == readers * nodes
        by_node = []
        for node in range(nodes):
            xs = [x for n, lat, _ in r_lat if n == node for x in lat]
            by_node.append({"reads": len(xs), "p50_us": percentile(xs, 50) if xs else None,
                            "p99_us": percentile(xs, 99) if xs else None, "max_us": max(xs) if xs else None})
        out.update({"reads": len(every), "bad_reads": bad, "read_rate": len(every) / elapsed,
                    "read_p50_us": percentile(every, 50) if every else None,
                    "read_p99_us": percentile(every, 99) if every else None,
                    "read_max_us": max(every) if every else None,
                    "read_steady_p50_us": percentile(steady, 50) if steady else None,
                    "read_steady_p99_us": percentile(steady, 99) if steady else None, "read_by_node": by_node})
    out["ok"] = ok_w and ok_r
    return out


def _ms(latencies) -> dict:
    from benchmarks.harness import latency_ms
    return latency_ms(latencies)


def cells(readers_list, levels, with_rqlite: bool) -> list:
    """The cells of one repetition, in run order: ``(system, writers, readers, level)``."""
    out = [("protodb", 4, 0, None)]
    for readers in readers_list:
        out += [("protodb", 0, readers, None), ("protodb", 4, readers, None)]
    if with_rqlite:
        out.append(("rqlite", 4, 0, None))
        for level in levels:
            for readers in readers_list:
                out += [("rqlite", 0, readers, level), ("rqlite", 4, readers, level)]
    return out


def cell_label(system, writers, readers, level) -> str:
    """The label of a cell in results and reports."""
    load = "writers only" if not readers else ("readers only" if not writers else "writers + readers")
    lv = f", level={level}" if level else ""
    return f"{system}, {load}" + (f", {readers} reader(s)/node" if readers else "") + lv


def run_cell(ctx, cell, seconds: float, catalog: int) -> dict:
    """Run one cell of :func:`cells`."""
    system, writers, readers, level = cell
    if system == "protodb":
        return protodb_cell(ctx, writers, readers, seconds, catalog)
    return rqlite_cell(ctx, ctx.rqlite, writers, readers, level, seconds, catalog)


@benchmark("mixed_read_write",
           quick={"seconds": 0.5, "catalog": 500, "readers": [1], "levels": ["weak"], "repeat": 1},
           full={"seconds": 3.0, "catalog": 10_000, "readers": [1, 4], "levels": list(RQLITE_LEVELS), "repeat": 2})
def mixed_read_write(ctx, p):
    """3 members: readers on every node during commits (R97); rqlite at each read consistency level."""
    import os
    with_rqlite = bool(ctx.rqlite and os.access(ctx.rqlite, os.X_OK))
    plan = cells(p["readers"], p["levels"], with_rqlite)
    result = BenchResult({"members": 3, "writers_per_node": 4, "catalog": p["catalog"], "seconds": p["seconds"],
                          "readers_per_node": p["readers"], "cells": len(plan),
                          "rqlite_levels": p["levels"] if with_rqlite else "pending (no rqlited binary)"}, None, None)
    if not with_rqlite:
        result.notes.append("rqlite cells pending: no rqlited binary (pass --rqlite PATH)")
    runs = {cell: [] for cell in plan}
    for rep in range(p["repeat"]):
        for cell in plan:                       # cells interleaved within each repetition
            runs[cell].append(run_cell(ctx, cell, p["seconds"], p["catalog"]))
    checksum, expected = {}, {}
    for cell, rs in runs.items():
        label = cell_label(*cell)
        ok = all(r["ok"] for r in rs)
        result.check(f"{label}: every read right, writes exactly the acknowledged ones", ok,
                     "; ".join(json.dumps({k: r.get(k) for k in ("ok", "bad_reads", "write_check", "ids_missing", "ids_extra",
                                                               "thread_errors")}) for r in rs))
        checksum[label], expected[label] = ok, True
        if cell[1]:
            result.add_rate(f"{label}: commits/s", [r["commit_rate"] for r in rs], "commits/s")
        if cell[2]:
            result.add_rate(f"{label}: reads/s", [r["read_rate"] for r in rs], "reads/s")
            result.metrics[f"{label}: read p50/p99 us, each run"] = [[r["read_p50_us"], r["read_p99_us"]] for r in rs]
        result.metrics[f"{label}: runs"] = rs
    result.checksum, result.expected = checksum, expected
    return result
