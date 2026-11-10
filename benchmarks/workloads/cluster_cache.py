"""Reads across transactions in a cluster under concurrent writes (Task 9c CARRY, ruling R110).

Shape: 3 members.  Writers on node A (``n0``), readers on nodes B and C (``n1``, ``n2``), each read
in a new transaction.  The writers update random keys of a ``catalog`` (one key per commit); the
readers either read random catalog keys (``point``) or run ``products where cat == c`` (``query``),
on a ``products`` collection the writers never touch.  Peers: :mod:`benchmarks.cache_peer`.

ProtoDB cells (R110 cases 1-3):

1. ``point`` reads with the decoded-atom cache at its default size (it holds the working set),
   at a size below the working set, and off (``atom_cache_entries=0``): the same work, so the
   difference is the cache.
2. In the same cells, the atom decodes per read on B and C (every decode from stored bytes,
   :mod:`benchmarks.decode_counter`), against the atoms each commit on A creates (the twins A's
   cache admits on save, per commit): with the cache, a reader decodes only atoms that are new in
   a root it has not read yet; without it, every atom a read touches.
3. ``query`` reads with the query result cache on (default size) and off
   (``query_cache_bytes=0``): its hit rate under writes to another collection, and the latency.

rqlite cells (case 4): three ``rqlited`` nodes; 4 writer clients on node 0 run ``UPDATE catalog``
with the same key partitions and values; 2 reader clients on each of nodes 1 and 2 run the same
point read (``SELECT v FROM catalog WHERE id = ?``) or query (``SELECT id FROM products WHERE cat =
? ORDER BY id``, index on ``cat``) at each read consistency level.

Verification, in every cell: every read is checked (a point value must be a well-formed value of
its key; a query must return exactly the model's ids); the final catalog, read by a new instance
from the store (ProtoDB) or by a ``strong`` query (rqlite), must equal the last value each writer
committed for each key; ProtoDB reader nodes must report that their reads saw more than one
database version (``versions_seen``: the reads crossed roots written on another node).
"""
import http.client
import json
import os
import random
import shutil
import subprocess
import tempfile
import threading
import time

from benchmarks.harness import BenchResult, benchmark, percentile

PEER_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache_peer.py")
RQLITE_LEVELS = ("none", "weak", "linearizable", "strong")
#: Default shape (the full parameter set): 20,000 catalog entries, 5,000 products in 100
#: categories (50 rows per query), 4 writers on A, 2 readers on each of B and C.
SHAPE = {"catalog": 20_000, "products": 5_000, "categories": 100, "writers": 4, "readers": 2}
#: Decoded-atom cache sizes of the point cells: the default (holds the working set), one below
#: the working set (a 20,000-entry Dictionary has about 20,000 tree nodes), and off.
ATOM_SIZES = {"on": 50_000, "small": 2_000, "off": 0}
QUERY_SIZES = {"on": 64 * 2**20, "off": 0}


def cells(levels, with_rqlite: bool) -> list:
    """The cells of one repetition, in run order: ``(system, read, atom, query, level)``."""
    out = [("protodb", "point", "on", "on", None), ("protodb", "point", "small", "on", None),
           ("protodb", "point", "off", "on", None),
           ("protodb", "query", "on", "on", None), ("protodb", "query", "on", "off", None)]
    if with_rqlite:
        out += [("rqlite", read, None, None, lv) for read in ("point", "query") for lv in levels]
    return out


def cell_label(system, read, atom, query, level) -> str:
    """The label of a cell in results and reports."""
    if system == "rqlite":
        return f"rqlite, {read} reads, level={level}"
    what = f"atom cache {atom}" if read == "point" else f"query cache {query}"
    return f"protodb, {read} reads, {what}"


# -- ProtoDB --------------------------------------------------------------------------------------

def protodb_cell(ctx, read: str, atom: str, query: str, seconds: float, shape: dict) -> dict:
    """One verified ProtoDB cell (module docstring); returns its figures, ``ok`` False on any failure."""
    from benchmarks.harness import child_command
    from benchmarks.cache_peer import catalog_value
    from benchmarks.support import DirObjectStore
    from proto_db import ObjectSpace, TieredStorage
    root = ctx.tempdir("cache")
    store = os.path.join(root, "store")
    ids = ["n0", "n1", "n2"]
    errs = [tempfile.TemporaryFile("w+") for _ in ids]

    def config(node):
        writer = node == "n0"
        return json.dumps({"node": node, "local": os.path.join(root, node), "store": store, "seconds": seconds,
                           "writers": shape["writers"] if writer else 0, "readers": 0 if writer else shape["readers"],
                           "catalog": shape["catalog"], "products": shape["products"],
                           "categories": shape["categories"], "read": read,
                           "atom_cache_entries": shape.get("atom_sizes", ATOM_SIZES)[atom],
                           "query_cache_bytes": QUERY_SIZES[query]})

    procs = [subprocess.Popen(child_command(PEER_SCRIPT, config(n)), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=e, text=True) for n, e in zip(ids, errs)]

    def send(proc, obj):
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()

    def recv(proc, err):
        line = proc.stdout.readline()
        if not line:
            proc.wait(10)
            err.seek(0)
            raise RuntimeError(f"cache peer exited early: {err.read()[-3000:]}")
        return json.loads(line)

    try:
        ports = [recv(p, e)["port"] for p, e in zip(procs, errs)]
        table = [[n, "127.0.0.1", port] for n, port in zip(ids, ports)]
        for p in procs:
            send(p, {"peers": table})
        for p, e in zip(procs, errs):
            if recv(p, e) != {"ready": True}:
                raise RuntimeError("peer not ready")
        for p in procs:
            send(p, {"go": True})
        for p, e in zip(procs, errs):
            if recv(p, e) != {"loaded": True}:
                raise RuntimeError("peer did not load the data")
        for p in procs:
            send(p, {"go2": True})
        results = [recv(p, e) for p, e in zip(procs, errs)]
        for p in procs:
            send(p, {"stop": True})
        for p, e in zip(procs, errs):
            if p.wait(60) != 0:
                e.seek(0)
                raise RuntimeError(f"cache peer failed: {e.read()[-3000:]}")
    finally:
        for p, e in zip(procs, errs):
            if p.poll() is None:
                p.kill()
                p.wait()
            p.stdin.close()
            p.stdout.close()
            e.close()
    a, readers = results[0], results[1:]
    # the final catalog: initial values, then each writer's last committed generation per key
    want = {k: catalog_value(k, 0) for k in range(shape["catalog"])}
    for per_thread in a["last"]:
        for k, g in per_thread.items():
            want[int(k)] = catalog_value(int(k), g)
    check = ObjectSpace(TieredStorage(os.path.join(root, "check"), DirObjectStore(store)))
    try:
        tr = check.open_database("db").new_transaction()
        cat = tr.get_root_object("catalog")
        final_ok = cat.count == len(want) and all(cat.get_at(k) == v for k, v in want.items())
        tr.abort()
    finally:
        check.close()
    commits = sum(a["counts"])
    reads = [x for r in readers for x in r["reads"]]
    lat = [us for x in reads for us in x["latencies_us"]]
    bad = sum(x["bad"] for x in reads)
    versions = [[min(x["versions"][0] for x in r["reads"]), max(x["versions"][1] for x in r["reads"])]
                if r["reads"] and all(x["versions"] for x in r["reads"]) else None for r in readers]
    advanced = all(v is not None and v[1] > v[0] for v in versions)
    errors = [r["thread_errors"] for r in results]
    window = max(r["seconds"] for r in readers)
    n_reads = len(lat)
    decodes = sum(r["window"]["decodes"] for r in readers)
    a_atoms = a["window"]["atoms"]
    ok = (final_ok and bad == 0 and n_reads > 0 and commits > 0 and advanced and not any(errors)
          and len(reads) == 2 * shape["readers"] and all(r["failures"] == 0 and r["healthy"] for r in results))
    out = {"system": "protodb", "read": read, "atom_cache": atom, "query_cache": query, "ok": ok,
           "final_catalog_ok": final_ok, "bad_reads": bad, "thread_errors": errors,
           "commits": commits, "commit_rate": commits / a["seconds"],
           "commit_p50_ms": round(percentile(a["commit_latencies"], 50) * 1000, 2) if a["commit_latencies"] else None,
           "reads": n_reads, "read_rate": n_reads / window,
           "read_p50_us": percentile(lat, 50) if lat else None, "read_p99_us": percentile(lat, 99) if lat else None,
           "versions_seen_by_reader_node": versions, "versions_advanced": advanced,
           "decodes_per_read": round(decodes / n_reads, 3) if n_reads else None,
           "decodes_by_reader_node": [r["window"]["decodes"] for r in readers],
           "new_versions_by_reader_node": [v[1] - v[0] if v else None for v in versions],
           "decodes_per_new_version": round(decodes / max(1, sum(v[1] - v[0] for v in versions if v)), 2),
           "atoms_saved_per_commit_on_A": round(a_atoms["saved"] / commits, 2) if a_atoms and commits else None,
           "atom_window_by_reader_node": [r["window"]["atoms"] for r in readers],
           "query_window_by_reader_node": [r["window"]["queries"] for r in readers],
           "atom_cache_end_by_node": [r["atom_cache_end"] for r in results],
           "replication_failures_by_peer": [r["failures"] for r in results],
           "healthy_by_peer": [r["healthy"] for r in results]}
    if read == "query":
        hits = sum(q["hits"] for q in out["query_window_by_reader_node"])
        misses = sum(q["misses"] for q in out["query_window_by_reader_node"])
        out["query_hit_rate"] = round(hits / (hits + misses), 4) if hits + misses else None
        out["query_hits"], out["query_misses"] = hits, misses
    if read == "point" and atom != "off":
        atoms = out["atom_window_by_reader_node"]
        loads = sum(x["loads"] for x in atoms)
        out["atom_loads_per_read"] = round(loads / n_reads, 3) if n_reads else None
        out["thrash_signal"] = round(sum(x["read_unloads"] for x in atoms) / loads, 3) if loads else 0.0
    if ok:
        shutil.rmtree(root, ignore_errors=True)
    else:
        kept = os.path.join(ctx.workdir, "failed-runs", os.path.basename(root))
        os.makedirs(os.path.dirname(kept), exist_ok=True)
        shutil.move(root, kept)
        out["kept_run_dir"] = kept
    return out


# -- rqlite ---------------------------------------------------------------------------------------

def _post(conn, path: str, body):
    conn.request("POST", path, body=json.dumps(body), headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    data = json.loads(resp.read().decode())
    if resp.status != 200 or "error" in data or any("error" in r for r in data.get("results", [])):
        raise RuntimeError(f"rqlite error {resp.status}: {data}")
    return data["results"]


def _await_rows(port: int, sql: str, want, timeout: float = 60.0) -> None:
    """Wait until the node at ``port`` answers ``sql`` locally (``level=none``) with ``want``."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    deadline = time.monotonic() + timeout
    try:
        while True:
            res = _post(conn, "/db/query?level=none", [[sql]])[0]
            if (res.get("values") or [[None]])[0][0] == want:
                return
            if time.monotonic() > deadline:
                raise RuntimeError(f"rqlite node on port {port} did not apply the data: {res}")
            time.sleep(0.05)
    finally:
        conn.close()


def rqlite_cell(ctx, binary: str, read: str, level: str, seconds: float, shape: dict) -> dict:
    """One verified rqlite cell (module docstring)."""
    from benchmarks.cache_peer import catalog_value, category_ids, value_ok
    from benchmarks.workloads.rqlite import RqliteCluster
    cl = RqliteCluster(binary, ctx.tempdir("rqcache"))
    n_cat, n_prod, n_cats = shape["catalog"], shape["products"], shape["categories"]
    try:
        conn = http.client.HTTPConnection("127.0.0.1", cl.http[0], timeout=30)
        cl.execute(conn, ["CREATE TABLE catalog (id INTEGER PRIMARY KEY, v TEXT)",
                          "CREATE TABLE products (id INTEGER PRIMARY KEY, cat INTEGER, name TEXT)",
                          "CREATE INDEX products_cat ON products(cat)"])
        for lo in range(0, n_cat, 1000):
            cl.execute(conn, [["INSERT INTO catalog(id, v) VALUES(?, ?)", k, catalog_value(k, 0)]
                              for k in range(lo, min(n_cat, lo + 1000))], transaction=True)
        for lo in range(0, n_prod, 1000):
            cl.execute(conn, [["INSERT INTO products(id, cat, name) VALUES(?, ?, ?)", i, i % n_cats, f"product-{i}"]
                              for i in range(lo, min(n_prod, lo + 1000))], transaction=True)
        conn.close()
        for port in cl.http:            # every node applied the data before the window (as ProtoDB)
            _await_rows(port, "SELECT COUNT(*) FROM catalog", n_cat)
            _await_rows(port, "SELECT COUNT(*) FROM products", n_prod)
        writers, readers = shape["writers"], shape["readers"]
        last = [dict() for _ in range(writers)]
        counts = [0] * writers
        r_lat, r_bad = [], []
        lock = threading.Lock()
        errors = []

        def writer(t):
            c = http.client.HTTPConnection("127.0.0.1", cl.http[0], timeout=30)
            rng = random.Random(t)
            keys = list(range(t, n_cat, writers))
            stop_at = time.monotonic() + seconds
            gen = 0
            try:
                while time.monotonic() < stop_at:
                    gen += 1
                    key = rng.choice(keys)
                    res = cl.execute(c, [["UPDATE catalog SET v = ? WHERE id = ?", catalog_value(key, gen), key]])
                    if sum(x.get("rows_affected", 0) for x in res) != 1:
                        raise RuntimeError(f"rqlite updated no row: {res}")
                    last[t][key] = gen
                    counts[t] += 1
            except BaseException as e:
                errors.append(f"writer {type(e).__name__}: {str(e)[:300]}")
                raise
            finally:
                c.close()

        def reader(r):
            node = 1 + r % 2
            c = http.client.HTTPConnection("127.0.0.1", cl.http[node], timeout=30)
            rng = random.Random(r)
            lat, bad = [], 0
            stop_at = time.monotonic() + seconds
            try:
                while time.monotonic() < stop_at:
                    t0 = time.perf_counter()
                    if read == "point":
                        key = rng.randrange(n_cat)
                        res = _post(c, f"/db/query?level={level}", [["SELECT v FROM catalog WHERE id = ?", key]])[0]
                        got = (res.get("values") or [[None]])[0][0]
                        good = value_ok(key, got)
                    else:
                        cat = rng.randrange(n_cats)
                        res = _post(c, f"/db/query?level={level}",
                                    [["SELECT id FROM products WHERE cat = ? ORDER BY id", cat]])[0]
                        got = [row[0] for row in (res.get("values") or [])]
                        good = got == category_ids(n_prod, n_cats, cat)
                    lat.append(round((time.perf_counter() - t0) * 1e6))
                    bad += not good
            except BaseException as e:
                errors.append(f"reader {type(e).__name__}: {str(e)[:300]}")
                raise
            finally:
                c.close()
                with lock:
                    r_lat.append(lat)
                    r_bad.append(bad)

        from benchmarks.support import run_threads
        fns = [lambda t=t: writer(t) for t in range(writers)] + [lambda r=r: reader(r) for r in range(2 * readers)]
        elapsed, _ = run_threads(fns)
        rows = cl.query_strong("SELECT id, v FROM catalog ORDER BY id")
    finally:
        cl.close()
    want = [[k, catalog_value(k, 0)] for k in range(n_cat)]
    for per_thread in last:
        for k, g in per_thread.items():
            want[k][1] = catalog_value(k, g)
    final_ok = [list(r) for r in rows] == want
    lat = [x for xs in r_lat for x in xs]
    bad = sum(r_bad)
    commits = sum(counts)
    ok = final_ok and bad == 0 and bool(lat) and commits > 0 and not errors and len(r_lat) == 2 * readers
    return {"system": "rqlite", "read": read, "level": level, "ok": ok, "final_catalog_ok": final_ok,
            "bad_reads": bad, "thread_errors": errors, "commits": commits, "commit_rate": commits / elapsed,
            "reads": len(lat), "read_rate": len(lat) / elapsed,
            "read_p50_us": percentile(lat, 50) if lat else None, "read_p99_us": percentile(lat, 99) if lat else None}


def run_cell(ctx, cell, seconds: float, shape: dict) -> dict:
    """Run one cell of :func:`cells`."""
    system, read, atom, query, level = cell
    if system == "protodb":
        return protodb_cell(ctx, read, atom, query, seconds, shape)
    return rqlite_cell(ctx, ctx.rqlite, read, level, seconds, shape)


@benchmark("cluster_cache",
           quick={"seconds": 0.5, "catalog": 400, "products": 200, "categories": 10, "writers": 2, "readers": 1,
                  "atom_sizes": {"on": 5_000, "small": 40, "off": 0}, "levels": ["weak"], "repeat": 1},
           full={"seconds": 3.0, **SHAPE, "levels": list(RQLITE_LEVELS), "repeat": 2})
def cluster_cache(ctx, p):
    """3 members, writers on A, readers on B and C: the cross-transaction caches under writes (R110)."""
    with_rqlite = bool(ctx.rqlite and os.access(ctx.rqlite, os.X_OK))
    plan = cells(p["levels"], with_rqlite)
    shape = {k: p[k] for k in ("catalog", "products", "categories", "writers", "readers")}
    if "atom_sizes" in p:
        shape["atom_sizes"] = p["atom_sizes"]
    result = BenchResult({"members": 3, **shape, "seconds": p["seconds"], "cells": len(plan),
                          "rqlite_levels": p["levels"] if with_rqlite else "pending (no rqlited binary)"}, None, None)
    if not with_rqlite:
        result.notes.append("rqlite cells pending: no rqlited binary (pass --rqlite PATH)")
    runs = {cell: [] for cell in plan}
    for _ in range(p["repeat"]):
        for cell in plan:                       # cells interleaved within each repetition
            runs[cell].append(run_cell(ctx, cell, p["seconds"], shape))
    checksum, expected = {}, {}
    for cell, rs in runs.items():
        label = cell_label(*cell)
        ok = all(r["ok"] for r in rs)
        result.check(f"{label}: every read right, final catalog exact, reads crossed new versions", ok,
                     "; ".join(json.dumps({k: r.get(k) for k in ("ok", "bad_reads", "final_catalog_ok", "thread_errors",
                                                               "versions_seen_by_reader_node")}) for r in rs))
        checksum[label], expected[label] = ok, True
        result.add_rate(f"{label}: reads/s", [r["read_rate"] for r in rs], "reads/s")
        result.metrics[f"{label}: runs"] = rs
    result.checksum, result.expected = checksum, expected
    return result
