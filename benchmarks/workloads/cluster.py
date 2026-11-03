"""Cluster mode (D44-D46): CAS-only baseline, peer scaling, store-latency independence, TCP peers.

References (rulings R37, R41, R45): SQLite is not a cluster reference.  Cluster mode is
compared with

* the CAS-only baseline: writers arbitrated by the object store's conditional root write
  (the reverted D5 approach): :class:`~proto_db.CloudStorage` without a coordinator, every
  commit uploads its segment and compare-and-swaps the root object on the commit path;
* itself with 1, 2, 4 and 8 peers, in one process (:class:`InProcCluster`) and as TCP
  subprocess peers (:mod:`benchmarks.cluster_peer`);
* itself with 0 and 30 ms of injected object-store latency: persistence is asynchronous,
  so the commit rate must not be bounded by the store;
* rqlite (:mod:`benchmarks.workloads.rqlite`).

Commits are made under ``durability="fsync"`` on every node's local tier (D46: the commit
point is the local WAL fsync plus majority-acknowledged replication).  Every run checks
that the final state holds exactly the acknowledged commits (every id once), that no
replication failed and that every node stayed healthy; the critical-section breakdown
(validation, intent replay, path copy, replication; ruling R41) is reported per commit.
"""
import itertools
import os
import threading
import time

from benchmarks.harness import BenchResult, benchmark, summarize
from benchmarks.support import MemoryObjectStore

COORDINATOR_KW = dict(lease_duration=2.0, drift_margin=0.2, request_timeout=0.25, startup_grace=0.0,
                      backoff=(0.001, 0.02))
RUN_KW = dict(retries=100_000, backoff=(0.0005, 0.01))
CRITICAL_KEYS = ("validation", "replay", "path_copy", "replication")


class InProcCluster:
    """``n`` coordinators on one in-process network, each with a TieredStorage and a space."""

    def __init__(self, workdir: str, n: int, store, *, durability: str = "fsync"):
        from proto_db import ClusterCoordinator, ObjectSpace, TieredStorage
        from proto_db.cluster.transport import InProcessNetwork
        self.net = InProcessNetwork(seed=0)
        self.ids = [f"n{i}" for i in range(n)]
        peers = [(i, "127.0.0.1", 0) for i in self.ids]
        self.nodes, self.spaces = [], []
        try:
            for i, node_id in enumerate(self.ids):
                node = ClusterCoordinator(node_id, peers, transport=self.net.endpoint(), seed=i, **COORDINATOR_KW)
                node.start()
                self.nodes.append(node)
            for node_id, node in zip(self.ids, self.nodes):
                storage = TieredStorage(os.path.join(workdir, node_id), store, durability=durability,
                                        ship_interval=0.05, backoff=(0.001, 0.05))
                self.spaces.append(ObjectSpace(storage, coordinator=node))
            deadline = time.monotonic() + 30
            while any(node.stats()["rejoining"] for node in self.nodes):
                if time.monotonic() > deadline:
                    raise RuntimeError("cluster members did not finish their first rejoin")
                time.sleep(0.005)
            self.spaces[0].new_database("db")
            self.dbs = [s.open_database("db") for s in self.spaces]
        except BaseException:
            self.close()
            raise

    def settle(self, timeout: float = 30.0) -> None:
        """Wait until every node handled its messages and knows the same committed root."""
        deadline = time.monotonic() + timeout
        while True:
            idle = all(node.wait_idle(timeout=0.0) for node in self.nodes)
            roots = {node.committed_root()[:3] for node in self.nodes}
            if idle and len(roots) == 1:
                return
            if time.monotonic() > deadline:
                raise RuntimeError(f"the cluster did not settle: {roots}")
            time.sleep(0.005)

    def read(self, name: str):
        """Root ``name`` read on node 0 by a new transaction."""
        tr = self.dbs[0].new_transaction()
        try:
            return tr.get_root_object(name)
        finally:
            tr.abort()

    def close(self) -> None:
        """Close every space, then stop every coordinator and the network."""
        for space in self.spaces:
            space.close()
        for node in self.nodes:
            node.stop()
        self.net.close()


def _drive(dbs, threads_per_db: int, seconds: float, mode: str, batch: int = 1):
    """Commit on every database from ``threads_per_db`` threads for ``seconds``.

    ``mode`` ``blind``: each commit adds ``batch`` unique ids to the ``ids`` CountedSet
    (merged, never a conflict); ``increment``: each commit also reads and increments the
    counter ``n`` (conflicting commits retry).  Returns ``(elapsed, committed_ids,
    attempts, latencies)``.
    """
    from benchmarks.support import run_threads
    ids = itertools.count()
    committed, latencies, attempts = [], [], []
    lock = threading.Lock()

    def worker_for(db):
        def worker():
            mine, lat, tries = [], [], [0]
            stop_at = time.monotonic() + seconds
            while time.monotonic() < stop_at:
                batch_ids = [next(ids) for _ in range(batch)]

                def fn(tr, batch_ids=batch_ids):
                    tries[0] += 1
                    if mode == "increment":
                        tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                    s = tr.new_counted_set()
                    for x in batch_ids:
                        s = s.add(x)
                    tr.set_root_object("ids", s)
                t0 = time.perf_counter()
                db.run(fn, **RUN_KW)
                lat.append(time.perf_counter() - t0)
                mine.extend(batch_ids)
            with lock:
                committed.extend(mine)
                latencies.extend(lat)
                attempts.append(tries[0])
        return worker

    elapsed, _ = run_threads([worker_for(db) for db in dbs for _ in range(threads_per_db)])
    return elapsed, committed, sum(attempts), latencies


def _latency_ms(latencies) -> tuple:
    """``(p50, p95)`` of ``latencies`` (seconds) in milliseconds."""
    if not latencies:
        return (0.0, 0.0)
    s = summarize(latencies)
    return (round(s["median"] * 1000, 2), round(s["p95"] * 1000, 2))


def _critical_per_commit(nodes_stats) -> dict:
    n = sum(st["critical_commits"] for st in nodes_stats) or 1
    return {k: round(sum(st["critical"][k] for st in nodes_stats) / n * 1000, 3) for k in CRITICAL_KEYS}


def _cluster_run(ctx, n, store, mode, threads, seconds, batch=1, durability="fsync"):
    """One in-process cluster run, verified; returns a dict of figures."""
    cl = InProcCluster(ctx.tempdir(f"cluster{n}"), n, store, durability=durability)
    try:
        elapsed, committed, attempts, latencies = _drive(cl.dbs, threads, seconds, mode, batch)
        stats = [node.stats() for node in cl.nodes]
        cl.settle()
        ids = cl.read("ids")
        n_root = cl.read("n")
        ok_ids = sorted(ids.elements()) == sorted(committed) and all(c == 1 for _, c in ids.items())
        commits = len(committed) // batch
        ok_n = mode != "increment" or n_root == commits
        cl.spaces[0].sync(timeout=120)
        persisted = cl.spaces[0].persisted_epoch >= cl.spaces[0].committed_epoch
    finally:
        cl.close()
    lat = summarize(latencies) if latencies else {"median": 0.0, "p95": 0.0}
    return {"commits": commits, "elapsed": elapsed, "rate": commits / elapsed, "attempts": attempts,
            "ok": ok_ids and ok_n and persisted, "detail": f"ids {ok_ids}, counter {ok_n}, persisted {persisted}",
            "failures": sum(st["replication_failures"] for st in stats),
            "healthy": all(st["healthy"] for st in stats),
            "lease_attempts_per_commit": round(sum(st["attempts"] for st in stats) / max(1, commits), 2),
            "critical_ms": _critical_per_commit(stats),
            "latency_ms": (round(lat["median"] * 1000, 2), round(lat["p95"] * 1000, 2))}


@benchmark("cluster_cas_baseline", quick={"latencies": [0.0, 0.030], "writers": [1, 2], "seconds": 0.3},
           full={"latencies": [0.0, 0.030], "writers": [1, 4], "seconds": 3.0})
def cluster_cas_baseline(ctx, p):
    """CAS-only baseline (reverted D5): CloudStorage writers arbitrated by the store's conditional root write."""
    from proto_db import CloudStorage, ObjectSpace
    from benchmarks.support import run_threads
    result = BenchResult({"writers": p["writers"], "store_latency_s": p["latencies"], "seconds": p["seconds"],
                          "commit": "read and increment counter n (one root)"}, None, None)
    checksum, expected = {}, {}
    for latency in p["latencies"]:
        for writers in p["writers"]:
            store = MemoryObjectStore(latency=latency)
            spaces = [ObjectSpace(CloudStorage(store, backoff=(0.0, 0.001))) for _ in range(writers)]
            spaces[0].new_database("db")
            dbs = [s.open_database("db") for s in spaces]
            counts = [0] * writers
            lat = []
            lock = threading.Lock()

            def worker_for(i):
                def worker():
                    stop_at = time.monotonic() + p["seconds"]
                    mine = []
                    while time.monotonic() < stop_at:
                        t0 = time.perf_counter()
                        dbs[i].run(lambda tr: tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1), **RUN_KW)
                        mine.append(time.perf_counter() - t0)
                        counts[i] += 1
                    with lock:
                        lat.extend(mine)
                return worker

            elapsed, _ = run_threads([worker_for(i) for i in range(writers)])
            reader = ObjectSpace(CloudStorage(store))
            tr = reader.open_database("db").new_transaction()
            final = tr.get_root_object("n")
            tr.abort()
            reader.close()
            for s in spaces:
                s.close()
            key = f"latency {latency * 1000:.0f} ms, {writers} writer(s)"
            result.check(f"{key}: counter equals commits", final == sum(counts), f"{final} != {sum(counts)}")
            checksum[key], expected[key] = final, sum(counts)
            result.add_rate(f"CAS-only commits/s, {key}", [sum(counts) / elapsed], "commits/s")
            s = summarize(lat)
            result.metrics[f"{key}: commit latency ms p50/p95"] = (round(s["median"] * 1000, 2),
                                                                     round(s["p95"] * 1000, 2))
            result.metrics[f"{key}: failed root CAS per commit"] = round(store.failed_preconditions /
                                                                          max(1, sum(counts)), 2)
    result.checksum, result.expected = checksum, expected
    result.notes.append("each writer is its own ObjectSpace/CloudStorage instance (as separate processes "
                        "would be); a commit's root CAS is on the commit path, so the store latency bounds it")
    return result


@benchmark("cluster_peers", quick={"peers": [1, 2], "threads": 2, "seconds": 0.3, "modes": ["blind"], "repeat": 1},
           full={"peers": [1, 2, 4, 8], "threads": 4, "seconds": 2.0, "modes": ["blind", "increment"], "repeat": 3})
def cluster_peers(ctx, p):
    """In-process cluster: commits/s, latency and critical-section breakdown with 1/2/4/8 peers."""
    result = BenchResult({"peers": p["peers"], "threads_per_peer": p["threads"], "seconds": p["seconds"],
                          "modes": p["modes"], "durability": "fsync (local tier)", "store_latency_s": 0.0},
                         None, None)
    checksum, expected = {}, {}
    for mode in p["modes"]:
        for peers in p["peers"]:
            runs = [_cluster_run(ctx, peers, MemoryObjectStore(), mode, p["threads"], p["seconds"])
                    for _ in range(p["repeat"])]
            key = f"{mode}, {peers} peer(s)"
            ok = all(r["ok"] and r["failures"] == 0 and r["healthy"] for r in runs)
            result.check(f"{key}: exact final state, no failed replication, healthy", ok,
                         "; ".join(r["detail"] for r in runs))
            checksum[key], expected[key] = ok, True
            result.add_rate(f"{mode}: commits/s, {peers} peer(s) x {p['threads']} threads",
                            [r["rate"] for r in runs], "commits/s")
            mid = sorted(runs, key=lambda r: r["rate"])[len(runs) // 2]
            result.metrics[f"{key}: commit latency ms p50/p95"] = mid["latency_ms"]
            result.metrics[f"{key}: attempts per commit"] = round(mid["attempts"] / max(1, mid["commits"]), 2)
            result.metrics[f"{key}: lease acquisitions tried per commit"] = mid["lease_attempts_per_commit"]
            result.metrics[f"{key}: critical section ms per commit"] = mid["critical_ms"]
    result.checksum, result.expected = checksum, expected
    return result


@benchmark("cluster_store_latency",
           quick={"latencies": [0.0, 0.030], "peers": 2, "threads": 2, "seconds": 0.3, "repeat": 1},
           full={"latencies": [0.0, 0.030], "peers": 3, "threads": 4, "seconds": 2.0, "repeat": 3})
def cluster_store_latency(ctx, p):
    """Storage-latency independence: the same 3-peer cluster with 0 and 30 ms object-store latency."""
    result = BenchResult({"peers": p["peers"], "threads_per_peer": p["threads"], "store_latency_s": p["latencies"],
                          "seconds": p["seconds"], "mode": "blind"}, None, None)
    checksum, expected, rates = {}, {}, {}
    for latency in p["latencies"]:
        runs = [_cluster_run(ctx, p["peers"], MemoryObjectStore(latency=latency), "blind", p["threads"],
                             p["seconds"]) for _ in range(p["repeat"])]
        key = f"store latency {latency * 1000:.0f} ms"
        ok = all(r["ok"] and r["failures"] == 0 and r["healthy"] for r in runs)
        result.check(f"{key}: exact final state, persisted, healthy", ok, "; ".join(r["detail"] for r in runs))
        checksum[key], expected[key] = ok, True
        rates[latency] = result.add_rate(f"commits/s, {key}", [r["rate"] for r in runs], "commits/s")["median"]
        mid = sorted(runs, key=lambda r: r["rate"])[len(runs) // 2]
        result.metrics[f"{key}: commit latency ms p50/p95"] = mid["latency_ms"]
        result.metrics[f"{key}: critical section ms per commit"] = mid["critical_ms"]
    lo, hi = min(rates), max(rates)
    result.metrics["rate ratio (slow store / fast store)"] = round(rates[hi] / rates[lo], 2)
    result.checksum, result.expected = checksum, expected
    return result


# -- TCP subprocess peers ----------------------------------------------------------------------

PEER_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cluster_peer.py")


def run_tcp_cluster(ctx, peers: int, threads: int, seconds: float, batch: int = 1, durability: str = "fsync") -> dict:
    """Run ``peers`` subprocess peers over TCP and verify the persisted result; return figures."""
    import json
    import subprocess
    import tempfile

    from benchmarks.harness import child_command
    from benchmarks.support import DirObjectStore
    root = ctx.tempdir(f"tcp{peers}")
    store_dir = os.path.join(root, "store")
    ids = [f"n{i}" for i in range(peers)]
    errs = [tempfile.TemporaryFile("w+") for _ in ids]
    procs = [subprocess.Popen(child_command(PEER_SCRIPT, node_id, os.path.join(root, node_id), store_dir,
                                            str(threads), str(seconds), str(batch), durability),
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, text=True)
             for node_id, err in zip(ids, errs)]

    def send(proc, obj):
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()

    def recv(proc, err):
        line = proc.stdout.readline()
        if not line:
            proc.wait(10)
            err.seek(0)
            raise RuntimeError(f"cluster peer exited early: {err.read()[-3000:]}")
        return json.loads(line)

    try:
        ports = [recv(pr, e)["port"] for pr, e in zip(procs, errs)]
        table = [[node_id, "127.0.0.1", port] for node_id, port in zip(ids, ports)]
        for pr in procs:
            send(pr, {"peers": table})
        for pr, e in zip(procs, errs):
            if recv(pr, e) != {"ready": True}:
                raise RuntimeError("peer not ready")
        for pr in procs:
            send(pr, {"go": True})
        results = [recv(pr, e) for pr, e in zip(procs, errs)]
        for pr in procs:
            send(pr, {"stop": True})
        for pr, e in zip(procs, errs):
            if pr.wait(60) != 0:
                e.seek(0)
                raise RuntimeError(f"cluster peer failed: {e.read()[-3000:]}")
    finally:
        for pr, e in zip(procs, errs):
            if pr.poll() is None:
                pr.kill()
                pr.wait()
            pr.stdin.close()
            pr.stdout.close()
            e.close()
    expected_ids = sorted(node * 10**9 + t * 10**7 + j
                          for node, r in enumerate(results) for t, c in enumerate(r["counts"])
                          for j in range(c * batch))
    from proto_db import ObjectSpace, TieredStorage
    reader = ObjectSpace(TieredStorage(os.path.join(root, "reader"), DirObjectStore(store_dir)))
    try:
        tr = reader.open_database("db").new_transaction()
        got = tr.get_root_object("ids")
        ok = got is not None and sorted(got.elements()) == expected_ids and all(c == 1 for _, c in got.items())
        tr.abort()
    finally:
        reader.close()
    commits = sum(sum(r["counts"]) for r in results)
    elapsed = max(r["seconds"] for r in results)
    stats = [{"critical": r["critical"], "critical_commits": r["critical_commits"]} for r in results]
    return {"commits": commits, "rows": commits * batch, "rate": commits / elapsed,
            "row_rate": commits * batch / elapsed,
            "ok": ok and all(r["failures"] == 0 and r["healthy"] for r in results),
            "latency_ms": _latency_ms([x for r in results for x in r["latencies"]]),
            "latency_ms_by_peer": [_latency_ms(r["latencies"]) for r in results],
            "lease_attempts_per_commit": round(sum(r["lease_attempts"] for r in results) / max(1, commits), 2),
            "critical_ms": _critical_per_commit(stats), "persisted_ids": len(expected_ids)}


@benchmark("cluster_tcp", quick={"peers": [2], "threads": 2, "seconds": 0.5, "repeat": 1},
           full={"peers": [1, 2, 4, 8], "threads": 4, "seconds": 3.0, "repeat": 2})
def cluster_tcp(ctx, p):
    """TCP subprocess peers (1/2/4/8, one process each): commits/s, latency, critical section."""
    result = BenchResult({"peers": p["peers"], "threads_per_peer": p["threads"], "seconds": p["seconds"],
                          "durability": "fsync (local tier)", "commit": "blind add of one id"}, None, None)
    checksum, expected = {}, {}
    for peers in p["peers"]:
        runs = [run_tcp_cluster(ctx, peers, p["threads"], p["seconds"]) for _ in range(p["repeat"])]
        key = f"{peers} peer(s)"
        ok = all(r["ok"] for r in runs)
        result.check(f"{key}: persisted state holds exactly the acknowledged ids", ok, "")
        checksum[key], expected[key] = ok, True
        result.add_rate(f"commits/s, {peers} TCP peer(s) x {p['threads']} threads", [r["rate"] for r in runs],
                        "commits/s")
        mid = sorted(runs, key=lambda r: r["rate"])[len(runs) // 2]
        result.metrics[f"{key}: commit latency ms p50/p95 (all peers)"] = mid["latency_ms"]
        result.metrics[f"{key}: commit latency ms p50/p95 per peer"] = mid["latency_ms_by_peer"]
        result.metrics[f"{key}: lease acquisitions tried per commit"] = mid["lease_attempts_per_commit"]
        result.metrics[f"{key}: critical section ms per commit"] = mid["critical_ms"]
    result.checksum, result.expected = checksum, expected
    return result
