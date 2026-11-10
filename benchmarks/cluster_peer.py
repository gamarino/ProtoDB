"""One TCP cluster peer process for the cluster benchmarks (``cluster_tcp``, ``rqlite_reference``).

Usage: ``python cluster_peer.py NODE_ID LOCAL_DIR STORE_DIR THREADS SECONDS BATCH DURABILITY [PAYLOAD [CPU
[READERS CATALOG]]]``

The peer runs a :class:`~proto_db.ClusterCoordinator` over a :class:`~proto_db.TcpTransport`
and a :class:`~proto_db.TieredStorage` (local tier in LOCAL_DIR, ``durability`` as given)
over a :class:`~benchmarks.support.DirObjectStore` in STORE_DIR shared by every peer.  Each
of THREADS threads commits for SECONDS seconds; a commit adds BATCH unique ids to the
``ids`` CountedSet root, written blind (merged at commit, D9).  Ids are
``node * 10**9 + thread * 10**7 + j``, so the parent can rebuild the exact expected set
from the per-thread counts.  With PAYLOAD > 0 bytes, each commit also writes one value of
exactly PAYLOAD bytes under its first id in the ``blobs`` Dictionary root (also written blind
and merged at commit), so the parent can verify one value per acknowledged commit.  With CPU >
0, each transaction body first runs :func:`cpu_work` for CPU iterations (a CPU-heavy body; its
result is checked).  THREADS may be 0: the peer then only serves as a voter and replica.

Readers (Task 9c, ruling R97): with CATALOG > 0, node ``n0`` first commits a ``catalog``
Dictionary of CATALOG entries (``k -> catalog_value(k)``) and every peer waits until it reads
the whole catalog; then READERS threads per peer read random catalog entries for SECONDS
seconds, each read in a new transaction (a snapshot of the node's last known committed root,
no lock, no coordination), while the THREADS writers commit.  Every read is checked against
``catalog_value``; the peer reports the read count, the wrong reads, every read latency and the
range of database versions the readers saw (proof that their snapshots advanced under the
writes).  With CATALOG > 0 the protocol has one more step: after ``go`` and the load, the peer
prints ``{"loaded": true}`` and starts its window on ``{"go2": true}``.

With the environment variable ``PROTODB_BENCH_BUDGET_DIR`` set, the peer records the events
of :mod:`benchmarks.commit_budget` and writes those of its commit window to
``<dir>/<NODE_ID>.json`` (Task 8.3h).

Every frame this peer's transport encodes is counted by message type (``wire``: frame bytes,
the TCP and IP headers excluded): ``replicate`` and ``records`` frames are what a committing
node sends; the rest is the protocol's control traffic.

Protocol with the parent, one JSON line each way:

1. the peer binds port 0 and prints ``{"port": P}``;
2. the parent writes ``{"peers": [[id, host, port], ...]}``;
3. the peer starts its coordinator and prints ``{"ready": true}``;
4. the parent writes ``{"go": true}`` once every peer is ready;
5. the peer commits, waits until its commits are persisted (``ObjectSpace.sync``) and prints
   its counters (per-thread commit counts, every commit latency, critical-section sums, and
   the fsync/fdatasync calls counted by :mod:`benchmarks.fsync_probe` during the commit
   window and until persisted);
6. the parent writes ``{"stop": true}`` once every peer has finished, and the peer exits.
"""
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks import fsync_probe  # noqa: E402
from benchmarks.support import DirObjectStore  # noqa: E402
from proto_db import ClusterCoordinator, ObjectSpace, TcpTransport, TieredStorage  # noqa: E402
from proto_db.errors import ProtoValidationError  # noqa: E402

COORDINATOR_KW = dict(lease_duration=6.0, request_timeout=0.25, startup_grace=0.0,
                      backoff=(0.001, 0.02))
RUN_KW = dict(retries=100_000, backoff=(0.0005, 0.01))


def _say(obj) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _hear() -> dict:
    line = sys.stdin.readline()
    if not line:
        raise SystemExit("parent closed stdin")
    return json.loads(line)


WIRE = {}
_WIRE_LOCK = threading.Lock()


def _count_frames() -> None:
    """Count the bytes of every frame the TCP transport encodes, by message type."""
    from proto_db.cluster import transport as transport_module
    encode = transport_module.encode_frame

    def counting(message):
        frame = encode(message)
        with _WIRE_LOCK:
            kind = message.get("type", "?")
            WIRE[kind] = WIRE.get(kind, 0) + len(frame)
        return frame

    transport_module.encode_frame = counting


def payload_value(first_id: int, size: int) -> bytes:
    """The deterministic value of ``size`` bytes a commit writes under ``first_id``."""
    unit = b"%d:" % first_id
    return (unit * (size // len(unit) + 1))[:size]


def catalog_value(key: int) -> str:
    """The deterministic catalog value of ``key`` (short: stored inline in its tree node)."""
    return f"v{key}:{key * 7919 % 1000003}"


def cpu_work(iterations: int) -> int:
    """A deterministic pure-Python CPU load: an LCG run ``iterations`` times."""
    x = 1
    for _ in range(iterations):
        x = (x * 1103515245 + 12345) & 0x7FFFFFFF
    return x


def main() -> None:
    """Run one peer (module docstring)."""
    node_id, local, store_dir, threads, seconds, batch, durability = sys.argv[1:8]
    payload = int(sys.argv[8]) if len(sys.argv) > 8 else 0
    cpu = int(sys.argv[9]) if len(sys.argv) > 9 else 0
    readers = int(sys.argv[10]) if len(sys.argv) > 10 else 0
    catalog = int(sys.argv[11]) if len(sys.argv) > 11 else 0
    expected_cpu = cpu_work(cpu) if cpu else None
    threads, seconds, batch = int(threads), float(seconds), int(batch)
    fsync_probe.install()
    budget_dir = os.environ.get("PROTODB_BENCH_BUDGET_DIR")
    if budget_dir:
        from benchmarks import commit_budget
        commit_budget.install()
    _count_frames()
    transport = TcpTransport("127.0.0.1", 0)
    _say({"port": transport.address[1]})
    peers = [tuple(p) for p in _hear()["peers"]]
    coordinator = ClusterCoordinator(node_id, peers, transport=transport, **COORDINATOR_KW)
    with coordinator:
        storage = TieredStorage(local, DirObjectStore(store_dir), durability=durability, ship_interval=0.05,
                                backoff=(0.001, 0.05))
        space = ObjectSpace(storage, coordinator=coordinator)
        deadline = time.monotonic() + 30
        while coordinator.stats()["rejoining"] and time.monotonic() < deadline:
            time.sleep(0.005)
        _say({"ready": True})
        _hear()
        if node_id == "n0":
            try:
                space.new_database("db")
            except ProtoValidationError:
                pass                        # already created
        db = None
        deadline = time.monotonic() + 30
        while db is None:
            try:
                db = space.open_database("db")
            except ProtoValidationError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.01)
        if catalog:
            _load_catalog(db, node_id, catalog)
            _say({"loaded": True})
            _hear()
        node = int(node_id[1:])
        counts = [0] * threads
        attempts = [0] * threads
        latencies = [[] for _ in range(threads)]
        barrier = threading.Barrier(max(1, threads))
        thread_errors = []      # the exception type that ended each worker thread that died early

        def worker(t):
            try:
                commit_loop(t)
            except BaseException as e:      # reported, not swallowed: the run then fails its check
                thread_errors.append(f"{type(e).__module__}.{type(e).__qualname__}: {str(e)[:300]}")
                raise

        def commit_loop(t):
            base = node * 10**9 + t * 10**7
            barrier.wait()
            stop_at = time.monotonic() + seconds
            j = 0
            while time.monotonic() < stop_at:
                first = base + j

                def fn(tr, first=first):
                    attempts[t] += 1
                    if cpu and cpu_work(cpu) != expected_cpu:
                        raise RuntimeError("the CPU-heavy body computed a wrong result")
                    s = tr.new_counted_set()
                    for x in range(first, first + batch):
                        s = s.add(x)
                    tr.set_root_object("ids", s)
                    if payload:
                        tr.set_root_object("blobs", tr.new_dictionary().set_at(first, payload_value(first, payload)))
                t0 = time.perf_counter()
                db.run(fn, **RUN_KW)
                latencies[t].append(time.perf_counter() - t0)
                j += batch
                counts[t] += 1

        before_syncs = sum(row[-1] for row in fsync_probe.snapshot())   # setup: kept for a strace cross-check
        fsync_probe.reset()            # count only the commit window (and the shipping after it, below)
        with _WIRE_LOCK:
            WIRE.clear()
        read_stats = [None] * readers

        def reader(r):
            try:
                read_stats[r] = read_loop(db, r, node, catalog, seconds, start)
            except BaseException as e:      # reported, not swallowed: the run then fails its check
                thread_errors.append(f"reader {type(e).__module__}.{type(e).__qualname__}: {str(e)[:300]}")
                raise

        start = time.monotonic()
        workers = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
        workers += [threading.Thread(target=reader, args=(r,)) for r in range(readers)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        elapsed = time.monotonic() - start
        if budget_dir:
            from benchmarks import commit_budget
            with open(os.path.join(budget_dir, f"{node_id}.json"), "w") as f:
                json.dump({"node": node_id, "start": start, "end": start + elapsed, "threads": threads,
                           "events": commit_budget.window(start, start + elapsed)}, f)
        window_syncs = fsync_probe.snapshot()
        window_bytes = fsync_probe.synced_bytes()
        with _WIRE_LOCK:
            wire = dict(WIRE)
        space.sync(timeout=120)
        total_syncs = fsync_probe.snapshot()
        st = coordinator.stats()
        flat = [x for lst in latencies for x in lst]
        _say({"counts": counts, "attempts": sum(attempts), "seconds": elapsed, "batch": batch,
              "latencies": [round(x, 6) for x in flat],
              "failures": st["replication_failures"], "healthy": st["healthy"],
              "critical": st["critical"], "critical_commits": st["critical_commits"],
              "lease_attempts": st["attempts"], "acquired": st["acquired"],
              "syncs_before_window": before_syncs, "syncs_window": window_syncs, "syncs_until_persisted": total_syncs, "pid": os.getpid(),
              "wire": wire, "payload": payload, "synced_bytes_window": window_bytes,
              "thread_errors": thread_errors,
              "reads": [x for x in read_stats if x is not None],
              # Task 8.3k (D55): pipelined permissions (absent before protocol 13: zeros).
              "pipeline": {k: st.get(k, 0) for k in ("permits_issued", "pipelined_permits", "proposals",
                                                     "gate_open_at_send", "gate_waits", "gate_fallbacks",
                                                     "gate_closures", "stores_sent", "replications",
                                                     "terms_abandoned")}})
        _hear()
        space.close()


def _load_catalog(db, node_id: str, catalog: int) -> None:
    """``n0`` commits the catalog; every peer then waits until its own reads see all of it."""
    if node_id == "n0":
        def fill(tr):
            d = tr.new_dictionary()
            for k in range(catalog):
                d = d.set_at(k, catalog_value(k))
            tr.set_root_object("catalog", d)
        db.run(fill, **RUN_KW)
    deadline = time.monotonic() + 120
    while True:
        tr = db.new_transaction()
        try:
            got = tr.get_root_object("catalog")
            if got is not None and got.count == catalog:
                return
        finally:
            tr.abort()
        if time.monotonic() > deadline:
            raise RuntimeError("the catalog did not become visible on this node")
        time.sleep(0.01)


def read_loop(db, r: int, node: int, catalog: int, seconds: float, start: float) -> dict:
    """One reader thread: random catalog reads, each in its own transaction, verified."""
    import random
    rng = random.Random(node * 1000 + r)
    latencies, bad, versions = [], 0, []
    stop_at = start + seconds
    steady_at = start + min(0.5, seconds / 4)
    steady_from = None
    clock = time.perf_counter
    while True:
        now = time.monotonic()
        if now >= stop_at:
            break
        if steady_from is None and now >= steady_at:
            steady_from = len(latencies)
        key = rng.randrange(catalog)
        t0 = clock()
        tr = db.new_transaction()
        value = tr.get_root_object("catalog").get_at(key)
        version = tr._db_root.version            # benchmark-only: which committed version was read
        tr.abort()
        latencies.append(round((clock() - t0) * 1e6))
        versions.append(version)
        if value != catalog_value(key):
            bad += 1
    return {"reads": len(latencies), "bad": bad, "latencies_us": latencies,
            "steady_from": len(latencies) if steady_from is None else steady_from,
            "versions": [min(versions), max(versions)] if versions else None}


if __name__ == "__main__":
    main()
