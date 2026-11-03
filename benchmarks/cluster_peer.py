"""One TCP cluster peer process for the cluster benchmarks (``cluster_tcp``, ``rqlite_reference``).

Usage: ``python cluster_peer.py NODE_ID LOCAL_DIR STORE_DIR THREADS SECONDS BATCH DURABILITY [PAYLOAD]``

The peer runs a :class:`~proto_db.ClusterCoordinator` over a :class:`~proto_db.TcpTransport`
and a :class:`~proto_db.TieredStorage` (local tier in LOCAL_DIR, ``durability`` as given)
over a :class:`~benchmarks.support.DirObjectStore` in STORE_DIR shared by every peer.  Each
of THREADS threads commits for SECONDS seconds; a commit adds BATCH unique ids to the
``ids`` CountedSet root, written blind (merged at commit, D9).  Ids are
``node * 10**9 + thread * 10**7 + j``, so the parent can rebuild the exact expected set
from the per-thread counts.  With PAYLOAD > 0 bytes, each commit also writes one value of
exactly PAYLOAD bytes under its first id in the ``blobs`` Dictionary root (also written blind
and merged at commit), so the parent can verify one value per acknowledged commit.

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

COORDINATOR_KW = dict(lease_duration=2.0, drift_margin=0.2, request_timeout=0.25, startup_grace=0.0,
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


def main() -> None:
    """Run one peer (module docstring)."""
    node_id, local, store_dir, threads, seconds, batch, durability = sys.argv[1:8]
    payload = int(sys.argv[8]) if len(sys.argv) > 8 else 0
    threads, seconds, batch = int(threads), float(seconds), int(batch)
    fsync_probe.install()
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
        node = int(node_id[1:])
        counts = [0] * threads
        attempts = [0] * threads
        latencies = [[] for _ in range(threads)]
        barrier = threading.Barrier(threads)

        def worker(t):
            base = node * 10**9 + t * 10**7
            barrier.wait()
            stop_at = time.monotonic() + seconds
            j = 0
            while time.monotonic() < stop_at:
                first = base + j

                def fn(tr, first=first):
                    attempts[t] += 1
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
        start = time.monotonic()
        workers = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        elapsed = time.monotonic() - start
        window_syncs = fsync_probe.snapshot()
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
              "wire": wire, "payload": payload})
        _hear()
        space.close()


if __name__ == "__main__":
    main()
