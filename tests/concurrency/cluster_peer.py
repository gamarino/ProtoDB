"""One cluster peer process for the TCP smoke and throughput tests (Tasks 8.3c and 8.3d).

Usage: ``python cluster_peer.py NODE_ID SPACE MODE THREADS AMOUNT``

``SPACE`` is a directory (a FileStorage shared by every peer) or
``tiered:LOCAL_DIR:STORE_DIR`` (a TieredStorage with this peer's local tier in LOCAL_DIR
over a :class:`~tests.support.dir_object_store.DirObjectStore` in STORE_DIR shared by
every peer).

``MODE`` is ``increment`` (each thread runs AMOUNT ``Database.run`` increments of the
``n`` root plus a blind add of a unique id to the ``ids`` CountedSet) or ``throughput``
(each thread runs blind adds for AMOUNT seconds).  Protocol with the parent, one JSON
line each way:

1. the peer binds a TCP transport to port 0 and prints ``{"port": P}``;
2. the parent writes ``{"peers": [[id, host, port], ...]}``;
3. the peer starts its coordinator, prints ``{"ready": true}``;
4. the parent writes ``{"go": true}`` once every peer is ready;
5. the peer commits, waits until its commits are persisted (``ObjectSpace.sync``) and prints
   ``{"commits": .., "acked": [..], "unknown": [..], "errors": [..], "seconds": .., "failures": ..,
   "commit_publishes": .., "critical": ..}`` (``commits``: acknowledged commits; ``acked`` and
   ``unknown``: the ids whose commit was acknowledged or ended with
   ``ProtoOutcomeUnknownError`` -- conservative, recorded, the worker goes on with its next id
   (ruling R86); ``errors``: any other exception that stopped a worker;
   ``failures``: replications without a majority; ``commit_publishes``: root writes made
   by other threads than the persister, D46;
   ``critical``: the tiered storage's critical-section counters, or ``None``);
6. the parent writes ``{"stop": true}`` once every peer has finished (a peer keeps
   granting until then), and the peer stops and exits.
"""
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from proto_db.cluster.coordinator import ClusterCoordinator  # noqa: E402
from proto_db.cluster.transport import TcpTransport  # noqa: E402
from proto_db.errors import ProtoOutcomeUnknownError, ProtoValidationError  # noqa: E402
from proto_db.storage.file import FileStorage  # noqa: E402
from proto_db.storage.tiered import TieredStorage  # noqa: E402
from proto_db.transactions import ObjectSpace  # noqa: E402
from tests.support.cluster import CasCountingStorage  # noqa: E402
from tests.support.dir_object_store import DirObjectStore  # noqa: E402


def _storage(space: str):
    if space.startswith("tiered:"):
        _, local, store = space.split(":", 2)
        return TieredStorage(local, DirObjectStore(store), durability="flush", ship_interval=0.1)
    return FileStorage(space, durability="none")


def _say(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _hear():
    line = sys.stdin.readline()
    if not line:
        raise SystemExit("parent closed stdin")
    return json.loads(line)


def main():
    node_id, space_dir, mode, threads, amount = sys.argv[1:6]
    threads = int(threads)
    transport = TcpTransport("127.0.0.1", 0)
    _say({"port": transport.address[1]})
    peers = [tuple(p) for p in _hear()["peers"]]
    coordinator = ClusterCoordinator(node_id, peers, transport=transport, lease_duration=2.0,
                                     request_timeout=0.25, startup_grace=0.0,
                                     backoff=(0.001, 0.02))
    with coordinator:
        _say({"ready": True})
        _hear()
        storage = CasCountingStorage(_storage(space_dir))
        space = ObjectSpace(storage, coordinator=coordinator)
        if "db" not in space.list_databases():
            try:
                space.new_database("db")          # a tiered space starts empty: one peer creates it
            except ProtoValidationError:
                pass                              # another peer created it first
        db = space.open_database("db")
        counts = []
        errors = []                               # each worker's exception, reported to the parent
        acked, unknown = [], []                   # ids, per outcome (ruling R86)
        lock = threading.Lock()
        prefix = int(node_id[1:]) * 1_000_000

        done_by = {}                              # commits completed, per worker

        def worker(index):
            try:
                _work(index)
            except Exception as e:                # reported to the parent, which fails the test
                with lock:
                    errors.append(f"thread {index} after {done_by.get(index, 0)} commits: "
                                  f"{type(e).__name__}: {e}")
            finally:
                with lock:
                    counts.append(done_by.get(index, 0))

        def commit(fn, uid):
            """One ``Database.run``; an unknown outcome is recorded and the worker goes on."""
            try:
                db.run(fn, retries=10_000, backoff=(0.0005, 0.01))
                outcome = acked
            except ProtoOutcomeUnknownError:
                outcome = unknown
            with lock:
                outcome.append(uid)
            return outcome is acked

        def _work(index):
            done = attempts = 0
            base = prefix + index * 100_000
            if mode == "increment":
                for i in range(int(amount)):
                    uid = base + i

                    def fn(tr, uid=uid):
                        tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                        tr.set_root_object("ids", tr.new_counted_set().add(uid))
                    if commit(fn, uid):
                        done += 1
                        done_by[index] = done
            else:
                stop_at = time.monotonic() + float(amount)
                while time.monotonic() < stop_at:
                    uid = base + attempts
                    attempts += 1
                    if commit(lambda tr, uid=uid: tr.set_root_object("ids", tr.new_counted_set().add(uid)), uid):
                        done += 1
                        done_by[index] = done

        start = time.monotonic()
        workers = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()
        seconds = time.monotonic() - start
        space.sync(timeout=60)
        stats = getattr(storage, "stats", None)
        critical = None
        if callable(stats):
            st = stats()
            critical = {k: st[k] for k in ("critical_misses", "root_writes_in_critical")}
        _say({"commits": sum(counts), "acked": acked, "unknown": unknown, "errors": errors, "seconds": seconds, "failures": coordinator.stats()["replication_failures"],
              "commit_publishes": sum(1 for name in storage.publishers if not name.startswith("proto-persister-")),
              "critical": critical, "breakdown": coordinator.stats()["critical"],
              "critical_commits": coordinator.stats()["critical_commits"]})
        _hear()
        space.close()


if __name__ == "__main__":
    main()
