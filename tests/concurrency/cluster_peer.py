"""One cluster peer process for the TCP smoke and throughput tests (Task 8.3c).

Usage: ``python cluster_peer.py NODE_ID SPACE_DIR MODE THREADS AMOUNT``

``MODE`` is ``increment`` (each thread runs AMOUNT ``Database.run`` increments of the
``n`` root plus a blind add of a unique id to the ``ids`` CountedSet) or ``throughput``
(each thread runs blind adds for AMOUNT seconds).  Protocol with the parent, one JSON
line each way:

1. the peer binds a TCP transport to port 0 and prints ``{"port": P}``;
2. the parent writes ``{"peers": [[id, host, port], ...]}``;
3. the peer starts its coordinator, prints ``{"ready": true}``;
4. the parent writes ``{"go": true}`` once every peer is ready;
5. the peer commits, prints ``{"commits": .., "seconds": .., "fencing": .., "failed_cas": ..}``;
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
from proto_db.storage.file import FileStorage  # noqa: E402
from proto_db.transactions import ObjectSpace  # noqa: E402
from tests.support.cluster import CasCountingStorage  # noqa: E402


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
                                     drift_margin=0.2, request_timeout=0.25, startup_grace=0.0,
                                     backoff=(0.001, 0.02))
    with coordinator:
        _say({"ready": True})
        _hear()
        storage = CasCountingStorage(FileStorage(space_dir, durability="none"))
        db = ObjectSpace(storage, coordinator=coordinator).open_database("db")
        counts = []
        lock = threading.Lock()
        prefix = int(node_id[1:]) * 1_000_000

        def worker(index):
            done = 0
            base = prefix + index * 100_000
            if mode == "increment":
                for i in range(int(amount)):
                    uid = base + i

                    def fn(tr, uid=uid):
                        tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                        tr.set_root_object("ids", tr.new_counted_set().add(uid))
                    db.run(fn, retries=10_000, backoff=(0.0005, 0.01))
                    done += 1
            else:
                stop_at = time.monotonic() + float(amount)
                while time.monotonic() < stop_at:
                    uid = base + done
                    db.run(lambda tr, uid=uid: tr.set_root_object("ids", tr.new_counted_set().add(uid)),
                           retries=10_000, backoff=(0.0005, 0.01))
                    done += 1
            with lock:
                counts.append(done)

        start = time.monotonic()
        workers = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()
        seconds = time.monotonic() - start
        _say({"commits": sum(counts), "seconds": seconds,
              "fencing": coordinator.stats()["fencing_events"], "failed_cas": storage.failed_cas})
        _hear()
        storage.close()


if __name__ == "__main__":
    main()
