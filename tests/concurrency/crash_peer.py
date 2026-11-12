"""One cluster peer process for the crash test (8.3d review M7): killed with SIGKILL, restarted.

Usage: ``python crash_peer.py NODE_ID LOCAL_DIR STORE_DIR MODE``

The peer runs a TieredStorage with ``durability="fsync"`` in LOCAL_DIR over a
:class:`~tests.support.dir_object_store.DirObjectStore` in STORE_DIR, and a coordinator with
the default (safe) startup grace.  Protocol with the parent, one JSON line each way:

1. the peer binds a TCP transport to port 0 and prints ``{"port": P}``;
2. the parent writes ``{"peers": [[id, host, port], ...]}``; the peer starts and prints
   ``{"ready": true}``; the parent writes ``{"go": true}``.
3. ``MODE`` ``load``: the peer commits forever (each commit increments ``n`` and adds a
   unique id to the ``ids`` CountedSet).  It prints ``{"attempt": id}`` before each commit, then
   ``{"acked": id, "owner": ..}`` after a commit that returned (``owner``: the member it believes
   owns the current term, D50), ``{"unknown": id}`` after an unknown outcome and
   ``{"failed": id}`` after a conflict or a timeout (not applied).  It never stops by itself:
   the parent kills it, so its last attempt may have no outcome (in flight).
   ``MODE`` ``verify``: the peer reads ``n`` and ``ids`` and prints ``{"n": .., "ids": [..]}``,
   waits until the persisted root covers what it read, prints ``{"synced": true}``, then
   waits for ``{"stop": true}`` (it keeps granting until then) and exits cleanly.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from proto_db.cluster.coordinator import ClusterCoordinator  # noqa: E402
from proto_db.cluster.transport import TcpTransport  # noqa: E402
from proto_db.errors import (ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError,  # noqa: E402
                             ProtoValidationError)
from proto_db.storage.tiered import TieredStorage  # noqa: E402
from proto_db.transactions import ObjectSpace  # noqa: E402
from tests.support import stress  # noqa: E402
from tests.support.dir_object_store import DirObjectStore  # noqa: E402

# No startup_grace: the default, lease_duration + drift_margin, is the shortest a restart may
# use (ruling R51); the peers restart on their logs.
LEASE = dict(lease_duration=1.0, request_timeout=0.25,
             backoff=(0.001, 0.02), persist_interval=0.05, persist_lag=0.5)


def _say(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _hear():
    line = sys.stdin.readline()
    if not line:
        raise SystemExit("parent closed stdin")
    return json.loads(line)


def _open_db(space):
    while True:
        try:
            if "db" not in space.list_databases():
                space.new_database("db")
            return space.open_database("db")
        except (ProtoValidationError, ProtoConflictError, ProtoTimeoutError, ProtoOutcomeUnknownError):
            time.sleep(0.05)           # another peer created it, or no majority yet (startup grace)


def main():
    node_id, local, store_dir, mode = sys.argv[1:5]
    transport = TcpTransport("127.0.0.1", 0)
    _say({"port": transport.address[1]})
    peers = [tuple(p) for p in _hear()["peers"]]
    coordinator = ClusterCoordinator(node_id, peers, transport=transport, **LEASE)
    with coordinator:
        _say({"ready": True})
        _hear()
        storage = TieredStorage(local, DirObjectStore(store_dir), durability="fsync", ship_interval=0.1)
        # The verifying incarnation reads and syncs within the stress budget (R142; quiet: the same 10 s).
        space = ObjectSpace(storage, coordinator=coordinator,
                            lock_timeout=10.0 if mode == "load" else stress.budget(10.0))
        db = _open_db(space)
        if mode == "load":
            uid = int(node_id[1:]) * 1_000_000
            while True:
                uid += 1

                def fn(tr, uid=uid):
                    tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                    tr.set_root_object("ids", tr.new_counted_set().add(uid))
                _say({"attempt": uid})       # a kill before its outcome leaves it in flight
                try:
                    db.run(fn, retries=10_000, backoff=(0.0005, 0.01))
                except ProtoOutcomeUnknownError:
                    _say({"unknown": uid})
                    continue
                except (ProtoConflictError, ProtoTimeoutError):
                    _say({"failed": uid})        # not applied
                    continue
                _say({"acked": uid, "owner": coordinator.owner})
        tr = db.new_transaction()
        n, ids = tr.get_root_object("n"), tr.get_root_object("ids")
        tr.abort()
        _say({"n": n, "ids": sorted(ids.elements()) if ids is not None else []})
        space.sync(timeout=stress.budget(60))
        _say({"synced": True})
        _hear()
        space.close()


if __name__ == "__main__":
    main()
