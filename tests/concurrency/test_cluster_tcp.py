"""Real TCP cluster: three subprocess peers on localhost (Tasks 8.3c and 8.3d, D44-D46).

Each peer (``cluster_peer.py``) binds its transport to port 0 and reports the port; the
parent sends every peer the full peer list, then starts them together.  The peers share
one FileStorage, or each has a local tier (TieredStorage) over one shared directory
object store.  The smoke tests check the final persisted count of concurrent increments
is exact, every committed id appears once and no committing thread wrote a root (only
the persisters did, D46).  The throughput tests print commits per second through the TCP
transport (informational, for the Phase 9 record).
"""
import json
import os
import subprocess
import sys
import tempfile

import pytest

from proto_db.storage.file import FileStorage
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.support.dir_object_store import DirObjectStore

PEER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cluster_peer.py")
PEERS = 3

pytestmark = [pytest.mark.timeout(180)]


def _send(proc, obj):
    proc.stdin.write(json.dumps(obj) + "\n")
    proc.stdin.flush()


def _errors(err):
    err.seek(0)
    return err.read()


def _recv(proc, err):
    line = proc.stdout.readline()
    if not line:
        proc.wait(10)
        raise AssertionError(f"peer exited early: {_errors(err)}")
    return json.loads(line)


def _space_arg(space_dir, node_id):
    """The peer's SPACE argument: the shared directory, or its tiered layout."""
    if isinstance(space_dir, tuple):
        root, store = space_dir
        return f"tiered:{os.path.join(root, node_id)}:{store}"
    return space_dir


def _run_cluster(space_dir, mode, threads, amount):
    if not isinstance(space_dir, tuple):
        space = ObjectSpace(FileStorage(space_dir, durability="none"))
        space.new_database("db")            # the tiered peers create it themselves (n0)
        space.close()
    ids = [f"n{i}" for i in range(PEERS)]
    errs = [tempfile.TemporaryFile("w+") for _ in ids]    # files: a chatty peer never blocks on stderr
    procs = [subprocess.Popen([sys.executable, PEER_SCRIPT, node_id, _space_arg(space_dir, node_id), mode,
                               str(threads), str(amount)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, text=True)
             for node_id, err in zip(ids, errs)]
    pairs = list(zip(procs, errs))
    try:
        ports = [_recv(p, e)["port"] for p, e in pairs]
        peers = [[node_id, "127.0.0.1", port] for node_id, port in zip(ids, ports)]
        for p in procs:
            _send(p, {"peers": peers})
        for p, e in pairs:
            assert _recv(p, e) == {"ready": True}
        for p in procs:
            _send(p, {"go": True})
        results = [_recv(p, e) for p, e in pairs]
        for p in procs:
            _send(p, {"stop": True})
        for p, e in pairs:
            if p.wait(30) != 0:
                raise AssertionError(_errors(e))
    finally:
        for p, e in pairs:
            if p.poll() is None:
                p.kill()
                p.wait()
            p.stdin.close()
            p.stdout.close()
            e.close()
    return results


def _final(space_dir):
    if isinstance(space_dir, tuple):
        reader = TieredStorage(os.path.join(space_dir[0], "reader"), DirObjectStore(space_dir[1]))
    else:
        reader = FileStorage(space_dir, durability="none")
    space = ObjectSpace(reader)
    try:
        tr = space.open_database("db").new_transaction()
        n, ids = tr.get_root_object("n"), tr.get_root_object("ids")
        tr.abort()
        return n, dict(ids.items())
    finally:
        space.close()


def _spaces(tmp_path):
    return {"file": str(tmp_path / "space"), "tiered": (str(tmp_path / "local"), str(tmp_path / "store"))}


@pytest.mark.parametrize("backend", ["file", "tiered"])
def test_three_tcp_peers_commit_concurrently(tmp_path, backend):
    space_dir = _spaces(tmp_path)[backend]
    threads, per_thread = 2, 25
    results = _run_cluster(space_dir, "increment", threads, per_thread)
    total = PEERS * threads * per_thread
    assert sum(r["commits"] for r in results) == total
    n, ids = _final(space_dir)
    assert n == total
    assert len(ids) == total and all(c == 1 for c in ids.values())
    assert sum(r["commit_publishes"] for r in results) == 0
    assert sum(r["failures"] for r in results) == 0
    if backend == "tiered":
        assert all(r["critical"]["root_writes_in_critical"] == 0 for r in results)


@pytest.mark.parametrize("backend", ["file", "tiered"])
def test_tcp_throughput_three_peers(tmp_path, capsys, backend):
    space_dir = _spaces(tmp_path)[backend]
    results = _run_cluster(space_dir, "throughput", 4, 2.0)
    commits = sum(r["commits"] for r in results)
    seconds = max(r["seconds"] for r in results)
    _, ids = _final(space_dir)
    assert len(ids) == commits and all(c == 1 for c in ids.values())
    assert sum(r["commit_publishes"] for r in results) == 0
    n = sum(r["critical_commits"] for r in results) or 1
    breakdown = {k: sum(r["breakdown"][k] for r in results) / n * 1000
                 for k in ("validation", "replay", "path_copy", "replication")}
    with capsys.disabled():
        print(f"\nTHROUGHPUT TCP [{backend}] 3 subprocess peers x 4 threads: {commits / seconds:.0f} commits/s "
              f"({commits} commits in {seconds:.2f}s); critical section ms/commit: "
              + ", ".join(f"{k}={v:.3f}" for k, v in breakdown.items()))


# -- one unreachable member must not stall the others (fix round 1, R39) ----------------------

def _black_hole():
    """A listening socket whose accept backlog is full: connects to it hang until their timeout."""
    import socket
    hole = socket.socket()
    hole.bind(("127.0.0.1", 0))
    hole.listen(0)
    fillers = []
    for _ in range(4):
        c = socket.socket()
        c.settimeout(0.2)
        try:
            c.connect(hole.getsockname())
        except socket.timeout:
            pass                  # a timed-out filler still occupies a SYN slot
        fillers.append(c)
    return hole, fillers


def _in_process_tcp_rate(black_holed: bool, seconds: float = 1.5) -> float:
    import itertools
    import threading
    import time

    from proto_db.cluster.coordinator import ClusterCoordinator
    from proto_db.cluster.transport import TcpTransport
    from proto_db.storage.memory import MemoryStorage

    transports = [TcpTransport() for _ in range(3)]
    hole = fillers = None
    addresses = [t.address for t in transports]
    if black_holed:
        transports[2].stop()                   # never started: n2 is unreachable
        hole, fillers = _black_hole()
        addresses[2] = hole.getsockname()
    peers = [(f"n{i}", host, port) for i, (host, port) in enumerate(addresses)]
    live = 2 if black_holed else 3
    # A restarted cluster (below): a grace of 0 is refused on a non-empty acceptor log (R50).
    grace = 0.2
    nodes = [ClusterCoordinator(f"n{i}", peers, transport=transports[i], startup_grace=grace).start()
             for i in range(live)]
    storage = MemoryStorage()
    from proto_db.cluster.rootlog import memory_log_for
    for i in range(live):
        # A cluster that ran before n2 became unreachable: non-empty acceptor logs.  A fresh
        # log would rejoin first, which needs both other members of three (D46, R47).
        memory_log_for(storage, f"n{i}").mark_rejoined()
    try:
        ObjectSpace(storage).new_database("db")
        uids = itertools.count()
        done = []

        def worker(node):
            db = ObjectSpace(storage, coordinator=node).open_database("db")
            n = 0
            while time.monotonic() < stop_at:
                uid = next(uids)
                db.run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(uid)),
                       retries=10_000, backoff=(0.0005, 0.01))
                n += 1
            done.append(n)

        threads = [threading.Thread(target=worker, args=(nodes[i % 2],)) for i in range(4)]
        time.sleep(grace)                      # the members grant nothing during their grace
        stop_at = time.monotonic() + seconds
        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start
        # Self-report: a worker that raised never appends (a share of 0 is contention, not failure).
        assert len(done) == len(threads) and sum(done) > 0, f"a worker failed or nothing committed: {done}"
        return sum(done) / elapsed
    finally:
        for node in nodes:
            node.stop()
        if hole is not None:
            for c in fillers:
                c.close()
            hole.close()


def test_an_unreachable_member_does_not_stall_the_healthy_majority(capsys):
    healthy = _in_process_tcp_rate(black_holed=False)
    degraded = _in_process_tcp_rate(black_holed=True)
    with capsys.disabled():
        print(f"\nTCP 3 members, 2 committing: all reachable {healthy:.0f} commits/s, "
              f"one black-holed {degraded:.0f} commits/s")
    # The stall this guards against was 8 against 460 commits/s.  Since D46 every commit needs
    # a majority ack: with one member of three down, each needs the other (busy) live member,
    # while the healthy run has an idle spare acker, so the expected ratio is lower than 0.5.
    assert degraded >= 0.3 * healthy
