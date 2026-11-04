"""Real TCP cluster: three subprocess peers on localhost (Tasks 8.3c and 8.3d, D44-D46).

Each peer (``cluster_peer.py``) binds its transport to port 0 and reports the port; the
parent sends every peer the full peer list, then starts them together.  The peers share
one FileStorage, or each has a local tier (TieredStorage) over one shared directory
object store.  The smoke tests check that no update is lost or duplicated (ruling R86:
every acknowledged id is persisted once, every other persisted id ended with
``ProtoOutcomeUnknownError``, and ``acked <= n <= acked + unknown``), that every peer made
progress, and that no committing thread wrote a root (only the persisters did, D46).  The throughput tests print commits per second through the TCP
transport (informational, for the Phase 9 record).
"""
import itertools
import json
import os
import subprocess
import sys
import tempfile
import threading
import time

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


def _run_cluster(space_dir, mode, threads, amount, peers=PEERS):
    if not isinstance(space_dir, tuple):
        space = ObjectSpace(FileStorage(space_dir, durability="none"))
        space.new_database("db")            # the tiered peers create it themselves (n0)
        space.close()
    ids = [f"n{i}" for i in range(peers)]
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
        # A worker thread that raised is named with its exception, never only as missing commits.
        errors = [f"{node_id}: {err}" for node_id, r in zip(ids, results) for err in r.get("errors", ())]
        for p in procs:
            _send(p, {"stop": True})
        for p, e in pairs:
            if p.wait(30) != 0:
                raise AssertionError(_errors(e))
        assert not errors, f"peer worker threads failed: {errors}"
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


def _check_outcomes(results, ids, n=None):
    """Ruling R86: no lost and no duplicated update.  Every acknowledged id is present once,
    every other id present ended with ``ProtoOutcomeUnknownError`` (conservative, P5), no id
    is present twice, ``n`` (when counted) equals the ids present, so
    ``acked <= n <= acked + unknown``; progress: every peer acknowledged a commit."""
    acked = {uid for r in results for uid in r["acked"]}
    unknown = {uid for r in results for uid in r["unknown"]}
    assert sum(r["commits"] for r in results) == len(acked)
    assert all(c == 1 for c in ids.values())                                  # never doubled
    assert acked <= set(ids), sorted(acked - set(ids))[:5]                    # never lost
    extra = set(ids) - acked
    assert extra <= unknown, sorted(extra - unknown)[:5]                      # each extra traced
    if n is not None:
        assert n == len(ids)
        assert len(acked) <= n <= len(acked) + len(unknown)
    assert all(r["commits"] > 0 for r in results), [r["commits"] for r in results]    # progress
    assert sum(r["failures"] for r in results) <= len(unknown)
    return acked, unknown


def _spaces(tmp_path):
    return {"file": str(tmp_path / "space"), "tiered": (str(tmp_path / "local"), str(tmp_path / "store"))}


@pytest.mark.parametrize("backend", ["file", "tiered"])
def test_three_tcp_peers_commit_concurrently(tmp_path, backend):
    space_dir = _spaces(tmp_path)[backend]
    threads, per_thread = 2, 25
    results = _run_cluster(space_dir, "increment", threads, per_thread)
    total = PEERS * threads * per_thread
    n, ids = _final(space_dir)
    acked, unknown = _check_outcomes(results, ids, n)
    assert len(acked) + len(unknown) == total
    assert sum(r["commit_publishes"] for r in results) == 0
    print(f"\n[tcp {backend}] {len(acked)} acknowledged, {len(unknown)} unknown, "
          f"{len(set(ids) - acked)} of those took effect")
    if backend == "tiered":
        assert all(r["critical"]["root_writes_in_critical"] == 0 for r in results)


def test_eight_tcp_peers_with_three_voters_all_commit(tmp_path):
    """Bounded replication over real TCP (Task 8.3g, D54): eight subprocess peers, the
    default three voters, every member committing; no update lost or duplicated (R86)."""
    space_dir = _spaces(tmp_path)["tiered"]
    threads, per_thread, peers = 2, 15, 8
    results = _run_cluster(space_dir, "increment", threads, per_thread, peers=peers)
    n, ids = _final(space_dir)
    acked, unknown = _check_outcomes(results, ids, n)
    assert len(acked) + len(unknown) == peers * threads * per_thread
    assert all(r["critical"]["root_writes_in_critical"] == 0 for r in results)
    print(f"\n[tcp 8 peers, 3 voters] {len(acked)} acknowledged, {len(unknown)} unknown")


@pytest.mark.parametrize("backend", ["file", "tiered"])
def test_tcp_throughput_three_peers(tmp_path, capsys, backend):
    space_dir = _spaces(tmp_path)[backend]
    results = _run_cluster(space_dir, "throughput", 4, 2.0)
    commits = sum(r["commits"] for r in results)
    seconds = max(r["seconds"] for r in results)
    _, ids = _final(space_dir)
    _check_outcomes(results, ids)
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


class _InProcessTcpCluster:
    """Three members over real TCP transports in this process, the first two committing
    through ``ObjectSpace``; with ``black_holed`` the third member's address is a socket whose
    accept backlog is full, so connects to it hang until their timeout."""

    def __init__(self, black_holed: bool):
        from proto_db.cluster.coordinator import ClusterCoordinator
        from proto_db.cluster.rootlog import memory_log_for
        from proto_db.cluster.transport import TcpTransport
        from proto_db.storage.memory import MemoryStorage

        transports = [TcpTransport() for _ in range(3)]
        self._hole = self._fillers = None
        addresses = [t.address for t in transports]
        if black_holed:
            transports[2].stop()                   # never started: n2 is unreachable
            self._hole, self._fillers = _black_hole()
            addresses[2] = self._hole.getsockname()
        peers = [(f"n{i}", host, port) for i, (host, port) in enumerate(addresses)]
        live = 2 if black_holed else 3
        # A restarted cluster (below): on a non-empty acceptor log any startup grace shorter than
        # lease_duration + drift_margin is refused (rulings R50, R51), so the members use the default.
        self.nodes = [ClusterCoordinator(f"n{i}", peers, transport=transports[i]).start() for i in range(live)]
        self.grace = self.nodes[0].restart_grace
        self.storage = MemoryStorage()
        for i in range(live):
            # A cluster that ran before n2 became unreachable: non-empty acceptor logs.  A fresh
            # log would rejoin first, which needs both other members of three (D46, R47).
            memory_log_for(self.storage, f"n{i}").mark_rejoined()
        ObjectSpace(self.storage).new_database("db")
        self._uids = itertools.count()

    def rate(self, seconds: float) -> float:
        """Commits per second of four threads (two per committing member) over ``seconds``."""
        done = []
        stop_at = time.monotonic() + seconds

        def worker(node):
            db = ObjectSpace(self.storage, coordinator=node).open_database("db")
            n = 0
            while time.monotonic() < stop_at:
                uid = next(self._uids)
                db.run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(uid)),
                       retries=10_000, backoff=(0.0005, 0.01))
                n += 1
            done.append(n)

        threads = [threading.Thread(target=worker, args=(self.nodes[i % 2],)) for i in range(4)]
        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start
        # Self-report: a worker that raised never appends (a share of 0 is contention, not failure).
        assert len(done) == len(threads) and sum(done) > 0, f"a worker failed or nothing committed: {done}"
        return sum(done) / elapsed

    def stop(self) -> None:
        """Stop the members and close the black hole."""
        for node in self.nodes:
            node.stop()
        if self._hole is not None:
            for c in self._fillers:
                c.close()
            self._hole.close()


#: Interleaved (healthy, degraded) sample pairs and the length of each sample.
_PAIRS = 5
_WINDOW = 1.0
#: The degraded cluster's median rate must reach both bounds.  The stall this test guards
#: against ran at 8 commits/s against 460 healthy (a ratio of 0.017): every commit waited on a
#: connect to the unreachable member.  Healthy runs measure about 160 commits/s and degraded
#: ones about 140 on the development machine, single-sample ratios spread from 0.22 (full-suite
#: load) to 1.6 (8.3d re-review 13, N5).
_MIN_DEGRADED_RATE = 20.0
_MIN_MEDIAN_RATIO = 0.1


def test_an_unreachable_member_does_not_stall_the_healthy_majority(capsys):
    """One unreachable member of three must not stall the two live ones.  Both clusters run
    side by side and are sampled alternately (healthy, degraded, healthy, ...), so a load phase
    of the machine affects both alike; the assertion is on medians, and on an absolute floor a
    stalled majority cannot reach (8.3d re-review 13, N5: the single-pair ratio was flaky)."""
    import statistics

    clusters = []
    try:
        clusters.append(_InProcessTcpCluster(black_holed=False))
        clusters.append(_InProcessTcpCluster(black_holed=True))
        time.sleep(max(c.grace for c in clusters))     # the members grant nothing during their grace
        healthy, degraded = [], []
        for _ in range(_PAIRS):
            healthy.append(clusters[0].rate(_WINDOW))
            degraded.append(clusters[1].rate(_WINDOW))
    finally:
        for c in clusters:
            c.stop()
    ratios = [d / h for h, d in zip(healthy, degraded)]
    with capsys.disabled():
        print(f"\nTCP 3 members, 2 committing, {_PAIRS} interleaved samples of {_WINDOW:g} s: all reachable "
              f"{' '.join(f'{r:.0f}' for r in healthy)} commits/s, one black-holed "
              f"{' '.join(f'{r:.0f}' for r in degraded)} commits/s, ratios {' '.join(f'{r:.2f}' for r in ratios)}")
    # Since D46 every commit needs a majority ack: with one member of three down, each needs the
    # other (busy) live member, while the healthy cluster has an idle spare acker.
    assert statistics.median(degraded) >= _MIN_DEGRADED_RATE, (healthy, degraded)
    assert statistics.median(ratios) >= _MIN_MEDIAN_RATIO, (healthy, degraded)
