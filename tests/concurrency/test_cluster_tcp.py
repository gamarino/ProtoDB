"""Real TCP cluster: three subprocess peers on localhost over one FileStorage (Task 8.3c, D44, D45).

Each peer (``cluster_peer.py``) binds its transport to port 0 and reports the port; the
parent sends every peer the full peer list, then starts them together.  The smoke test
checks the final count of concurrent increments is exact, every committed id appears once
and no conditional root write failed.  The throughput test prints commits per second
through the TCP transport (informational, for the Phase 9 record).
"""
import json
import os
import subprocess
import sys
import tempfile

import pytest

from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace

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


def _run_cluster(space_dir, mode, threads, amount):
    space = ObjectSpace(FileStorage(space_dir, durability="none"))
    space.new_database("db")
    space.close()
    ids = [f"n{i}" for i in range(PEERS)]
    errs = [tempfile.TemporaryFile("w+") for _ in ids]    # files: a chatty peer never blocks on stderr
    procs = [subprocess.Popen([sys.executable, PEER_SCRIPT, node_id, space_dir, mode, str(threads), str(amount)],
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
    space = ObjectSpace(FileStorage(space_dir, durability="none"))
    try:
        tr = space.open_database("db").new_transaction()
        n, ids = tr.get_root_object("n"), tr.get_root_object("ids")
        tr.abort()
        return n, dict(ids.items())
    finally:
        space.close()


def test_three_tcp_peers_commit_concurrently_to_a_shared_file_storage(tmp_path):
    space_dir = str(tmp_path / "space")
    threads, per_thread = 2, 25
    results = _run_cluster(space_dir, "increment", threads, per_thread)
    total = PEERS * threads * per_thread
    assert sum(r["commits"] for r in results) == total
    n, ids = _final(space_dir)
    assert n == total
    assert len(ids) == total and all(c == 1 for c in ids.values())
    assert sum(r["failed_cas"] for r in results) == 0
    assert sum(r["fencing"] for r in results) == 0


def test_tcp_throughput_three_peers(tmp_path, capsys):
    space_dir = str(tmp_path / "space")
    results = _run_cluster(space_dir, "throughput", 4, 2.0)
    commits = sum(r["commits"] for r in results)
    seconds = max(r["seconds"] for r in results)
    _, ids = _final(space_dir)
    assert len(ids) == commits and all(c == 1 for c in ids.values())
    assert sum(r["failed_cas"] for r in results) == 0
    with capsys.disabled():
        print(f"\nTHROUGHPUT TCP [file] 3 subprocess peers x 4 threads: {commits / seconds:.0f} commits/s "
              f"({commits} commits in {seconds:.2f}s)")
