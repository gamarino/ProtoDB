"""Real crashes: three TCP peer processes killed with SIGKILL, then restarted (8.3d review M7).

The peers (``crash_peer.py``) run tiered storages with ``durability="fsync"`` and a nonzero
startup grace, and commit without pause.  One peer is killed while the others go on, then
the remaining two are killed: no clean close, no persister pass, no log compaction, no
flush beyond what each commit did.  The three restart on their local directories; every
commit any peer acknowledged must be in the recovered committed root and, after ``sync``,
in the persisted root of the object store; ``n`` equals the number of ids (each commit did
both, so no commit is half applied).
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time

import pytest

from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.support.dir_object_store import DirObjectStore

PEER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_peer.py")
IDS = ["n0", "n1", "n2"]

pytestmark = [pytest.mark.timeout(240), pytest.mark.skipif(sys.platform == "win32", reason="SIGKILL")]


class Peer:
    """One peer process and the JSON lines it printed (read by a thread)."""

    def __init__(self, node_id, local, store, mode):
        self.err = tempfile.TemporaryFile("w+")
        self.proc = subprocess.Popen([sys.executable, PEER_SCRIPT, node_id, local, store, mode],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.err, text=True)
        self.lines = []
        self.cond = threading.Condition()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        for line in self.proc.stdout:
            try:
                msg = json.loads(line)
            except ValueError:
                continue                    # a line cut by the kill
            with self.cond:
                self.lines.append(msg)
                self.cond.notify_all()
        with self.cond:
            self.cond.notify_all()

    def send(self, obj):
        self.proc.stdin.write(json.dumps(obj) + "\n")
        self.proc.stdin.flush()

    def wait_for(self, key, timeout=60.0):
        deadline = time.monotonic() + timeout
        with self.cond:
            while True:
                for msg in self.lines:
                    if key in msg:
                        return msg
                remaining = deadline - time.monotonic()
                if remaining <= 0 or (self.proc.poll() is not None and not self.reader.is_alive()):
                    self.err.seek(0)
                    raise AssertionError(f"peer printed no {key!r}: {self.err.read()[-3000:]}")
                self.cond.wait(remaining)

    def acked(self):
        with self.cond:
            return {m["acked"] for m in self.lines if "acked" in m}

    def owner(self):
        """The owner of the current term this peer reported with its latest commit (D50)."""
        with self.cond:
            return next((m.get("owner") for m in reversed(self.lines) if "acked" in m), None)

    def signal(self, sig):
        if self.proc.poll() is None:
            os.kill(self.proc.pid, sig)

    def kill(self):
        if self.proc.poll() is None:
            os.kill(self.proc.pid, signal.SIGKILL)
        self.proc.wait(10)

    def close(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)
        self.reader.join(5)
        for f in (self.proc.stdin, self.proc.stdout, self.err):
            try:
                f.close()
            except OSError:
                pass


def _start(tmp_path, mode):
    store = str(tmp_path / "store")
    peers = [Peer(n, str(tmp_path / n), store, mode) for n in IDS]
    ports = [p.wait_for("port")["port"] for p in peers]
    table = [[n, "127.0.0.1", port] for n, port in zip(IDS, ports)]
    for p in peers:
        p.send({"peers": table})
    for p in peers:
        p.wait_for("ready")
    for p in peers:
        p.send({"go": True})
    return peers


def _total_acked(peers):
    return sum(len(p.acked()) for p in peers)


def _wait_acked(peers, count, timeout=60.0):
    deadline = time.monotonic() + timeout
    while _total_acked(peers) < count:
        if time.monotonic() > deadline:
            raise AssertionError(f"only {_total_acked(peers)} commits acknowledged")
        time.sleep(0.02)


def _victim(peers, role):
    """Index of the peer with ``role`` (D50): the owner of the current term as the peers
    report it, a permission holder (a committing peer that is not the owner), or the last."""
    if role == "any":
        return 2
    owners = [p.owner() for p in peers]
    owner = max(set(o for o in owners if o), key=owners.count, default="n0")
    index = IDS.index(owner)
    return index if role == "owner" else (index + 1) % len(IDS)


@pytest.mark.parametrize("role", ["any", "owner", "holder"])
def test_sigkilled_peers_lose_no_acknowledged_commit(tmp_path, role):
    peers = _start(tmp_path, "load")
    try:
        _wait_acked(peers, 30)
        victim = _victim(peers, role)
        peers[victim].kill()                             # a minority dies mid-commit
        before = _total_acked(peers)
        _wait_acked(peers, before + 30)                  # the majority goes on
        for i, p in enumerate(peers):
            if i != victim:
                p.kill()                                 # then the whole cluster
        acked = set().union(*(p.acked() for p in peers))
    finally:
        for p in peers:
            p.close()
    _verify(tmp_path, acked)


def test_an_owner_stopped_past_its_lease_loses_no_acknowledged_commit(tmp_path):
    """SIGSTOP of the owner for several leases while the others commit, then SIGCONT (D50)."""
    peers = _start(tmp_path, "load")
    try:
        _wait_acked(peers, 30)
        victim = _victim(peers, "owner")
        peers[victim].signal(signal.SIGSTOP)
        before = _total_acked(peers)
        time.sleep(3.0)                                  # three leases (crash_peer.LEASE)
        _wait_acked(peers, before + 30)                  # a new owner serves the majority
        peers[victim].signal(signal.SIGCONT)
        before = _total_acked(peers)
        _wait_acked(peers, before + 30)
        for p in peers:
            p.kill()
        acked = set().union(*(p.acked() for p in peers))
    finally:
        for p in peers:
            if p.proc.poll() is None:
                p.signal(signal.SIGCONT)
            p.close()
    _verify(tmp_path, acked)


def _verify(tmp_path, acked):
    assert len(acked) >= 60
    peers = _start(tmp_path, "verify")
    try:
        result = peers[0].wait_for("ids")
        for p in peers:
            p.wait_for("synced")
        for p in peers:
            p.send({"stop": True})
        for p in peers:
            assert p.proc.wait(30) == 0
    finally:
        for p in peers:
            p.close()
    ids = set(result["ids"])
    assert acked <= ids, f"{len(acked - ids)} acknowledged commits lost"
    assert result["n"] == len(result["ids"]) == len(ids)  # every commit applied whole, none twice
    reader = ObjectSpace(TieredStorage(str(tmp_path / "reader"), DirObjectStore(str(tmp_path / "store"))))
    try:
        tr = reader.open_database("db").new_transaction()
        persisted = set(tr.get_root_object("ids").elements())
        tr.abort()
    finally:
        reader.close()
    assert acked <= persisted
