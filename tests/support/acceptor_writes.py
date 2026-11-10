"""Helpers shared by the acceptor-log tests (``tests/unit/test_cluster_acceptor_writes.py`` and
``tests/unit/test_cluster_tier_failures.py``), moved here so that no test module imports
another (re-review 2 of 8.3n, M5)."""
import os
import threading
import time

import pytest

from proto_db.errors import ProtoDBError
from proto_db.storage.memory import MemoryStorage
from tests.support.manual_cluster import Harness


class DirStorage(MemoryStorage):
    """A ``MemoryStorage`` with a directory: its ``PlainTier`` keeps a real ``RootsLog`` file."""

    def __init__(self, path):
        super().__init__()
        self.path = str(path)
        self.durability = "fsync"


@pytest.fixture
def harness():
    made = []

    def make(names="abc", **kw):
        h = Harness(names, **kw)
        made.append(h)
        return h

    yield make
    for h in made:
        h.close()


def file_backed(h, name, path):
    """Give ``name`` a file-backed acceptor log (a fresh one, rejoined at the first start)."""
    old = h.nodes[name]
    old.stop()
    h.retired.append(old)
    os.makedirs(path)
    h.disks[name] = DirStorage(path)
    h.nodes[name] = h._node(name, 0.0)
    assert h.rejoin(name)


def replicate_in_background(node, lease, root):
    out = {}

    def run():
        try:
            out["r"] = node.replicate(lease, root)
        except ProtoDBError as e:   # the outcome under test
            out["r"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, out


def start_commit(h, lease, root):
    """a replicates ``root``; its copy to b is dropped, its copy to c is handed to c's worker."""
    t, out = replicate_in_background(h.nodes["a"], lease, root)
    deadline = time.monotonic() + 10.0
    handed = False
    while not handed:
        assert time.monotonic() < deadline, "a never sent its replication"
        h.net.drop(src="a", dst={"b"}, kinds=("records", "replicate"))
        for _, to, msg in h.net.take("a", {"c"}, ("records", "replicate")):
            h.net.handlers[to](msg)
            handed = handed or msg["type"] == "replicate"
        time.sleep(0.001)
    return t, out


def logged(tier):
    accepted = tier.load_roots().accepted
    return None if accepted is None else accepted[1]
