#!/usr/bin/env python3
"""10. Cluster: local and linearizable reads

Generated from docs/tutorial/10-cluster.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "10_cluster: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/10_cluster.py
"""
import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_REPO = _os.path.normpath(_os.path.join(_HERE, *[_os.pardir] * 2))
_sys.path[:0] = [_REPO, _os.path.join(_REPO, "examples", "tutorial")]


class _Tee:
    """Standard output that also keeps what was printed since the last check."""

    def __init__(self, stream):
        self.stream, self.kept = stream, []

    def write(self, text):
        self.kept.append(text)
        return self.stream.write(text)

    def flush(self):
        self.stream.flush()


_sys.stdout = _Tee(_sys.stdout)


def _expect(shown):
    """Exit with status 1 unless the code since the last check printed ``shown`` (the chapter's Output)."""
    printed = "".join(_sys.stdout.kept)
    _sys.stdout.kept.clear()
    def normal(text):
        return "\n".join(line.rstrip() for line in text.strip().splitlines())
    if normal(printed) != normal(shown):
        _sys.stderr.write(f"--- the chapter shows ---\n{normal(shown)}\n--- the code printed ---\n{normal(printed)}\n")
        _sys.exit(1)


def _sample():
    """The chapter shows what the code since the last check printed as a sample of one run: not compared."""
    _sys.stdout.kept.clear()


# -- 10-cluster.md, line 28 ----------------------------------------
import os
import time

from proto_db import ClusterCoordinator, ObjectSpace, Peer, ProtoOutcomeUnknownError, TieredStorage
from proto_db.cluster.transport import InProcessNetwork

from memory_store import MemoryObjectStore

store, network = MemoryObjectStore(), InProcessNetwork(seed=1)
peers = [Peer(node_id, "127.0.0.1", 0) for node_id in ("n0", "n1", "n2")]
# Short timers so the chapter runs fast. They drive liveness only: no safety rule depends on them.
FAST = dict(lease_duration=0.9, request_timeout=0.1, startup_grace=0.0, backoff=(0.001, 0.02),
            persist_interval=0.02, persist_lag=0.3)
nodes = {p.node_id: ClusterCoordinator(p.node_id, peers, transport=network.endpoint(), **FAST) for p in peers}
for node in nodes.values():
    node.start()
spaces = {i: ObjectSpace(TieredStorage(os.path.join("nodes", i), store, durability="none", ship_interval=0.02),
                         coordinator=nodes[i], lock_timeout=5.0) for i in nodes}
while any(node.stats()["rejoining"] for node in nodes.values()):
    time.sleep(0.005)                                 # wait until every node has joined

writer = spaces["n0"].new_database("erp")
reader = spaces["n1"].open_database("erp")
print("voters:", sorted(nodes["n0"].voters))
_expect("""
voters: ['n0', 'n1', 'n2']
""")

# -- 10-cluster.md, line 67 ----------------------------------------
def close_deal(k):
    """Set the last closed deal to k on n0: True if acknowledged, False if the outcome is unknown."""
    try:
        writer.run(lambda tr: tr.set_root_object("last_closed", k), retries=200)
        return True
    except ProtoOutcomeUnknownError:
        return False

stale = acknowledged = k = 0
while acknowledged < 10 and k < 100:
    k += 1
    if close_deal(k):
        acknowledged += 1
        tr = reader.new_transaction()
        stale += tr.get_root_object("last_closed") != k
        tr.abort()
assert acknowledged == 10
print(f"local reads on n1 that missed the commit just acknowledged on n0: {stale} of {acknowledged}")
_sample()

# -- 10-cluster.md, line 105 ----------------------------------------
def linearizable(database, attempts=10):
    """A transaction that sees every commit acknowledged before this call started."""
    for attempt in range(attempts):
        try:
            database.run(lambda tr: tr.set_root_object("_read_barrier", True), retries=200)
            return database.new_transaction()
        except ProtoOutcomeUnknownError:
            if attempt == attempts - 1:
                raise

missed = acknowledged = 0
k = 1000
while acknowledged < 10 and k < 1100:
    k += 1
    if close_deal(k):
        acknowledged += 1
        tr = linearizable(reader)
        missed += tr.get_root_object("last_closed") != k
        tr.abort()
assert acknowledged == 10
print(f"linearizable reads that missed it: {missed} of {acknowledged}")
_expect("""
linearizable reads that missed it: 0 of 10
""")

# -- 10-cluster.md, line 141 ----------------------------------------
for space in spaces.values():
    space.close()
for node in nodes.values():
    node.stop()
network.close()

print("10_cluster: OK")
