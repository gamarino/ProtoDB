# 10. Cluster: local and linearizable reads

When several processes, on several machines, must write the same ERP, the space runs on a cluster of peers.
This chapter is short: it starts three nodes, writes on one, reads on another, and shows the one decision a
cluster application has to make, **which reads may lag**. The protocol itself is the
[coordinator protocol](../source/coordinator-protocol.rst).

## What the coordinator gives you

* **One commit order.** Every commit is ordered by agreement among the voting nodes (three by default), not by
  the storage; the precondition model of chapter 5 validates each commit on the node that builds it, under its
  write permission, with the same rules on every node (protocol 16).
* **Durable on a majority.** A commit that returned is held by a majority of the voters' synced logs, so it
  survives the loss of one voter of three, power loss included.
* **Honest outcomes.** Besides "committed" and "conflict" (which `Database.run` retries), a commit can end with
  `ProtoOutcomeUnknownError`: it may or may not take effect. `run` never retries it, because a retry could apply
  the work twice.
* **Two kinds of read.** A **local** read is a committed snapshot that may lag behind the newest commit; a
  **linearizable** read sees every commit acknowledged before it started, at the price of a commit.

## Three nodes in one process

The nodes talk over an in-process simulated network and share an in-memory object store
(`examples/tutorial/memory_store.py`), so the chapter runs anywhere. A deployment gives each node its own
process and machine, a `TcpTransport` (the default) and `S3ObjectStore` or `GCSObjectStore`; nothing else in
the code changes.

```python
import os
import tempfile
import time

from proto_db import ClusterCoordinator, ObjectSpace, Peer, ProtoOutcomeUnknownError, TieredStorage
from proto_db.cluster.transport import InProcessNetwork

from memory_store import MemoryObjectStore

workdir = tempfile.TemporaryDirectory(prefix="proto_db-tutorial-")   # this chapter's own directory
home = os.getcwd()
os.chdir(workdir.name)                                # its files stay out of yours

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
```

Output:

```text
voters: ['n0', 'n1', 'n2']
```

## A local read may lag

Bob closes deals on `n0`; a dashboard on `n1` reads right after each commit returns. The commit is
acknowledged once a majority has it, and `n1` learns of it asynchronously. Each write stores a value no other
commit writes, so an unknown outcome could be resolved by looking for it; the loop simply skips them.

```python
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
```

Sample output (one run on the machine that built this page, an AMD Ryzen 5 5500U, CPython 3.14.6 free-threaded;
the count varies from run to run, from none to most of them):

```text
local reads on n1 that missed the commit just acknowledged on n0: 10 of 10
```

That is the definition of a local read: it never waits and never contends with writers, and what it sees is
always a state that was committed, never a mixture. No bound on the lag is promised. For a dashboard, a report
or a search page it is the right trade.

## A linearizable read

When a read must see every commit acknowledged anywhere before it started (the user's own refresh after
closing a deal, a check before an irreversible action), the reading node first runs a **barrier commit**,
and reads after it returns. The commit asks for the write permission, and the permission round makes the node
learn the newest root first:

```python
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
```

Output:

```text
linearizable reads that missed it: 0 of 10
```

The barrier must **write** something (a transaction that writes nothing asks for no permission and teaches the
node nothing) and must **return normally**. Its cost is a commit, a round trip to a majority, and it is
permanent: each barrier adds a version to the history. Use it where the guarantee is worth that, and local
reads for the rest. `space.sync()` is not a barrier: it waits until the commits a node already knows are in
the object store (durability, not visibility).

```python
for space in spaces.values():
    space.close()
for node in nodes.values():
    node.stop()
network.close()

os.chdir(home)
workdir.cleanup()                                     # removes only the directory this chapter created
```

The cluster is new: it is documented, tested and measured in this repository, and it has no production
history. Under heavy write contention on one hot key from many processes it can be unfair between writers
(open defect D-LIVE-2).

Depth: [the cluster tutorial](../source/tutorials/cluster.rst), [the coordinator
protocol](../source/coordinator-protocol.rst), [unknown outcomes](../source/design-guide/unknown-outcomes.rst).

Next: [11. Where to go next](11-where-next.md)
