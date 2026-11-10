Cluster: local reads and linearizable reads
===========================================

Several processes can write one space through a cluster of peers. Every commit is ordered by agreement
among the peers, not by the storage (P4), and a commit that has returned is durable on a majority of the
voting nodes. This tutorial starts three nodes in one process, writes on one, reads on another, and shows
the two ways to read: a fast **local snapshot read**, which may miss a commit another node just
acknowledged, and a **linearizable read**, which cannot. It does not explain the protocol; that is the
:doc:`../coordinator-protocol`. Every code block runs in the test suite.

The nodes in this tutorial talk over an *in-process simulated network* (``InProcessNetwork``) and share an
in-memory object store, so the page runs anywhere and quickly. A real deployment gives each node its own
process and machine, a ``TcpTransport`` (the default) and a real object store (``S3ObjectStore`` or
``GCSObjectStore``). Nothing else in the code changes.

Starting three nodes
--------------------

.. code-block:: python

   import os
   import threading
   import time

   from proto_db import ClusterCoordinator, ObjectSpace, Peer, TieredStorage
   from proto_db.cluster.transport import InProcessNetwork
   from proto_db.storage.cloud import ObjectNotFoundError, PreconditionFailedError

   class MemoryObjectStore:
       """The five operations of an object store, in memory: conditional writes are the only atomic ones."""

       def __init__(self):
           self._lock, self._objects, self._generation = threading.Lock(), {}, 0

       def _put(self, key, data):
           self._generation += 1
           self._objects[key] = (bytes(data), str(self._generation))
           return self._objects[key][1]

       def put_if_absent(self, key, data):
           with self._lock:
               if key in self._objects:
                   raise PreconditionFailedError(key)
               return self._put(key, data)

       def put_if_match(self, key, data, etag):
           with self._lock:
               current = self._objects.get(key)
               if current is None or current[1] != etag:
                   raise PreconditionFailedError(key)
               return self._put(key, data)

       def get(self, key):
           with self._lock:
               if key not in self._objects:
                   raise ObjectNotFoundError(key)
               return self._objects[key]

       def list(self, prefix):
           with self._lock:
               return sorted(k for k in self._objects if k.startswith(prefix))

       def delete(self, key):
           with self._lock:
               self._objects.pop(key, None)

   store, network = MemoryObjectStore(), InProcessNetwork(seed=1)
   ids = ["n0", "n1", "n2"]
   peers = [Peer(node_id, "127.0.0.1", 0) for node_id in ids]
   # Short timers so the page runs fast. They are liveness settings only: no safety rule depends on them.
   FAST = dict(lease_duration=0.9, request_timeout=0.1, startup_grace=0.0, backoff=(0.001, 0.02),
               persist_interval=0.02, persist_lag=0.3)

   nodes = {i: ClusterCoordinator(i, peers, transport=network.endpoint(), **FAST) for i in ids}
   for node in nodes.values():
       node.start()
   spaces = {i: ObjectSpace(TieredStorage(os.path.join("nodes", i), store, durability="none", ship_interval=0.02),
                            coordinator=nodes[i], lock_timeout=5.0) for i in ids}
   while any(node.stats()["rejoining"] for node in nodes.values()):
       time.sleep(0.005)                              # wait until every node has joined

   writer = spaces["n0"].new_database("shop")
   reader = spaces["n1"].open_database("shop")
   print("voters:", sorted(nodes["n0"].voters), "| nodes up:", len(nodes))

.. code-block:: text
   :caption: Output

   voters: ['n0', 'n1', 'n2'] | nodes up: 3

Each node has a ``TieredStorage``: a local directory (its write-ahead log and cache) plus the shared object
store. ``durability="none"`` here is the test setting for the local files; acknowledged commits are made
durable by the voters' synced acceptor logs whatever the mode, which is the point of the design (see
:doc:`../durability`). Three voters order the commits and tolerate the loss of one.

Local snapshot reads
--------------------

A transaction on node ``n1`` reads the newest committed root *that n1 knows*. A commit made on ``n0`` is
acknowledged to its writer once a majority has it; ``n1`` learns of it when the release message
arrives, asynchronously. So a read on ``n1`` made right after ``n0``'s ``run`` returned can still see the
previous state: a consistent snapshot, slightly old.

.. code-block:: python

   stale = 0
   for k in range(1, 21):
       writer.run(lambda tr: tr.set_root_object("orders", k), retries=200)       # acknowledged on n0
       tr = reader.new_transaction()
       stale += tr.get_root_object("orders") != k                              # read on n1 right away
       tr.abort()
   print(f"local reads that missed the commit acknowledged just before: {stale} of 20")

.. code-block:: text
   :caption: Sample output (the count varies from run to run, from 0 to most of them)

   local reads that missed the commit acknowledged just before: 14 of 20

The count is not a defect, it is the definition of a local read: it never waits and never contends with
writers (P4), at the price of lagging behind the newest commit. No bound on the lag is promised: it is
whatever delivery of the release message gives, and a delayed or lost message leaves the node behind until
it learns a newer root. The snapshot is always a state that was committed, never a mixture. For many reads
(a dashboard, a report, a page of search results) that is the right trade.

``sync()`` does **not** change that. It waits until the commits this node *already knows* are persisted in
the object store: a durability barrier, not a visibility barrier.

.. code-block:: python

   spaces["n0"].sync(timeout=10)
   print("n0 committed epoch equals its persisted epoch:",
         spaces["n0"].committed_epoch == spaces["n0"].persisted_epoch)

.. code-block:: text
   :caption: Output

   n0 committed epoch equals its persisted epoch: True

A node that wants to be sure that data is safely in the object store before it tells a user "saved" uses
``sync``. It must not use it to mean "I now see everything".

The linearizable read
---------------------

When a read must observe every commit acknowledged anywhere before it started (the user's own "refresh"
after a payment, a check before an irreversible action), run a **barrier commit** on the reading node and
start the read after it returns:

.. code-block:: python

   def linearizable(database):
       """A transaction that sees every commit acknowledged before this call started."""
       database.run(lambda tr: tr.set_root_object("_read_barrier", True), retries=200)
       return database.new_transaction()

   missed = 0
   for k in range(21, 41):
       writer.run(lambda tr: tr.set_root_object("orders", k), retries=200)
       tr = linearizable(reader)
       missed += tr.get_root_object("orders") != k
       tr.abort()
   print(f"linearizable reads that missed the commit acknowledged just before: {missed} of 20")

.. code-block:: text
   :caption: Output

   linearizable reads that missed the commit acknowledged just before: 0 of 20

The barrier is an ordinary commit, so it asks for the write permission, and the permission round makes the
node learn the newest root first (a commit whose base missed an acknowledged commit would lose it, which the
protocol forbids). That is why the barrier works, and why it has two conditions:

* It must **write at least one root or mutable**. A transaction that writes nothing returns at once, asks for
  no permission and teaches the node nothing: ``db.run(lambda tr: None)`` is not a barrier.
* It must **return normally**. A ``ProtoOutcomeUnknownError``, a conflict after all retries or a timeout
  raise, so you know the barrier did not hold; retry it or give up.

The cost is a commit: a round trip to a majority and a write of the barrier root, instead of a memory
read. It is also a **permanent** cost, because nothing is ever deleted: every barrier adds a version to
``history()`` and the root ``_read_barrier`` to the database's root names, and since the barrier is a blind
write of one value, concurrent barriers simply overwrite each other. A linearizable read path used at a
high rate therefore grows the history by one version per read. Use it where the guarantee is worth that,
and keep local snapshot reads for the rest. (A read that waits
without writing is proposed in decision D46's amendment but not implemented in 2.0.) The
:doc:`../coordinator-protocol` document draws the two paths as sequence diagrams.

Closing
-------

Close every space before stopping its node, and stop the nodes before the network:

.. code-block:: python

   for space in spaces.values():
       space.close()
   for node in nodes.values():
       node.stop()
   network.close()

When another system is the better choice
----------------------------------------

* A cluster of three voters orders commits by majority agreement: one commit costs at least one round to
  a majority plus a synced write on each of them. A single-node database, or a system with a single leader
  and asynchronous replicas, commits faster when you do not need the cluster's guarantees.
* Contention: under heavy write contention on one hot key from many processes the cluster can be unfair
  between writers (decision D-LIVE-2 is open for eight contending writer processes). Partition your writes
  or keep them to fewer writers.
* Losing a majority of the voters stops writes (learners keep serving reads). That is the price of never
  acknowledging a commit that could be lost.
* A managed distributed SQL database, with years of production history, is the safer choice when you need
  a battle-tested cluster. ProtoDB's cluster is new and is documented, tested and measured in this
  repository, not by a user base.
