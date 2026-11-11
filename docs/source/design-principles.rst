Design principles
=================

Every decision record in ``docs/decisions.md`` cites these principles, and a change that contradicts one is
treated as conceptual debt and resolved before it is built. They are summarised here, in the order they are
numbered in ``docs/design-principles.md`` (the authoritative text, kept in the repository root).

P1. Everything is immutable
   A stored value never changes. A modification creates a new version, shares structure with the old one, and
   becomes visible only by publishing a new root. A pointer identifies one content forever, history is never
   lost, and no compaction or release drops a data version. Caches keyed by pointer never go stale and can be
   shared across transactions, threads, processes and nodes; snapshots are stable, so pagination is stable;
   any past root stays readable.

P2. A transaction is an explicit object; its commit concerns only its own work
   A transaction is not bound to a thread, a connection or a context variable, and its commit validates what
   it read and wrote and nothing else. What the model allows but cannot protect is documented prominently
   instead of being policed with costly checks (reusing objects of a failed commit; a read-modify-write split
   across two transactions).

P3. Each node owns its work
   The work of a commit is done by the node that commits, and is never forwarded. Contention is resolved
   locally first. A node holds durable copies only of what it writes or replicates, and other data is fetched
   on demand. Durability uses a bounded replication factor, so the cluster's size does not multiply the cost
   of a commit.

P4. Contention is resolved among the processes, never by the storage
   Ordering between writers is agreed among the peers (a lease, permissions, ballots and quorums). The
   storage's conditional write is only a fencing safety net, and the object store is written after the commit
   point. Snapshot reads never contend with writers.

P5. Safety never depends on time
   Every safety argument holds under arbitrary delay, loss, duplication and reordering of messages and under
   arbitrary pauses of processes. Timers drive liveness only; timing can cause only conservative failures (a
   timeout, an abandoned term, a refused or retried commit, an unknown outcome), never a lost or forked
   committed commit.

P6. Known techniques, honestly measured
   Where a mechanism has an established form (Paxos ballots and quorums, Raft learners, GFS/HDFS replication,
   BookKeeper ensembles, VR recovery) ``proto_db`` adopts and cites it. Every performance claim carries a
   measurement on stated hardware, every benchmark verifies the work it reports, and comparisons state where
   ``proto_db`` loses.

P7. Correct before fast
   A slower correct mechanism beats a faster one with a gap. The failure model is never narrowed to escape a
   finding, and optimisations are attributed to measured causes.
