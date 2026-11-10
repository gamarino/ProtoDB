# ProtoDB design principles

These principles come before every decision record (`docs/decisions.md`), task and review. A decision
cites the principles it relies on. A change that contradicts a principle is **conceptual debt**:
it is flagged and resolved before it is implemented. It is never accepted as a documented limitation.
Bugs are expected and must be hunted down. Conceptual exceptions are not accepted.

## P1. Everything is immutable

A stored value never changes. A modification creates a new version, shares structure with the old
one, and becomes visible only by publishing a new root.

- A pointer identifies one content forever. A changed object has a different pointer.
- History is never lost. No compaction or release drops a data version. Releasing local copies
  is allowed only once another tier holds the data in full.
- Consequences to exploit, not work around:
  - caches keyed by pointer (records, decoded objects, query results) never go stale and
    can be shared across transactions, threads, processes and nodes;
  - snapshots are stable, so pagination is stable;
  - any past root stays readable.

## P2. A transaction is an explicit object; its commit concerns only its own work

A transaction is not bound to a thread, a connection or a context variable. Several can be open in
one thread, and one can be handed from thread to thread. The commit validates what *that*
transaction read and wrote, and nothing else.

- Uncommitted data does not exist for anyone else.
- Objects of a committed version are shared values usable in any transaction.
- What the model allows but cannot protect is documented prominently rather than policed with
  costly checks. Examples are reusing objects of a failed commit, and a read-modify-write split
  across two transactions.

## P3. Each node owns its work

The work of a commit (building, merging, serializing, writing) is done by the node that commits.
Work is never forwarded to another node.

- Contention is resolved locally first: threads and open transactions of one process are grouped
  before anything reaches the cluster.
- A node holds durable copies only of what it writes or replicates. Any other data is fetched on
  demand and cached. Remote data is a normal, supported case.
- Durability uses a bounded replication factor, not every node. The cluster's size must not
  multiply the cost of a commit.

## P4. Contention is resolved among the processes, never by the storage

Ordering between writers is agreed among the peers: a lease, permissions, ballots and quorums.
The storage's conditional write is only a fencing safety net. The object store is written
asynchronously, after the commit point, never in series with it.

- The owner of a term grants permissions. It does not do other nodes' work.
- Snapshot reads never contend with writers.

## P5. Safety never depends on time

Every safety argument holds under arbitrary delay, loss, duplication and reordering of messages,
and under arbitrary pauses of processes.

- Timers are local durations, never clocks. They drive liveness and efficiency only: suspecting
  an owner, renewing, retrying, giving up.
- Timers are generous. No rule may rely on tight tolerances.
- Timing may only cause conservative failures: a timeout, an abandoned term, a refused or
  retried commit, an unknown outcome. Their thresholds are tuned to reasonable values. A lost or
  forked committed commit is never acceptable under any timing.
- Message handlers never block. An operation that must wait is started, and processing continues.
  A reply is sent only after the durable operation it vouches for has completed.
- If a new message order breaks something, the defect is in the logic and is fixed there. The
  order is not constrained to hide it.

## P6. Known techniques, honestly measured

Where a mechanism has an established form (Paxos ballots and quorums, Raft learners, GFS/HDFS
replication and re-replication, BookKeeper ensembles, VR recovery), ProtoDB adopts it and cites it
instead of inventing a variant. A rule that departs from its reference says why.

- Every performance claim carries a measurement made on stated hardware with stated settings, and
  every benchmark verifies the work it reports.
- Comparisons state where ProtoDB loses.

## P7. Correct before fast

- A slower correct mechanism beats a faster one with a gap.
- The failure model is never narrowed to escape a finding.
- Optimizations are attributed to measured causes: per-commit time budgets, before and after.
