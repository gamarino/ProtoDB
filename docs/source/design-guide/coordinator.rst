The coordinator, in brief
=========================

*An overview. The full protocol, with sequence and state diagrams, is* :doc:`/coordinator-protocol` *(Task
10.4). Decisions D44 (which reverted D5), D45, D46, D50, D54 and D55; ruling R82; principles P3, P4 and P5.*

What it gives the ERP
---------------------

Three application servers run the ERP, each with ProtoDB embedded, all writing the same database. A sales
person on any server can commit; a report on any server reads its snapshot without asking anyone (readers never
wait); and if one server dies, no commit that any server acknowledged is lost. There is no database server to
run, and the object store, if one is used, is written after the commit, not during it.

Contention among the processes, not in the storage
--------------------------------------------------

**The first design (D5)** made the storage the arbiter: every commit published its root with a conditional
write (compare-and-swap) on the object store, and the loser of a race retried.

**Why it failed (D44, the maintainer's amendment of 2026-11-01).** With the storage as arbiter, every lost race
costs object-store round trips and a transaction retry, and the write ceiling is set by the object store's
latency instead of the peers' network latency. Restoring the old UDP voting of version 0.1 was rejected too: a
vote was granted per time window, not by a majority with a lease and a token, so a crashed holder or a
partition could produce two writers. An external coordinator (etcd, ZooKeeper) was rejected as an extra
service for an embedded database.

**The choice.** Contention is resolved among the peers (P4), with established techniques (P6):

* a **majority lease with terms**: the terms are Paxos ballots, and the grant round is Paxos phase 1 (D45,
  D46);
* the **commit point** is a majority of the voters having durably accepted the new root within the lease
  (D46). The object store is written afterwards, asynchronously; its conditional write survives only as a
  **fencing** safety net that refuses a stale holder;
* an **owner** of a term grants short **permissions** to commit, and each node groups its own threads'
  commits into one round (D50); the next holder may build on a proposed root before it commits (D55, see
  :doc:`pipelined-permits`);
* a fixed, odd set of **voters** (three by default) orders roots; a commit's records are made durable on the
  committer and on ``f`` further members, not on every member (D54).

Timing never decides safety
---------------------------

Ruling R82, now principle P5, is the rule every cluster chapter of this guide returns to: **every safety
argument must hold under arbitrary delay, loss, duplication and reordering of messages, and arbitrary pauses of
processes.** Timers are local durations; they drive liveness only (suspecting an owner, renewing, retrying,
giving up), and they may only cause conservative failures: a timeout, an abandoned term, a refused or retried
commit, an unknown outcome. A timing-dependent safety argument found by a review is a Critical finding, never a
documented limitation.

Timers are also generous (ruling R78b): a holder believes its lease for at most a third of the promise its
granters made, so exclusion holds while one node's timer runs up to three times as fast as another's. That
protects *liveness and efficiency*; it is not what keeps commits safe.

A mistake: a rejoin that waited for time
----------------------------------------

The review of Task 8.3f found one place where commit safety still rested on elapsed time (D50 amendment,
ruling R78a). A node that had lost its disk rejoined the cluster by counting only answers sent at least
``restart_grace`` after it attached, so that every lease its lost log had served would have ended by then, *on
its holder's clock*. The probe ``r7/probe_drift_boundary.py`` lost an acknowledged commit with the wiped node's
timer running 1.3 times as fast as the holder's: the wait was shorter, in real time, than the lease it was
meant to outlast.

The fix replaced the wait with a **ballot**: a rejoin of an empty log is a Paxos round, whose promises exclude
any older term regardless of clocks. The lesson: an argument of the form "by the time X happens, Y has ended"
is a statement about clocks, and under P5 it is not an argument.

The same family of error reappeared later, in a subtler form, in the recovery round of Task 8.3n. It is told in
:doc:`unknown-outcomes`.

Back to the ERP
---------------

The three ERP servers agree on the order of commits among themselves, at network latency, and the object store
never arbitrates between them. When one dies, the others keep committing as long as a majority of the voters
is alive, and no acknowledged commit is lost, whatever the clocks of the survivors say. The protocol that makes
that true, step by step, is :doc:`/coordinator-protocol`.
