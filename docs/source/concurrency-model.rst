The concurrency and isolation model
===================================

This page states precisely what a transaction sees, what a commit checks, which concurrent writes merge and
which conflict, and what the model leaves to you. The decisions are D9, D11, D33, D34, D35 and D52; the
principles are P1, P2 and P4. The :doc:`concurrency tutorial <tutorials/concurrency>` runs every statement
here.

Snapshot isolation
------------------

A transaction reads the storage root once, when it starts, and every read goes to that snapshot. A commit by
anyone else never changes what an open transaction reads, and uncommitted work does not exist for anyone but
its transaction. Readers never block writers or one another, and snapshot reads never contend with writers
(P4). A ``Snapshot`` (``Database.snapshot_at``) is the same view of any past version, read-only.

The read set
------------

``get_root_object(name)`` records ``(name, token)`` in the transaction's **read set**. The token is exact:
atoms by storage pointer, plain values by their type and exact payload (so ``1`` becoming ``1.0`` is a
change), and the *absence* of a root is a token too. ``get_mutable`` does the same for mutable cells, in
their own namespace. ``root_names()`` does not enter the read set.

At commit, under the root lock, the transaction's read set is compared with the current root. If any
recorded root changed, appeared or disappeared, the commit raises ``ProtoConflictError`` (a
``ProtoLockingError``) and **nothing is published**. The check is per root: a root is the unit of isolation,
so what happens inside a collection is covered by the root that holds it. Because everything a transaction
read is validated at commit and a transaction commits atomically, committed transactions behave as if they
ran one after another in commit order, for the roots and cells they read.

``Database.run(fn, retries=10, backoff=(0.001, 0.2))`` runs ``fn(tr)`` in a fresh transaction and retries on
``ProtoConflictError`` with exponential back-off and jitter. Any other exception aborts and propagates, and
``ProtoOutcomeUnknownError`` is never retried: the commit may have been applied.

Writes that were not read: merge or replace
-------------------------------------------

A root written without having been read has nothing to validate, and what happens to it depends on the value:

* If the value is a ``ConcurrentOptimized`` collection (``Dictionary``, ``RepeatedKeysDictionary``,
  ``HashDictionary``, ``Set``, ``CountedSet``), its **operation log is complete** (it was built from empty in
  this transaction) and the current root holds a value of the same type, the commit stores
  ``written.rebase_onto(current)``: the transaction's *intent* replayed onto the current value (per key or
  per element, by net effect). Writers of different keys do not conflict; writers of the same key are
  applied in commit order. ``CountedSet`` and ``RepeatedKeysDictionary`` counters compose by their transform
  (D26). A conflict raised by the replay is a conflict like any other.
* In every other case the written value replaces the current one (**last writer wins**). That includes
  integers, strings, records, lists, and ``remove_root_object`` of an unread root.
  ``set_root_object(name, value, replace=True)``, and a set following a ``remove_root_object`` of the same
  name in the same transaction, always replace.

The merge replays recorded operations. It never infers what a value *means*, so two blind writes of
different integers to one root do not add: the last one wins (D9 rejects "+1" merges). A value reused in a
later transaction after its commit has its log marked incomplete, so it replaces instead of replaying the
same operations twice (D33).

Mutable cells
-------------

``MutableObject`` cells live in a second namespace, the database's mutable table (D35). Reads enter the read
set with exact tokens like roots, and they conflict the same way, but per namespace: a root and a cell of the
same name are unrelated. Cells are never merged. ``new_mutable`` records the absence of the name, so two
concurrent creators of one name conflict. ``set_mutable`` requires the cell to exist, and a blind set of a
cell removed meanwhile is a conflict, so it never resurrects it. A handle (``MutableObject``) is an immutable
value naming a cell, valid in any transaction.

Commit order, groups and the cluster
------------------------------------

Commits to a database are totally ordered by the publication of the root (an epoch compare-and-swap).
Without a coordinator the publication is serialised by a thread lock and, between processes, an OS file
lock. A commit that finds a round running queues, and the thread that finds none running takes everything
queued and publishes it in one round: the group builds each commit on the root the previous one produced,
so conflicts inside a group are those of sequential commits, and one ``flush`` and one ``publish_root``
serve all of them (D52). Every commit still keeps its own version in the history. With a cluster
coordinator the order is agreed among the peers (:doc:`coordinator-protocol`). In both cases a commit's
validation reads the newest root at the moment it is built.

The commit never has an in-between state: if publishing fails, the outcome may be unknown. The storage's
``publish_outcome`` hook, or for a storage whose root lock excludes every writer a re-read of the root,
resolves it where that is provable; otherwise the caller gets ``ProtoOutcomeUnknownError``, an honest "I do
not know", never a guess (P5). After a successful publish nothing can fail the commit: releasing the lock is
best effort and a failure there is logged.

What the model leaves to you
----------------------------

A transaction commits for what it read and wrote and nothing else (P2). That is what makes it an explicit
object, and it means two things are possible and not policed:

* a read-modify-write split across two transactions (read in one, write the result in another) is not a
  conflict, because the second read nothing;
* an object taken from a transaction whose commit failed can be reused in another; its content is what it
  was, and it is on you to know that it was never published.

Free-threading
--------------

Collections, indexes and records are immutable and need no locks. Shared mutable state is guarded by locks:
the storage write buffer and segment writer, the atom and record caches, the publication of a lazy load, the
transaction's own bookkeeping, and the space's root reference. ``ObjectSpace`` and ``Database`` are shared
freely between threads. A ``Transaction`` is meant for one thread at a time; an internal lock serialises
its methods, so a misuse from several threads sees each call happen entirely before or after the others. The
test suite runs on free-threaded CPython 3.14t and with the GIL.

Interrupts and signal handlers on Python 3.14
---------------------------------------------

On CPython 3.14, including the free-threaded build, an exception raised by a signal handler, such as
``KeyboardInterrupt`` from Ctrl-C, can arrive just after a ``with lock:`` statement has taken its lock and
before its protected block starts. ``__exit__`` then never runs, and the lock stays held for good. Up to
3.13 that window did not exist for a C-implemented lock (measured: no leak in 3,000 interrupts on 3.12 and
3.13; more than half of 3,000 on 3.14). It is a property of the interpreter, not of ``proto_db``, and code
in Python cannot close it.

``proto_db`` takes such locks while it loads, caches and saves objects. If the interrupt lands inside one,
the effect is a hang, not damage:

* a lock held by the interrupted thread blocks every other thread that needs it, and a non-reentrant
  lock blocks even that thread;
* the stored data is not at risk, because a commit is published by one root write, so an interrupted
  commit is either published whole or not at all.

The rule on 3.14: after a ``KeyboardInterrupt``, or any exception from a signal handler, that may have
interrupted ``proto_db`` code, do not keep using the ``ObjectSpace`` in that process; let the process end,
and open the space again in a new one. A long-running service that must survive Ctrl-C should handle the
signal by setting a flag that its threads check between transactions, rather than letting the exception
unwind through them. Python 3.12 and 3.13 are not affected. The upstream issue is CPython gh-148874: in
3.14 ``with`` calls ``__enter__`` as an ordinary call, so the pending signal check at the end of that call
runs outside the protected range. The fix (gh-150911) is in 3.15; as of November 2026 it has not been
backported to 3.14.
