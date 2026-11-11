Durability and recovery
=======================

This page states what a commit that has returned survives, in what order the storage writes things, what
opening a space does after a crash, and what is different in a cluster. The decisions are D4, D6, D19, D34,
D43, D46, D50, D52 and D54. The byte formats are in :doc:`format-spec`. The :doc:`operations tutorial
<tutorials/operations>` shows the modes and the damage cases on running code.

The ordering rule
-----------------

Everything durable follows one rule: **data strictly before root**. A commit writes its records, makes them
durable, and only then publishes the root that refers to them. A crash at any point therefore leaves either
the previous root or the new one, and never a root that names records which were not written. Records are
append-only and written once, so an unpublished record left by a crash is harmless: nothing refers to it.

Commit pipeline on ``FileStorage``
----------------------------------

1. The transaction's atoms are serialised, children first, and appended to the instance's own open segment
   (pushes return a pointer at once; reads of unflushed data are served from memory).
2. ``flush`` writes the buffer and makes it durable according to the mode.
3. ``publish_root`` verifies that the pointer names a readable record (a root that would make the space
   unopenable is refused with ``ProtoCorruptionError``), then, under the root lock, checks the epoch
   (compare-and-swap) and replaces ``space.root`` atomically: write a temporary file, ``fsync`` it, rename it
   over the root, ``fsync`` the directory.

Each process writes only to segments it created (opened with ``O_EXCL``), and never appends to a segment it did
not create or has rolled away from. So a torn record at the end of a crashed process's segment is never
followed by a later append. The root lock combines a thread lock and an OS advisory lock on ``space.lock``,
and a timeout raises; a commit never proceeds unlocked.

Durability modes
----------------

========= ======================================================== ======================= ===================
Mode      What is synced                                           Survives process kill   Survives power loss
========= ======================================================== ======================= ===================
``fsync`` segment data, new segment directory entries, the root    yes                     yes
          temporary file and the directory after the rename
``flush`` segment data is handed to the operating system, without  yes                     no
          ``fsync``; the root write keeps its ``fsync`` calls
``none``  nothing                                                  not promised            no
========= ======================================================== ======================= ===================

``fsync`` is the default. ``flush`` and ``none`` are explicit opt-outs for tests, benchmarks and bulk loads
that can be repeated (D6). After a power loss in the weaker modes, acknowledged commits can be lost and the
published root can name records that never reached the disk; opening then fails with ``ProtoCorruptionError``
instead of pretending to be sound.

Failure policy
--------------

An ``OSError`` while writing segment data or the root raises ``ProtoUnexpectedError`` and marks the instance
*failed*: later pushes, flushes and publishes raise, reads keep working, and recovery is to open a new
instance. A retry of ``fsync`` after a failure is unsafe (the kernel may have dropped the dirty pages), and
continuing in a new segment could let a later root reference a record whose write was lost. After a failure in
the root rename or the final directory ``fsync``, the outcome of that publish is unknown until the space is
reopened. The crash matrix (``tests/crash/test_crash_matrix.py``) injects a fault at every write, ``fsync``,
rename and directory-``fsync`` point, under a process-crash model and two power-loss models, and checks that
the visible root after reopening is one of the committed prefixes, that no acknowledged commit disappears and
that the space can be reopened and extended.

Recovery on open
----------------

Opening a ``FileStorage`` reads and CRC-checks ``space.root``, and checks that the root record exists and
passes its checksum. A missing ``space.root`` is an empty space; a **corrupt or truncated one raises**
``ProtoCorruptionError``, so damage never looks like an empty database. A leftover ``space.root.tmp`` is
ignored, never deleted at open, because another process may be writing it. Segments are not trimmed at open
(a directory shared between processes may hold a segment another live process is writing). Records after
the last published root are ignored and never reused. Every record read is checked against its CRC32, so
damage inside a record raises ``ProtoCorruptionError`` when it is read.

Unknown outcomes
----------------

If publishing fails, the commit may or may not have been applied. ``proto_db`` resolves the question where it can
prove the answer and reports ``ProtoOutcomeUnknownError`` where it cannot (P5). For a storage whose root lock
excludes every writer, the root is read again: if it is ours the commit succeeded; if another epoch is
visible, the commit conflicted. For ``CloudStorage`` the root is read back and our ``(segment, offset,
epoch)`` is looked up in the short ``recent`` history kept in the root object. Under a cluster lease an
epoch mismatch proves nothing, so an unresolved outcome is reported. ``Database.run`` never retries an unknown
outcome. The way to settle one is to read: write a unique token with each commit and look for it afterwards.

Group commit on a standalone storage
------------------------------------

Commits that arrive while a round is running join the next one: the thread that finds no round takes every
queued commit, holds the root lock once, builds each on the root the previous one produced, and makes the last
root durable with one ``flush`` and one ``publish_root`` (D52). Each commit keeps its own version in the history
and its own validation; a failure of the flush or the publish reaches every commit of the round, and nothing of
the round is visible. A commit alone runs exactly the single-commit protocol. Grouping is per process: threads
of one process share a device flush, separate processes do not. The measured effect, by number of writers, is
in :doc:`performance`.

Cloud storage
-------------

``CloudStorage`` uploads sealed segment objects first (each exactly once), then writes the root object with a
conditional write (S3 ``If-Match`` / ``If-None-Match``, GCS ``ifGenerationMatch``). A failed precondition raises
``ProtoConflictError``. The root object keeps a short history of recent roots, so a commit whose response was
lost can be recognised on read-back. With no cluster coordinator the conditional write is the arbiter between
writers; with one, it is only a fencing safety net (D44).

Cluster durability
------------------

In a cluster the commit point is **a majority of the voters having durably accepted the root**, and the
object store is written afterwards, asynchronously (P4). Two failure models must be kept apart, and all of
this assumes that the devices honour ``fsync``:

* **Power loss, disks intact.** An acceptor syncs one acceptor-log entry (the root and the commit's records,
  in a single append) before it replies, in every durability mode (D50). An acknowledged commit therefore
  survives power loss of any set of nodes. The lease and ballot promises are synced the same way, so across
  process crashes and power losses no term is granted twice, provided at most one acceptor log is lost at a
  time (R48, D46).
* **Disks lost.** Replication is bounded (D54): a fixed odd set of voters (three by default) orders roots, the
  other members are learners, and a commit's records are made durable on the committer and on ``f`` further
  members, not on every member; the rest keep them only in a bounded cache and fetch what they miss. Losing
  ``f + 1`` of the voters stops writes while learners keep serving reads. Losing every durable copy of a
  commit's records (``f + 1`` disks) before the persister has uploaded them to the object store loses that
  data, because no automatic re-replication runs.

``ObjectSpace.sync()`` waits until every commit this node knows is persisted in the object store. It is a
durability barrier, not a visibility barrier: it says nothing about commits this node has not learnt yet (see
the :doc:`cluster tutorial <tutorials/cluster>` and :doc:`coordinator-protocol`).
