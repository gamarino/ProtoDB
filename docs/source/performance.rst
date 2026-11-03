Performance
===========

This page reports the first full run of the ProtoDB 2.0 performance suite (spec section 12,
Tasks 9.1-9.3, decision D49) and explains what wins, what loses and why. The suite lives in
``benchmarks/``; usage is in ``benchmarks/README.md``. Raw results, with every sample, are in
``benchmarks/results/2026-11-03-py314t-gil-off.json`` and
``benchmarks/results/2026-11-03-py314t-gil-on.json``.

.. contents::
   :local:
   :depth: 1

How to read these numbers
-------------------------

- **Every figure is verified.** Each workload prints the work it did and checks its result
  against an independent computation (brute force, a Python model, a read-back of the persisted
  state, a strongly consistent query on rqlite) before any timing is reported; a mismatch fails
  the run. Both runs below verified all 19 workloads (0 failed, 0 pending).
- **Medians and p95** over 5 timed repetitions after 1 warm-up (the cluster workloads use 2-3
  runs of 2-3 s each). Per-operation figures divide a batch's median by its size.
- **References are matched, not convenient** (rulings R37, R45): SQLite is a *single-process*
  reference at equal durability only; cluster mode is compared with a CAS-only baseline, with
  itself at 1/2/4/8 peers and at 0/30 ms object-store latency, and with rqlite.
- **One machine, one disk.** All cluster peers and all rqlite nodes share one local disk under
  ZFS, so their fsyncs queue on the same device; a real cluster has one disk per node.
- Memory figures are deliberately not reported.

Environment
~~~~~~~~~~~

=============================  =====================================================
CPU                            AMD Ryzen 5 5500U, 6 cores / 12 threads (12 logical CPUs)
OS / file system               Linux 7.0, work directory on ZFS (not tmpfs)
Python                         CPython 3.14.6, free-threaded build (``Py_GIL_DISABLED``)
Modes                          GIL off (default) and ``-X gil=1``
ProtoDB                        2.0.0.dev0 (git ``c19a96e`` plus this task's changes)
SQLite                         3.53.1 (bundled with the interpreter)
rqlite                         v10.6.6 official linux-amd64 release, SHA-256 verified
                               against the digest published on the release page
=============================  =====================================================

Summary
-------

**Where ProtoDB wins**

- *Versions are cheap.* Producing a new version of a dictionary costs 112 / 142 / 184 us at
  1k / 10k / 100k entries (a path copy, O(log n)); copying a Python dict for the same purpose
  costs 12 us / 103 us / 2.86 ms. Past ~10k entries every retained version is cheaper in
  ProtoDB, and every old version stays readable (checked).
- *Storage latency is off the cluster commit path.* With 30 ms injected object-store latency
  a 3-peer cluster commits 65.2/s against 65.7/s with a zero-latency store (ratio 0.99); the
  CAS-only baseline (storage-arbitrated, the reverted D5 design) falls to 6.5/s with one
  writer and 5.9/s with four, with a p95 of 3.5 s and 0.32 failed root CAS per commit. The
  cluster is 10x the CAS-only path at a realistic store latency, which is the point of D44/D46.
- *Free threading pays where work is parallel.* ``ParallelPool.scan`` reaches 4.5x at 8
  workers with the GIL off and is slower than sequential with the GIL on (0.66-0.78x).
  Concurrent reads scale 2.3x at 4 threads with the GIL off and degrade (0.6-0.9x) with it on;
  conflicting commits at 16 threads are 1.8x faster with the GIL off.
- *Indexes are used and are exact.* Every indexed plan is checked to be the index plan and to
  return the same rows as a scan; an equality lookup costs 76 us against 45 ms for a scan of
  20,000 records, a 1 % range 0.47 ms against 55 ms.

**Where ProtoDB loses, and why**

- *Single-process commit cost.* A small read-modify-write commit costs 3.3 ms without fsync
  (300 commits/s) against 53 us for SQLite ``synchronous=OFF``; with fsync 7.8 ms (128/s)
  against 1.6 ms (619/s) for SQLite ``FULL``. Profiling shows the cause: each transaction loads
  and decodes again, from the storage, every atom on the paths it touches (there is no
  decoded-atom cache shared between transactions, because atoms carry their transaction as
  context), and the commit encodes and writes the new path, rewrites the root file and, under
  ``fsync``, syncs data, root and directory (three fsyncs against SQLite's one WAL fsync).
- *Pure-Python data structures.* Point lookups by key (4.1 us) are on par with SQLite's
  primary key (3.9 us), but secondary-index queries through the LINQ front end cost about 70 us more than
  a key read (76 us against 4.6 us for SQLite), which we attribute to per-query plan
  construction (inferred from the plan structure, not profiled), and bulk loading is 60-100x slower than
  SQLite's C B-tree (168 us per record, 560 us with two indexes).
- *Cluster throughput is tens of commits per second, not hundreds.* See
  `Cluster mode`_: on this machine the critical section is 6 ms per commit with one peer and
  13-25 ms with 2-8 peers, dominated by majority replication (a follower fsync on a shared
  disk), and commits are not grouped, so rqlite, which batches concurrent requests into one
  Raft entry, commits 588 requests/s where a 3-peer ProtoDB cluster commits 60/s.
- *Top-k over a range is not seeked* (carry item R28): see `Carry items (ruling R63)`_.

Single-process queries
----------------------

20,000 records ``{id, code, city, score}`` in a committed ``Dictionary`` (key ``id``) with an
EqualityIndex on ``code`` and ``city`` and a RangeIndex on ``score``; SQLite holds the same
rows with a primary key and the same indexes. Per-query medians, GIL off (GIL on in brackets):

==================================  =================  =================  ==================
Query                               ProtoDB            Python dict/list   SQLite
==================================  =================  =================  ==================
point, primary key                  4.1 us (6.3)       0.1 us             3.9 us (4.5)
point, secondary index ``code``     75.8 us (102)      0.1 us (dict)      4.6 us (5.3)
point, no index (scan)              45 ms (48)         1.05 ms            1.16 ms (1.23)
range 1 % (200 rows), indexed       472 us (505)       50 us (bisect)     182 us (232)
range 1 %, no index                 55 ms (57)         2.8 ms             1.06 ms
top-10 of ``score >= x``            16.5 ms (13.1)     3.6 ms (heapq)     17.5 us (20.3)
top-10 of ``city == c`` by score    0.79 ms (0.64)     1.67 ms (heapq)    129 us (112)
==================================  =================  =================  ==================

Interpretation: a primary-key read is a few AVL node visits and matches SQLite. Anything that
goes through the query layer pays for building a plan (expression normalisation, estimates for
each index, explain-able plan nodes) on every call; that overhead is constant, so it matters
for point queries and fades for large results. A full scan is 40x slower than SQLite's scan
because each element is a Python ``DBObject`` and the predicate is evaluated with the value
model's total-order semantics.

Single-process writes
---------------------

====================================================  ===============  ===============
Variant                                               GIL off          GIL on
====================================================  ===============  ===============
bulk insert, no index (per record, 10k, one fsync)    168 us           168 us
bulk insert, EqualityIndex + RangeIndex               560 us           638 us
SQLite bulk insert, PK / PK + 2 indexes               2.7 / 4.8 us     3.4 / 5.5 us
update, no index (per update, 10k records)            143 us           153 us
update, 1 index                                       340 us           355 us
update, 2 indexes                                     556 us           533 us
update, 3 indexes                                     885 us           705 us
====================================================  ===============  ===============

Commit throughput per durability mode (one small read-modify-write commit: read a counter,
increment it, insert one key into a Dictionary root; SQLite: ``BEGIN; UPDATE; INSERT; COMMIT``
in WAL mode):

======================  ==================  ==========================  ==================
ProtoDB durability      commits/s           SQLite ``synchronous``      commits/s
======================  ==================  ==========================  ==================
``fsync``               128 (125)           ``FULL``                    619 (621)
``flush``               161 (163)           ``NORMAL``                  11,760 (11,221)
``none``                300 (303)           ``OFF``                     18,839 (18,546)
======================  ==================  ==========================  ==================

``flush`` is not an exact match: ProtoDB keeps the root write's fsyncs in ``flush`` mode (D19),
SQLite ``NORMAL`` in WAL mode syncs nothing at commit, so ProtoDB's ``flush`` is the stronger
guarantee and the comparison flatters SQLite. The ``fsync`` row is the like-for-like one: both
survive power loss; ProtoDB is 4.8x slower, of which ~4.5 ms per commit is its three fsyncs on
ZFS and ~3.3 ms the engine (see the summary for the cause).

Persistent update cost (``versions``):

=========  =========================  ======================  =================================
entries    new version per update     Python dict copy        committed update (``none``)
=========  =========================  ======================  =================================
1,000      112 us                     12 us                   2.8 ms
10,000     142 us                     103 us                  3.3 ms
100,000    184 us                     2,856 us                4.0 ms
=========  =========================  ======================  =================================

Cold open and caches (20,000 indexed records, 500 commits of history): opening a space and
reading one record takes 1.5 ms (2.1 ms GIL on); walking every record takes 1.53 s cold and
1.45 s warm (76 / 72 us per record). The page cache hit rate is 99.4 % on the cold walk, 99.98 %
warm and 99.9 % for random lookups, and a lookup issues 7.7 page reads: the time is not I/O, it
is decoding atoms, which every new transaction does again. Reopening a TieredStorage local tier
(log and own-segment scan, 500 commits) takes 9 ms. "Cold" is a new storage instance, not an
empty OS page cache (dropping it needs root).

Concurrency (free threading)
----------------------------

Commits on one process (``FileStorage``, ``durability="none"``), commits/s:

=============================  ========  ========  ========  ========  ========
Variant (GIL off / GIL on)     1 thr     2 thr     4 thr     8 thr     16 thr
=============================  ========  ========  ========  ========  ========
conflicting counter            460/470   408/354   344/201   168/97    92/51
blind merge (CountedSet)       253/284   266/237   270/231   248/232   250/225
blind merge, indexed dict      154/164   134/144   122/135   122/141   116/134
disjoint roots                 477/484   408/332   386/326   412/326   392/323
=============================  ========  ========  ========  ========  ========

Attempts per commit for the conflicting counter grow from 1.0 to about 7.6 at 16 threads
(retries with backoff): one counter is a single serialisation point by construction. Blind
merges never conflict (1.0 attempts) and hold their rate flat: their cost is the intent replay
inside the root lock, which serialises them. Disjoint roots also serialise on the root lock;
the commit, not the transaction body, is the bottleneck, so threads cannot add throughput.

Reads (own transaction per thread, 20,000 records), aggregate ops/s and speedup:

====================================  ==================================  ==================================
Workload                              GIL off: 1 / 4 / 16 threads         GIL on: 1 / 4 / 16 threads
====================================  ==================================  ==================================
``get_at``                            6,946 / 15,816 / 11,215 (x2.3 @4)   7,335 / 6,053 / 4,501 (x0.6 @16)
EqualityIndex query                   115 / 207 / 184 (x1.9 @8)           94 / 89 / 82
Python dict lookup                    1.07 M / 2.00 M / 3.02 M            0.80 M / 0.54 M / 0.48 M
====================================  ==================================  ==================================

With the GIL off reads scale to about the number of physical cores (6) and then flatten or
fall; with the GIL on they never scale. The read path's ceiling at 4-8 threads is not explained
yet (candidates: the storage's page-cache and state locks, reference-count contention on shared
atoms); it is reported, not diagnosed.

``ParallelPool.scan`` of a CPU-bound function (2,000 items): speedup 1.87 / 3.34 / 4.49 / 4.78 at
2 / 4 / 8 / 16 workers with the GIL off; 0.66-0.78 with the GIL on.

Vector search
-------------

10,000 vectors in 32 dimensions (50 Gaussian clusters), 100 queries, k = 10, cosine; the
reference is a numpy brute-force top-k, and ``exact`` must reproduce it exactly.

=====================  =================  ==============  =======================  ===========
Backend                build (per vec)    first query     query (median)           recall@10
=====================  =================  ==============  =======================  ===========
numpy brute force      --                 --              1.12 ms                  1.0
exact                  81 us              40 ms           1.00 ms (0.92 GIL on)    1.0
ivf_flat (100 / 10)    141 us             33 ms           1.50 ms (1.18)           1.0
hnsw (isolated)        88 us              921 ms          0.68 ms (0.81)           1.0
=====================  =================  ==============  =======================  ===========

At 10,000 vectors the exact backend's screened numpy search already equals brute force, so
IVF-Flat's probing does not pay yet; HNSW is the fastest per query but builds its graph lazily
on the first query (0.9 s). hnswlib re-enables the GIL when imported on a free-threaded build,
so its workload runs in a separate interpreter. The single "build" samples are noisy (one run).

Cluster mode
------------

The reference class for the cluster is a single-state shared database in the style of
Oracle RAC, not SQLite or a client/server SQL engine: all peers commit to one coherent state,
so writers contend by construction (as RAC orders them by SCN), and the lever is the cost of
the critical section (ruling R41). Every node's local tier commits with ``durability="fsync"``
(D46: the commit point is the local WAL fsync plus majority-acknowledged replication). Each run
checks that the final state holds exactly the acknowledged commits, every id once, with no
failed replication and every node healthy.

Peer scaling, commits/s (4 threads per peer, blind adds unless noted; GIL off / GIL on):

==========  ====================  ====================  ===================
Peers       in-process, blind     in-process, counter   TCP processes
==========  ====================  ====================  ===================
1           122 / 108             58 / 58               115 / 105
2           43 / 65               37 / 27               56 / 55
4           57 / 37               24 / 11               53 / 37
8           31 / 11               9 / 2.5               24 / 15
==========  ====================  ====================  ===================

Critical-section breakdown per commit (ms, GIL off, TCP peers): validation, intent replay,
path copy, replication:

==========  ==========  ======  =========  ============  ======
Peers       validation  replay  path copy  replication   total
==========  ==========  ======  =========  ============  ======
1           0.26        1.05    0.86       4.2           6.4
2           0.29        0.95    0.85       11.5          13.6
4           0.37        0.94    1.00       12.0          14.3
8           0.49        1.60    1.72       21.8          25.6
==========  ==========  ======  =========  ============  ======

What this shows:

- The critical section is 6-26 ms and **replication is 65-85 % of it**. Replication waits for a
  majority to write the records and the root to their acceptor logs with fsync; on this
  machine every follower fsyncs on the same disk, so a majority acknowledgement is a queue of
  fsyncs. Validation, replay and path copy together stay at 2-4 ms. With one disk per node the
  follower fsyncs run in parallel and the replication figure should approach one fsync plus a
  network round trip; that is not measured here.
- Throughput is bounded by ``1 / critical section``: 6.4 ms gives ~150/s with one peer (115
  measured), 14 ms ~70/s with 2-4 peers (53-56 measured). Commits are not grouped: each one
  acquires the lease, replicates and releases on its own. Grouping the transactions waiting on a
  node under one lease and one replication round (group commit) is the obvious lever; it is not
  implemented.
- **Lease contention grows with the peer count**: lease acquisitions tried per commit are 1.0
  (1 peer), 2.0 (2), 12 (4) and 35-43 (8), and the counter workload reaches 119 at 8 peers.
  Latency fairness is poor: with 2 TCP peers one peer's median commit latency was 69 ms and the
  other's 3.1 s (it committed only a handful of times in 3 s). The lease has no fairness
  mechanism (no queue, no hand-off): the node that holds it and has more waiting threads tends to
  win again. This is a real defect for latency-sensitive use and is reported as such.
- "Hundreds of commits per second" is reached by the single peer in-process (122/s) and by the
  CAS-only path without store latency, not by multi-peer clusters on this machine (24-57/s).

Storage-latency independence (3 in-process peers, blind adds): 65.7 commits/s with a 0 ms store
and 65.2 commits/s with a 30 ms store (GIL on: 54.5 and 53.5); commit latency p50 59 / 58 ms.
The object store is off the commit path, as designed.

CAS-only baseline (the reverted D5 design: ``CloudStorage`` writers, each its own space,
arbitrated by the store's conditional root write):

=====================  ===========  =============  ===================  ==========================
Store latency          writers      commits/s      latency p50 / p95    failed root CAS per commit
=====================  ===========  =============  ===================  ==========================
0 ms                   1            744            1.0 / 1.3 ms         0
0 ms                   4            902            1.1 / 1.4 ms         0
30 ms                  1            6.5            153 / 153 ms         0
30 ms                  4            5.9            153 / 3,472 ms       0.32
=====================  ===========  =============  ===================  ==========================

Without latency the CAS-only path is fast because it has no fsync and no replication (an
in-memory store): it is not a durability-equivalent reference, only a statement of what store
arbitration costs once the store is remote. At 30 ms the commit pays several sequential store round trips and
contention turns into retries and multi-second tails.

rqlite (3 nodes on loopback, rqlite defaults, 12 client threads with keep-alive HTTP
connections, 3 s per run, 2 runs) against a 3-peer ProtoDB TCP cluster doing the same work
(one commit inserts one id, or 50 ids):

==================  ================================  ================================
Work                rqlite                            ProtoDB (3 TCP peers)
==================  ================================  ================================
1 row per commit    588 commits/s, p50 21 ms          60 commits/s, p50 65 ms
50 rows per commit  573 commits/s, 28,647 rows/s      15 commits/s, 744 rows/s
==================  ================================  ================================

rqlite is ~10x faster per request and ~38x faster in batched rows on this machine, although its
figures include an HTTP round trip per request that the embedded ProtoDB does not pay. The
reasons are visible in the breakdown: rqlite queues concurrent writes and commits them as one
Raft entry (one fsync for many requests), and its storage engine is C; ProtoDB replicates and
fsyncs once per transaction, and a 50-id commit spends 27 ms replaying its intent and 11 ms
copying paths in the critical section (pure-Python persistent structures). These are the two
levers: group commit, and a cheaper replay/path copy for large intents.

GIL on versus GIL off
---------------------

Single-thread figures are within noise of each other (the free-threaded interpreter's
single-thread penalty is not visible in these workloads). Multi-thread figures favour GIL off
everywhere threads do Python work: reads (2.3x vs 0.8x at 4 threads), parallel scan (4.5x vs
0.75x), conflicting commits (1.8x at 16 threads), and the 8-peer in-process cluster (31 vs 11
commits/s, where all peers share one interpreter). Blind merges onto an indexed dictionary are
the one place GIL on is faster (134 vs 116 at 16 threads): that path is serialised by the root
lock anyway, and with the GIL the waiting threads cost less.

Carry items (ruling R63)
------------------------

Measured; none was optimised in Phase 9, because none had a small and safe fix:

- **Top-k early termination (R28).** ``where(score >= x).order_by(score).take(10)`` plans
  ``Limit <- Filter <- IndexOrderedScan``. The scan is lazy and stops after k matches, but it
  starts at the smallest key and filters its way to ``x``: 16.5 ms per query at 20,000 rows
  against 17.5 us for SQLite, which seeks. With a selective equality predicate the planner sorts
  the index lookup instead (0.79 ms). Fix: let an ordered index scan start at the bounds of a
  range term on the same key (a planner and index-plan change, deferred).
- **canonical() re-walk of frozen input.** ``canonical(FrozenList)`` of 220,000 elements takes
  73 ms (0.33 us per element) to return the same object, more than converting the list in the
  first place (60 ms). A trusted-frozen short-circuit needs constructor-level validation of
  ``FrozenList``/``FrozenDict``; deferred.
- **Index update cost.** An update (``set_at`` replacing a record) costs 143 us without index and
  197-329 us more per index at 10,000 rows (each index removes the old entry and adds the new one:
  two AVL path copies plus exact tokens); the 80-95 us noted in Phase 4 was measured on smaller
  collections with inserts. Bulk load with two indexes is 3.3x the unindexed cost.
- **Blind merges queue on the root lock.** Blind merges onto a 1,000-entry indexed dictionary:
  6.5 ms per commit with one thread, 8.6 ms with 16 (154 to 116 commits/s): the replay maintains
  the index inside the root lock, so they serialise, as noted in Task 5.2 (~8 ms).
- **Critical section (R41).** Reported above: 6-26 ms per cluster commit, replication-dominated.

Reproducing
-----------

::

    .venv314t/bin/python -m benchmarks --json benchmarks/results/<date>-py314t-gil-off.json
    .venv314t/bin/python -X gil=1 -m benchmarks --json benchmarks/results/<date>-py314t-gil-on.json
    python -m benchmarks --compare benchmarks/results/2026-11-03-py314t-gil-off.json

A full run took 18 minutes with the GIL off and 25 minutes with it on, on the machine above.
