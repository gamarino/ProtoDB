Performance
===========

This page reports the ProtoDB 2.0 performance suite (spec section 12, Tasks 9.1-9.3, decision
D49) after its first review round, and explains what wins, what loses and why. The suite lives
in ``benchmarks/``; usage is in ``benchmarks/README.md``. Raw results, with every sample, are in
``benchmarks/results/2026-11-03-py314t-gil-off.json`` and
``benchmarks/results/2026-11-03-py314t-gil-on.json``.

.. contents::
   :local:
   :depth: 1

How to read these numbers
-------------------------

- **Every figure is verified, every repetition.** Each workload prints the work it did and
  checks every timed and warm-up repetition against an independent computation (brute force,
  a Python model, a read-back of the persisted state by a new instance, a strongly consistent
  query on rqlite); one wrong repetition fails the run. Both runs below verified all 19
  workloads (0 failed, 0 pending).
- **Medians, and a tail labelled for what it is.** Most variants run 5 timed repetitions after
  1 warm-up (bulk loads and committed-version runs 3, the cluster workloads 3 runs of 3 s,
  rqlite 2). With fewer than 20 samples the raw results report the **maximum** (minimum for a
  rate) rather than a p95, because a p95 of 5 samples is just the maximum. Commit-latency p95
  figures are over every commit of a run (hundreds of samples). A figure from a single run is
  marked *single sample*. Per-operation figures divide a batch's median by its size.
- **References are matched, not convenient** (rulings R37, R45): SQLite is a *single-process*
  reference at equal durability only; cluster mode is compared with a CAS-only baseline, with
  itself at 1/2/3/4/8 peers, at 0/30 ms object-store latency and in each durability mode, and
  with rqlite.
- **One machine, one disk.** All cluster peers and all rqlite nodes share one local disk under
  ZFS, so their syncs queue on the same device; a real cluster has one disk per node.
- **Frequency scaling was on**: governor ``powersave`` with the ``amd-pstate-epp`` driver,
  EPP ``balance_power``, boost enabled (recorded in the JSON). Differences under ~15 % between
  the two runs, or against the first report, are within this machine's noise.
- Memory figures are deliberately not reported.
- **Provenance.** The results JSON was produced from a working tree with uncommitted changes
  (it records ``git: 8ad6bf3, dirty: true``), so it is not tied to a commit. It will be
  regenerated at a clean commit after Task 8.3e.

Environment
~~~~~~~~~~~

=============================  =====================================================
CPU                            AMD Ryzen 5 5500U, 6 cores / 12 threads (12 logical CPUs)
CPU frequency                  ``amd-pstate-epp``, governor ``powersave``, EPP
                               ``balance_power``, boost on
OS / file system               Linux 7.0, work directory on ZFS (not tmpfs)
Python                         CPython 3.14.6, free-threaded build (``Py_GIL_DISABLED``)
Modes                          GIL off (default) and ``-X gil=1``
ProtoDB                        2.0.0.dev0 (git ``8ad6bf3`` plus fix round 1; the JSON
                               records a dirty tree)
SQLite                         3.53.1 (bundled with the interpreter)
rqlite                         v10.6.6 official linux-amd64 release, SHA-256 verified
                               against the digest published on the release page
=============================  =====================================================

Summary
-------

**Lease starvation under TCP with fsync (D-LIVE-1): fixed at 2-4 peers by Task 8.3e; D-LIVE-2 closed in Task 8.3f-B for** ``Database.run`` **(a caller's own retry loop keeps the 5 ms window)**

The figures in this paragraph are from the Phase 9 run, before the fix. Task 8.3e measured the
cause (the releasing process re-asked 0.17 ms after its release, contenders 3.3 ms after it)
and added lease fairness: releases name the member waiting longest and the others defer to it
(``docs/decisions.md``, D46 Amendment (Task 8.3e)). Afterwards 2, 3 and 4 TCP peers split their
commits evenly (min/max 0.93-1.00 in every run, both GIL modes) at about the same throughput.
At 8 peers vote splitting is not solved: min/max ratios of 0.12-0.85 (GIL off) and 0.55-0.77
(GIL on, about 27 lease attempts per commit) were measured. Fair turns are fair for leases,
not for commits of conflicting transactions: on hot-key read-modify-write workloads whole
members starve (defect **D-LIVE-2**). Task 8.3f phase A's retry under the permission did not
close it: a retry still arrived after its permission had ended. With the owner-to-holder link
delayed by 0.25 s, a holder held 16 of 16 usable permissions without committing. It is closed
for transactions committed through ``Database.run`` by the held retries of Task 8.3f-B. A
caller's own retry loop is not covered: its retry still arrives after the round's 5 ms wait, so
the phase-A behaviour (permissions held without committing behind a delayed link) still applies
to it (``docs/decisions.md``, D50 Amendment "held retries";
``tests/concurrency/test_cluster_permit_hot_key.py``). The before/after
benchmark figures are in the Task 8.3e report and will replace this run's when the results
are regenerated. Before the fix, the cluster lease had no fairness mechanism, and with fsync-durable
local tiers over TCP one peer could starve. With 2 TCP peers x 4 threads for 3 s, the per-peer commit counts of the three
runs were 113/49, 142/40 and **178/4** with the GIL off, and 85/87, 113/55 and **169/4** with
it on; the starved peer's p95 commit latency was 2.4 s. In the first run of the suite one peer
made 4 commits in 3 s with a median latency of 3.1 s. This contradicts the "every thread
progresses" / "no starvation remained" statements of D45 and ruling R39, which were measured
with peers that do not fsync; ``docs/decisions.md`` carries the amendment. Data safety is
not affected.

**Where ProtoDB wins**

- *Versions are cheap.* Producing a new version of a dictionary costs 117 / 137 / 170 us at
  1k / 10k / 100k entries (a path copy, O(log n)); copying a Python dict for the same purpose
  costs 9 us / 124 us / 3.0 ms. Past ~10k entries every retained version is cheaper in
  ProtoDB, and every old version stays readable (checked in every repetition).
- *Storage latency is off the cluster commit path.* With 30 ms injected object-store latency
  a 3-peer in-process cluster commits 68.6/s against 63.6/s with a zero-latency store (ratio
  1.08, GIL off; with the GIL on 38.6 against 52.0/s, ratio 0.74, see `Cluster mode`_). The
  CAS-only baseline (storage-arbitrated, the reverted D5 design) falls to 6.5/s with one
  writer and 5.8/s with four, with a p95 of 3.6 s and 0.27-0.30 failed root CAS per commit.
  The cluster is ~10x the CAS-only path at a realistic store latency, which is the point of
  D44/D46.
- *Free threading pays where work is parallel.* ``ParallelPool.scan`` reaches 4.2x at 8
  workers with the GIL off and 1.0x with it on. Concurrent reads scale 1.8x at 4 threads with
  the GIL off and degrade (0.67x at 16) with it on; conflicting commits at 16 threads are 1.8x
  faster with the GIL off.
- *Indexes are used and are exact.* Every indexed plan is checked to be the index plan and to
  return the same rows as a scan; an equality lookup costs 72 us against 58 ms for a scan of
  20,000 records, a 1 % range 0.48 ms against 65 ms.

**Where ProtoDB loses, and why**

- *Single-process commit cost.* A small read-modify-write commit costs 3.3 ms without sync
  (303 commits/s) against 51 us for SQLite ``synchronous=OFF``; with ``fsync`` 8.0 ms (125/s)
  against 1.7 ms (595/s) for SQLite ``FULL``. Profiling shows the cause: each transaction loads
  and decodes again, from the storage, every atom on the paths it touches (there is no
  decoded-atom cache shared between transactions, because atoms carry their transaction as
  context), and the commit encodes and writes the new path and rewrites the root file. Under
  ``fsync`` FileStorage issues **3 fsyncs per commit** (counted in-process: segment data, root
  file, directory).
- *Pure-Python data structures.* Point lookups by key (4.0 us) are on par with SQLite's
  primary key (5.7 us in this run, 3.9-4.3 us in others), but secondary-index queries through
  the LINQ front end cost about 70 us more than a key read (72 us against 7.4 us for SQLite),
  which we attribute to per-query plan construction (inferred from the plan structure, not
  profiled), and bulk loading is 60-120x slower than SQLite's C B-tree (186 us per record,
  579 us with two indexes).
- *Cluster throughput is tens of commits per second with fsync, and that is the durability
  mode.* A 3-peer TCP cluster (4 threads per peer) commits **61/s with fsync-durable local
  tiers, 183/s with ``flush`` and 191/s with ``none``** (GIL off; 48 / 128 / 115 with the GIL
  on). Only the ``fsync`` figure carries the cluster's power-loss guarantees; ``flush`` and
  ``none`` do not sync data (see `Cluster mode`_; in this pre-8.3e run they synced nothing, and
  since Task 8.3e they sync the acceptor log, about 3 syncs per commit across the nodes). The concurrency tests report ~230/s because their peers run with ``none``/``flush``
  local tiers; the benchmark's headline uses ``fsync``, the D46 commit point. With fsync a
  commit causes **12.6 syncs across the 3 nodes** (see `Syncs per commit`_), and replication
  (11.3 ms) is 83 % of a 13.6 ms critical section; without syncs the critical section is 4.5 ms.
  Commits are not grouped: rqlite commits 592 requests/s against ProtoDB's 59/s (see
  `rqlite`_ for why that comparison is fair on durability).
- *Top-k over a range is not seeked* (carry item R28): see `Carry items (ruling R63)`_.

Single-process queries
----------------------

20,000 records ``{id, code, city, score}`` in a committed ``Dictionary`` (key ``id``) with an
EqualityIndex on ``code`` and ``city`` and a RangeIndex on ``score``; SQLite holds the same
rows with a primary key and the same indexes. Per-query medians, GIL off (GIL on in brackets):

==================================  =================  =================  ==================
Query                               ProtoDB            Python dict/list   SQLite
==================================  =================  =================  ==================
point, primary key                  4.0 us (4.4)       0.4 us (0.1)       5.7 us (4.3)
point, secondary index ``code``     72 us (77)         0.2 us (dict)      7.4 us (5.0)
point, no index (scan)              58 ms (46)         1.22 ms            1.61 ms (1.12)
range 1 % (200 rows), indexed       484 us (506)       27 us (bisect)     160 us (214)
range 1 %, no index                 65 ms (54)         2.0 ms             1.03 ms (1.08)
top-10 of ``score >= x``            12.9 ms (13.0)     2.6 ms (heapq)     16.5 us (19.3)
top-10 of ``city == c`` by score    0.62 ms (0.66)     1.27 ms (heapq)    101 us (101)
==================================  =================  =================  ==================

Interpretation: a primary-key read is a few AVL node visits and matches SQLite. Anything that
goes through the query layer pays for building a plan (expression normalisation, estimates for
each index, explain-able plan nodes) on every call; that overhead is constant, so it matters
for point queries and fades for large results. A full scan is 40-60x slower than SQLite's scan
because each element is a Python ``DBObject`` and the predicate is evaluated with the value
model's total-order semantics.

Single-process writes
---------------------

====================================================  ===============  ===============
Variant                                               GIL off          GIL on
====================================================  ===============  ===============
bulk insert, no index (per record, 10k, one fsync)    186 us           174 us
bulk insert, EqualityIndex + RangeIndex               579 us           659 us
SQLite bulk insert, PK / PK + 2 indexes               2.9 / 4.7 us     3.0 / 4.9 us
update, no index (per update, 10k records)            134 us           146 us
update, 1 index                                       317 us           366 us
update, 2 indexes                                     474 us           508 us
update, 3 indexes                                     976 us           1,015 us
====================================================  ===============  ===============

Commit throughput per durability mode (one small read-modify-write commit: read a counter,
increment it, insert one key into a Dictionary root; SQLite: ``BEGIN; UPDATE; INSERT; COMMIT``
in WAL mode), with the FileStorage syncs per commit counted in-process:

==================  ==========  ============  ======================  ==================
ProtoDB durability  commits/s   syncs/commit  SQLite ``synchronous``  commits/s
==================  ==========  ============  ======================  ==================
``fsync``           125 (129)   3             ``FULL``                595 (648)
``flush``           165 (162)   2             ``NORMAL``              11,123 (11,090)
``none``            303 (306)   0             ``OFF``                 19,765 (18,979)
==================  ==========  ============  ======================  ==================

Durability classes: ``fsync`` and SQLite ``FULL`` are the like-for-like pair (both intended
to survive power loss; ProtoDB's crash matrix checks this for ``fsync``). ``flush`` and SQLite
``NORMAL`` are in the same class: **process-crash safe, not power-loss safe** (D19).
FileStorage ``flush`` still fsyncs the root file and its directory (the 2 syncs above) but not
the segment data the root points to, so those syncs cost time without making the commit's data
power-loss safe, and no crash-matrix test covers ``flush`` under power loss; no claim beyond
"same class as NORMAL" is made. In the ``fsync`` row ProtoDB is 4.8x slower: ~4.7 ms per
commit is its three syncs on ZFS and ~3.3 ms the engine (see the summary for the cause).

Persistent update cost (``versions``):

=========  =========================  ======================  =================================
entries    new version per update     Python dict copy        committed update (``none``)
=========  =========================  ======================  =================================
1,000      117 us                     9 us                    2.7 ms
10,000     137 us                     124 us                  3.3 ms
100,000    170 us                     3,027 us                3.6 ms
=========  =========================  ======================  =================================

Cold open and caches (20,000 indexed records, 500 commits of history): opening a space and
reading one record takes 1.6 ms; walking every record takes 1.39 s cold and 1.39 s warm
(70 / 69 us per record). The page cache hit rate is 99.4 % on the cold walk, 99.98 % warm and
99.9 % for random lookups, and a lookup issues 7.7 page reads: the time is not I/O, it is
decoding atoms, which every new transaction does again. Reopening a TieredStorage local tier
(log and own-segment scan, 500 commits) takes 7.9 ms, and every reopen is checked to serve the
committed state. "Cold" is a new storage instance, not an empty OS page cache (dropping it
needs root).

Concurrency (free threading)
----------------------------

Commits on one process (``FileStorage``, ``durability="none"``), commits/s:

=============================  ========  ========  ========  ========  ========
Variant (GIL off / GIL on)     1 thr     2 thr     4 thr     8 thr     16 thr
=============================  ========  ========  ========  ========  ========
conflicting counter            498/494   447/346   315/219   191/96    91/50
blind merge (CountedSet)       292/284   263/241   265/239   269/232   260/231
blind merge, indexed dict      159/154   160/143   152/140   151/142   139/117
disjoint roots                 500/489   453/325   398/205   411/323   374/306
=============================  ========  ========  ========  ========  ========

Attempts per commit for the conflicting counter grow from 1.0 to 8.6 at 16 threads (retries
with backoff): one counter is a single serialisation point by construction. Blind merges never
conflict (1.0 attempts) and hold their rate flat: their cost is the intent replay inside the
root lock, which serialises them. Disjoint roots also serialise on the root lock; the commit,
not the transaction body, is the bottleneck, so threads cannot add throughput.

Reads (own transaction per thread, 20,000 records), aggregate ops/s and speedup:

====================================  ==================================  ==================================
Workload                              GIL off: 1 / 4 / 16 threads         GIL on: 1 / 4 / 16 threads
====================================  ==================================  ==================================
``get_at``                            7,383 / 13,141 / 11,316 (x1.8 @4)   6,357 / 5,008 / 4,284 (x0.67 @16)
EqualityIndex query                   119 / 201 / 189 (x1.7 @4)           121 / 91 / 81
Python dict lookup                    0.78 M / 1.94 M / 2.88 M            0.74 M / 0.55 M / 0.45 M
====================================  ==================================  ==================================

With the GIL off reads scale to 4 threads and then flatten (the first run reached 2.3x at 4
threads; the spread between runs is large under frequency scaling); with the GIL on they never
scale. The read path's ceiling at 4-8 threads is not explained yet (candidates: the storage's
page-cache and state locks, reference-count contention on shared atoms); it is reported, not
diagnosed.

``ParallelPool.scan`` of a CPU-bound function (2,000 items): speedup 1.8 / 3.0 / 4.2 / 4.4 at
2 / 4 / 8 / 16 workers with the GIL off; 0.95-1.0 with the GIL on.

Vector search
-------------

10,000 vectors in 32 dimensions (50 Gaussian clusters), 100 queries, k = 10, cosine; the
reference is a numpy brute-force top-k, and ``exact`` must reproduce it exactly in every
repetition (the approximate backends must reach recall 0.8 in every repetition).

=====================  =================  ==============  =======================  ===========
Backend                build (per vec)    first query     query (median)           recall@10
=====================  =================  ==============  =======================  ===========
numpy brute force      --                 --              1.08 ms                  1.0
exact                  96 us (29)         31 ms *         0.82 ms (0.75 GIL on)    1.0
ivf_flat (100 / 10)    124 us (119)       29 ms *         1.28 ms (1.21)           1.0
hnsw (isolated)        39 us (35)         901 ms *        0.69 ms (0.66)           1.0
=====================  =================  ==============  =======================  ===========

\* single sample. Builds are 3 samples each and noisy (the ``exact`` build ranged 0.96-3.6 s
with the GIL off). At 10,000 vectors the exact backend's screened numpy search already equals
brute force, so IVF-Flat's probing does not pay yet; HNSW is the fastest per query but builds
its graph lazily on the first query (0.9 s). hnswlib re-enables the GIL when imported on a
free-threaded build, so its workload runs in a separate interpreter.

Cluster mode
------------

The reference class for the cluster is a single-state shared database in the style of
Oracle RAC, not SQLite or a client/server SQL engine: all peers commit to one coherent state,
so writers contend by construction (as RAC orders them by SCN), and the lever is the cost of
the critical section (ruling R41). Unless noted, every node's local tier commits with
``durability="fsync"`` (D46: the commit point is the local log fsync plus majority-acknowledged
replication). Each run checks that the final state holds exactly the acknowledged commits,
every id once, with no failed replication and every node healthy.

Peer scaling, commits/s (4 threads per peer, blind adds unless noted; GIL off / GIL on):

==========  ====================  ====================  ===================
Peers       in-process, blind     in-process, counter   TCP processes
==========  ====================  ====================  ===================
1           122 / 88              62 / 57               112 / 105
2           66 / 61               37 / 27               58 / 55
3           --                    --                    61 / 48
4           59 / 38               23 / 11               42 / 35
8           31 / 11               8.6 / 2.5             20 / 11
==========  ====================  ====================  ===================

Durability mode, 3 TCP peers x 4 threads (GIL off / GIL on):

==============================  ==============  ======================  ==================
Local tier durability           commits/s       critical section (ms)   of which replication
==============================  ==============  ======================  ==================
``fsync`` (headline, D46)       61 / 48         13.6                    11.3 ms (83 %)
``flush``                       183 / 128       4.5                     1.8 ms (39 %)
``none``                        191 / 115       4.3                     1.7 ms (39 %)
==============================  ==============  ======================  ==================

The difference between the 232 commits/s quoted by the concurrency tests and the ~60/s here
is the durability mode: the test peers (``tests/concurrency/cluster_peer.py``) run their tiers
with ``none``/``flush``, where a commit issues no sync at all; the benchmark's headline runs
them with ``fsync``. TieredStorage ``flush`` issues no sync on the commit path (counted: 0
syncs per commit, like ``none``); the two modes measure the same thing, the engine and the
network without the disk.

.. warning::

   **Acknowledged commits survive power loss only with** ``durability="fsync"``. Since Task
   8.3e (defect D-SAFE-2 fixed) every node syncs its acceptor log -- lease promises, accepted
   roots, the rejoined marker -- before the reply each entry justifies, in every mode, so no
   lease term is granted twice and no two roots share a label, across power loss, whatever the
   mode. The mode governs the data: with ``fsync`` an acknowledged commit survives power loss
   of any set of nodes; with ``flush`` or ``none`` it survives every process crash and a power
   loss of a minority, but a power loss of a majority can lose it (a read of a lost record then
   raises ``ProtoCorruptionError``). The ``flush`` and ``none`` figures above come from the
   Phase 9 run, before the acceptor syncs were added; neither mode is durability-equivalent to
   the ``fsync`` headline or to rqlite.

Critical-section breakdown per commit (ms, GIL off, TCP peers, ``fsync``): validation, intent
replay, path copy, replication:

==========  ==========  ======  =========  ============  ======
Peers       validation  replay  path copy  replication   total
==========  ==========  ======  =========  ============  ======
1           0.25        0.99    0.84       4.5           6.6
2           0.30        0.84    0.90       11.3          13.3
3           0.30        0.99    0.97       11.3          13.6
4           0.33        1.23    1.33       14.5          17.4
8           0.46        2.09    2.06       24.1          28.7
==========  ==========  ======  =========  ============  ======

Syncs per commit
~~~~~~~~~~~~~~~~

Each TCP peer counts its own ``os.fsync``/``os.fdatasync`` calls by role and call site
(``benchmarks/fsync_probe.py``, a monkeypatch inside every peer process). Totals were
cross-checked with ``strace -f -c`` on the three peers of a 3 x 4 ``fsync`` run: the probe
counted 593 / 611 / 626 syncs per process, strace 626 / 629 / 629; the 3 % difference is the
shutdown syncs after a peer reports. In ``flush`` mode both count 0. Median run, GIL off:

- **1 peer: 4.3 syncs per commit**, all on the committing thread: 2.0 segment data
  (``FileStorage._write_out``), 1.0 lease promise record (``RootsLog.promise``), 1.0 root-log
  acceptance (``RootsLog.append``), 0.2 directory and tier flushes.
- **3 peers: 12.6 syncs per commit across the cluster.**

  - *Holder* (the committing node's application thread): **4.7** -- 2.0 segment data, 1.0
    root-log acceptance of its own root, 1.2 promise record for its own lease vote, 0.5
    directory and tier flushes.
  - *Followers* (the other nodes applying the replicated root, ``proto-cluster-worker``):
    **5.8 in total, ~2.9 per follower** -- 2.0 record copies (``TieredStorage._write_copies``),
    1.75 ranges log (``_log_ranges``), 1.6 root-log append, 0.4 directory. These run in
    sequence on each follower before it acknowledges, and the holder waits for one of them
    (the majority): that chain is the replication time in the critical section.
  - *Lease grants* (other nodes recording the promise they grant, on the transport thread):
    **2.1**, outside the critical section but on the commit path; they grow with failed lease
    attempts (5.8 tries per commit at 3 peers, 54 at 8 peers, where grants reach 32 syncs per
    commit).
  - *Background shipping* (``proto-persister``): 0.02 per commit during the window; after the
    window, until everything was persisted, the nodes issued 26 more syncs in total
    (stragglers' follower and grant records), and shipping itself issued none (the
    benchmark's directory object store does not sync).

- 2 / 4 / 8 peers: 8.7 / 21 / 67 syncs per commit.

rqlite
~~~~~~

rqlite (3 nodes on loopback, rqlite defaults, 12 client threads with keep-alive HTTP
connections, 3 s per run, 2 runs) against a 3-peer ProtoDB TCP cluster in ``fsync`` mode
doing the same work (one commit inserts one id, or 50 ids). GIL off (GIL on in brackets):

==================  ====================================  ====================================
Work                rqlite                                ProtoDB (3 TCP peers, ``fsync``)
==================  ====================================  ====================================
1 row per commit    592 (617) commits/s, p50 21 ms        59 (49) commits/s, p50 67 ms
50 rows per commit  567 (527) commits/s, 28,330 rows/s    14 (13) commits/s, 712 rows/s
==================  ====================================  ====================================

How rqlite gets there, and why the comparison is fair: each HTTP request is **its own Raft
log entry** (requests are not merged into one entry), but the Raft log store syncs with
``fdatasync`` and concurrent appends share a sync, so the sync count per node is below one per
request. Measured with ``strace -f`` attached to the three ``rqlited`` processes during a 3 s
drive with 12 clients (``PROTODB_BENCH_RQLITE_WRAPPER``; strace slows rqlite to 521
requests/s): each node issued 968 ``fdatasync`` (and 4 ``fsync``) for 1,580 requests, **0.61
syncs per request per node, 1.8 across the cluster**; batched x50, 0.71 per request per node.
ProtoDB issues 12.6 per commit across its cluster, about 7x as many. Every acknowledged request is on a majority's disk, so its
durability is comparable to ProtoDB's ``fsync`` mode, and the comparison is like for like on
durability. ProtoDB is ~10x slower per request and ~40x in batched rows on this machine,
although rqlite's figures include an HTTP round trip per request that the embedded ProtoDB does
not pay. The reasons are visible in the breakdowns: ProtoDB issues 12.6 syncs per commit
across the nodes, none shared between commits (each commit acquires the lease, replicates and
releases on its own), and a 50-id commit spends 27 ms replaying its intent and 11.5 ms copying
paths in the critical section (pure-Python persistent structures). The levers are group commit
(several transactions under one lease and one replication round, sharing its syncs), fewer
syncs per replication, and a cheaper replay/path copy for large intents.

Other findings
~~~~~~~~~~~~~~

- **Lease contention grows with the peer count**: lease acquisitions tried per commit are
  1.0 (1 peer), 2.1 (2), 5.8 (3), 13 (4) and 54 (8) over TCP, and the in-process counter
  workload reaches 119 at 8 peers (Phase 9 run). See the summary for the starvation defect
  (D-LIVE-1, fixed at 2-4 peers by Task 8.3e: with fairness the TCP figures fall to about
  1.0-1.3 attempts per commit at 2-4 peers; at 8 they spread from 1.7 to about 27 over runs).
- "Hundreds of commits per second" is reached by the cluster without syncs (183-191/s at 3
  peers in this pre-8.3e run; ``flush``/``none`` data survives process crashes and a minority
  power loss, not a majority one), by the single peer in-process with ``fsync`` (122/s) and by the CAS-only path without
  store latency, not by multi-peer ``fsync`` clusters on this machine (20-61/s).
- Storage-latency independence (3 in-process peers, blind adds): GIL off, 63.6 commits/s with
  a 0 ms store and 68.6/s with a 30 ms store (ratio 1.08). GIL on, 52.0 and 38.6/s (ratio 0.74,
  3 runs each): the in-process peers and their persister threads share one interpreter, and
  with the GIL the 30 ms run's path copy grew from 1.2 to 6.3 ms per commit. The 0.74 ratio is
  **undiagnosed** (it may be noise, interpreter contention or a real interaction) and is
  tracked as an open item. The TCP figures, one process per peer, are unaffected by it.

CAS-only baseline (the reverted D5 design: ``CloudStorage`` writers, each its own space,
arbitrated by the store's conditional root write; 3 runs each, GIL off):

=====================  ===========  =============  ===================  ==========================
Store latency          writers      commits/s      latency p50 / p95    failed root CAS per commit
=====================  ===========  =============  ===================  ==========================
0 ms                   1            971            1.0 / 1.2 ms         0
0 ms                   4            740            1.1 / 1.7 ms         0.01-0.02
30 ms                  1            6.5            153 / 153 ms         0
30 ms                  4            5.8            153 / 3,635 ms       0.27-0.30
=====================  ===========  =============  ===================  ==========================

Without latency the CAS-only path is fast because it has no sync and no replication (an
in-memory store): it is not a durability-equivalent reference, only a statement of what store
arbitration costs once the store is remote. At 30 ms the commit pays several sequential store
round trips and contention turns into retries and multi-second tails.

Payload size (Task 8.3f, D50 phase A)
-------------------------------------

The claim tested: under D50 each node builds and writes its own data, so total throughput
should depend less on transaction size than in a system that ships all data to one leader.
Measured at commit ``976e52c`` (``cluster_tcp`` and ``rqlite_reference``, full mode): 3 peers
or 3 rqlite nodes on loopback, ``fsync``, 4 client threads per node, one value of the given
size per commit (ProtoDB: a blind write into a ``Dictionary`` root; rqlite: one row per HTTP
request with a TEXT value). Both sides are verified value by value or by count and total
length. Medians of 3 runs (ProtoDB, ``cluster_tcp``) and 2 runs (``rqlite_reference``), GIL off.
ProtoDB's bytes are every frame the committing node encodes, once per destination: the total
for its two followers. rqlite's are its total loopback bytes, the client hop included.

=================  ==============  ==============  =================  =================
payload            ProtoDB         rqlite          ProtoDB bytes on   rqlite bytes on
                   commits/s       requests/s      loopback / commit  loopback / commit
=================  ==============  ==============  =================  =================
100 B              69-72           564             19 KB              2.0 KB
10 KB              59 / 99 (*)     455             190-212 KB         19 KB
100 KB             40-41           287             1.63 MB            172 KB
=================  ==============  ==============  =================  =================

(*) Two runs of the same configuration gave 58.8 (``cluster_tcp``) and 98.7 (the ProtoDB
line of ``rqlite_reference``) commits/s: a 1.7x spread between runs, i.e. instability of this
measurement on this machine, not a range of expected values (8.3f review, M5).

ProtoDB p50 commit latency 160 / 113-199 / 293-302 ms; rqlite p50 21 / 22 / 40 ms. Fsyncs per
node per commit (ProtoDB, counted in-process): 2.2, 1.1-2.3 and 2.5.

What the numbers show, honestly:

- The claim does **not** hold in this implementation. ProtoDB's throughput falls with payload
  (72 -> 41 commits/s, about 1.8x from 100 B to 100 KB); rqlite's falls by about 2x as well
  (564 -> 287), and rqlite stays 6-8x faster at every size.
- D50 does not avoid replication: a commit's records travel to every follower with the
  replication, as rqlite's leader fans entries out. ProtoDB saves only the client-to-leader hop
  and spreads the build and serialization CPU over the nodes.
- ProtoDB puts about **8x the payload on the wire per follower**: 1.63 MB per 100 KB commit
  for its two followers together (about 0.82 MB each), against rqlite's 172 KB in total
  (about 1.7x the payload, client hop included; 8.3f review, M4). The cause is the data
  model, not the commit path: a commit's records are the path-copied tree nodes of the roots it
  changed, and a ``Dictionary`` node carries its values inline, so each copied node along the
  path re-ships the value (about log2 of the dictionary's size). Even at 100 B a commit ships
  about 16 KB of tree nodes. A value stored once as its own record and referenced from the
  tree would bring this to about payload x followers; Task 8.3f item 2 (D51) does this, see
  the next section.
- Under these conditions the bytes, not the syncs, bound ProtoDB at 100 KB: about 4 MB/s of
  payload against rqlite's 30 MB/s.

After Task 8.3f phases B and C
------------------------------

Measured at commit ``ed70d3a`` against ``f03798b``, same machine and workloads. The baseline
is ``f03798b``, measured in the same session with the same driver as each comparison below:
the headline table uses the sync-probe driver (``cluster_tcp`` peers driven for 3 s, 5 runs,
88.3 commits/s GIL off at 100 B), and the payload sweep uses ``cluster_tcp`` in full mode (3
runs, median 86 commits/s at 100 B). The 69-72 commits/s of the payload table above are from
an earlier commit (``976e52c``) and are not a baseline here. Changes between the two commits: one self-describing root-log append per acceptance (D50 phase
B), compact references and values of 256 bytes or more stored out of line (D51, format 3),
group commit on a standalone storage (D52), a decoded-atom cache shared by the transactions of
an ``ObjectSpace`` (D53), and ruling R78b's lease margin. **The benchmark peers' lease changed
from 2 s to 6 s** (``benchmarks/cluster_peer.py``, ``benchmarks/workloads/cluster.py``): with
R78b's margin a holder believes a third of its lease, and at 2 s the 8-peer and the batched x50
runs ended replications after that belief and failed verification. The ``f03798b`` figures
were taken with a 2 s lease and the pre-R78b margin (belief 1.8 s); the ``ed70d3a`` figures
with a 6 s lease (belief 2 s).

3 TCP peers x 4 threads, ``fsync``, one 100-byte value per commit (the ``cluster_tcp`` peers
driven for 3 s with the in-process sync probe, 5 runs each, median run):

==============================  ==================  ==================
Figure                          ``f03798b``         ``ed70d3a``
==============================  ==================  ==================
commits/s, GIL off              88.3                104.7
commits/s, GIL on               76.3                105.4
syncs per commit (cluster)      6.2                 4.1 (3.2 GIL on)
committer frame bytes / commit  16.8 KB             11.9 KB
==============================  ==================  ==================

**Correction (Task 8.3f-B fix rounds): the "bimodal" rates were a benchmark artifact, not a
second operating mode.** An earlier version of this section explained the 144-231 commits/s
runs by one node keeping the owner role. Instrumenting the peers showed otherwise:
- In those runs one peer's first permission request waited 4.6-4.7 s in the fairness
  deferral (``_await_turn``), while the peer named by the turn became owner and kept
  renewing.
- That peer then ran its 3 s window alone, after the others had finished, and the benchmark
  divided the commits of non-overlapping windows by one window.
- The true wall-clock rate of such a run was 95 commits/s, against about 105 for the fair
  runs.

Commit ``b4259ce`` stops deferring to a turn while an owner is known. Afterwards the longest
permission wait was 0.23 s and no run was fast in 12 runs. The headline above is unaffected
(medians of fair runs).

Re-measured on the final tree of Task 8.3f-B: message lanes (D50 Amendment "Message lanes"),
held retries (D50 Amendment "held retries"), and the owner-hint fix. Same driver, 5 runs
each:
- GIL off: median 107.0 commits/s (runs 107.5, 107.0, 238.5, 86.4, 74.0), 3.88 syncs per
  commit, 12.0 KB committer frame bytes per commit.
- GIL on: median 101.4 commits/s (runs 58.4, 69.9, 106.8, 101.4, 105.9), 3.14 syncs per
  commit.
- The 238.5 run has unequal shares (190/357/188). Whether it is the same window artifact is
  not established by these figures.
- The run-to-run spread (58-107 GIL on) is wider than the medians' differences, so no change
  of the median is claimed.
- Re-measured after the review fix rounds (narrowed push lock, serialized checkpoints), same
  driver, 5 runs each, interleaved with ``0bea02a`` in one session: GIL off 118.4 commits/s
  median (runs 111.2-118.7; ``0bea02a`` 113.3, runs 102.1-119.2), 3.37 syncs per commit, 12.1
  KB committer frame bytes per commit; GIL on 108.7 (runs 100.2-120.5; ``0bea02a`` 107.8, runs
  97.2-112.0), 3.15 syncs per commit. The two trees do not differ beyond the spread, and the
  difference from the figures above is between sessions, so no gain is claimed.

Payload sweep (``cluster_tcp`` 3 runs and ``rqlite_reference`` 2 runs, GIL off, at ``ed70d3a``;
ProtoDB rates as the per-run samples). Samples above about 130 commits/s come from the window
artifact described above and are not throughput:

=========  ===========================  ==============  ==============  ==============
payload    ProtoDB commits/s            rqlite req/s    ProtoDB frame   rqlite loopback
                                                        bytes / commit  bytes / commit
=========  ===========================  ==============  ==============  ==============
100 B      109, 105, 107; 232, 108      610, 626        12.0 KB         1.8 KB
10 KB      207, 106, 95; 331, 229       625, 611        31.2 KB         18.6 KB
100 KB     184, 190, 59; 93, 94         392, 359        217 KB          173 KB
=========  ===========================  ==============  ==============  ==============

Before (``f03798b``, same runs): ProtoDB 86 / 76 / 75 commits/s (``cluster_tcp`` medians) with
16.8 KB / 196 KB / 1.87 MB per commit; rqlite 581 / 564 / 337 requests/s.

- Bytes per commit at 100 KB fall 8.6x (1.87 MB to 217 KB, about payload x followers plus 12
  KB): the value is written once as its own record and the tree carries a reference to it.
  ProtoDB's wire volume at 10 KB and 100 KB is now within 1.7x and 1.3x of rqlite's.
- rqlite is still faster at every size: 4-6x per request in the runs without the artifact. The remaining
  gap is per-commit cost (syncs, the owner hand-off between peers), not bytes.
- Batched x50 (``rqlite_reference``): 16.7 -> 17.5 commits/s (835 -> 874 rows/s) against
  rqlite's 29,100 rows/s; unchanged, because each batched commit still rebuilds and ships 50
  path-copied entries.

Standalone ``FileStorage`` with ``fsync``, 8 threads of one process committing a 100-byte
value each, three 3 s runs: 89 / 94 / 55 -> 167 / 108 / 157 commits/s, median 89 -> 157 (group
commit, D52, probe ``levels.py``). The spread between runs is large; an earlier single run with
4 threads gave 88.5 -> 147.3. Point reads of a
100,000-entry ``Dictionary``, each in a fresh transaction (``FileStorage``): 821 -> 4,721
reads/s (D53).

GIL on versus GIL off
---------------------

Single-thread figures are within noise of each other (the free-threaded interpreter's
single-thread penalty is not visible in these workloads). Multi-thread figures favour GIL off
everywhere threads do Python work: reads (1.8x vs 0.8x at 4 threads), parallel scan (4.2x vs
1.0x), conflicting commits (1.8x at 16 threads), and the 8-peer clusters (31 vs 11 commits/s
in-process, where all peers share one interpreter; 20 vs 11 over TCP). The cluster without
syncs gains most (183 vs 128 commits/s in ``flush`` mode), because there the CPU work, not the
disk, bounds it.

Carry items (ruling R63)
------------------------

Measured; none was optimised in Phase 9, because none had a small and safe fix:

- **Top-k early termination (R28).** ``where(score >= x).order_by(score).take(10)`` plans
  ``Limit <- Filter <- IndexOrderedScan``. The scan is lazy and stops after k matches, but it
  starts at the smallest key and filters its way to ``x``: 12.9 ms per query at 20,000 rows
  against 16.5 us for SQLite, which seeks. With a selective equality predicate the planner sorts
  the index lookup instead (0.62 ms). Fix: let an ordered index scan start at the bounds of a
  range term on the same key (a planner and index-plan change, deferred).
- **canonical() re-walk of frozen input.** ``canonical(FrozenList)`` of 220,000 elements takes
  71 ms (0.32 us per element) to return the same object, more than converting the list in the
  first place (57 ms). A trusted-frozen short-circuit needs constructor-level validation of
  ``FrozenList``/``FrozenDict``; deferred.
- **Index update cost.** An update (``set_at`` replacing a record) costs 134 us without index and
  160-500 us more per index at 10,000 rows (each index removes the old entry and adds the new one:
  two AVL path copies plus exact tokens); the 80-95 us noted in Phase 4 was measured on smaller
  collections with inserts. Bulk load with two indexes is 3.1x the unindexed cost.
- **Blind merges queue on the root lock.** Blind merges onto a 1,000-entry indexed dictionary:
  6.3 ms per commit with one thread, 7.2 ms with 16 (159 to 139 commits/s): the replay maintains
  the index inside the root lock, so they serialise, as noted in Task 5.2 (~8 ms).
- **Critical section (R41).** Reported above: 7-29 ms per ``fsync`` cluster commit,
  replication-dominated (68-85 %), 4.3-4.5 ms without syncs.

Reproducing
-----------

::

    .venv314t/bin/python -m benchmarks --json benchmarks/results/<date>-py314t-gil-off.json
    .venv314t/bin/python -X gil=1 -m benchmarks --json benchmarks/results/<date>-py314t-gil-on.json
    python -m benchmarks --compare benchmarks/results/2026-11-03-py314t-gil-off.json

A full run took 20 minutes with the GIL off and 27 minutes with it on, on the machine above.
``--compare`` refuses a baseline taken with a different GIL state, mode or CPU unless
``--allow-env-mismatch`` is given. To repeat the sync cross-check, wrap the peers with
``PROTODB_BENCH_PEER_WRAPPER`` and the rqlite nodes with ``PROTODB_BENCH_RQLITE_WRAPPER``
(for example ``strace -f -qq -c -e trace=fsync,fdatasync -o /path/{node}.txt``).
