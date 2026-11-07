Performance
===========

This page reports the ProtoDB 2.0 performance suite (spec section 12, decision D49) by
**workload profile** (ruling R100). For each profile it gives the conceptual reason a system
fits the work, the figures every system measured on the same work, and a plain statement of
where SQLite or rqlite is the better choice. There is no overall winner and no single headline
ratio: the systems are built on different models, and each wins where its model fits.

The suite lives in ``benchmarks/``; usage is in ``benchmarks/README.md``.

.. contents::
   :local:
   :depth: 1

How to read these numbers
-------------------------

- **Every figure was measured in Task 9c**, at a committed state, from a clean ``git archive``
  export (``python -m benchmarks.export``). Each results file records the commit and a clean flag
  that the harness re-verifies against a SHA-256 of every exported file. Figures from earlier tasks
  appear only as labelled history, with their commit.
- **Task 9c predates Task 8.3m.** Every tagged figure except [RES] was measured at ``4aab0f2`` or
  ``8b439cd``, before the residency change of Task 8.3m (the decoded-atom cache now unloads what
  it evicts). They are history for the current code; the measured differences are listed in
  `Measured costs of Task 8.3m`_ and must be applied when reading them. The full regeneration
  follows when Task 8.3m closes.
- **Every figure is verified, in every repetition.** Each workload checks its work against an
  independent computation: a brute-force scan, a model computed from pure functions, a read-back
  of the persisted state by a new instance, or a strongly consistent query on rqlite. One wrong
  repetition fails the run, and a failed run is reported, never averaged in. In the session
  [SES], one cell had failed runs (ProtoDB, 4 readers per node, GIL on); it is reported run by
  run, and its rates come only from verified runs (`Mixed reads and writes`_).
- **Medians and ranges.** The suite runs 5 timed repetitions after 1 warm-up (bulk loads 3; the
  cluster workloads 3 runs of 3 s; the rqlite workloads 2). The interleaved session runs every
  cell 5 times, alternating the GIL mode cell by cell. Figures are medians, with the range of the
  runs in brackets (``[min-max]``). With fewer than 20 samples the raw files report the maximum
  (minimum for a rate) rather than a p95. A figure from one run is marked *single run*: the
  inventory builds (one per step and GIL mode) and the 10^7 step are single runs.
- **GIL off is the default**; figures with the GIL on (``-X gil=1``) follow in parentheses or in
  their own column.
- **One machine, one disk.** All cluster peers and all rqlite nodes share one laptop CPU and one
  ZFS pool, so their syncs queue on the same device and their threads on the same 6 cores. A real
  cluster has a machine per node.
- **Frequency scaling was on** (governor ``powersave``, ``amd-pstate-epp``, EPP
  ``balance_power``, boost on). Differences under about 15 % between runs of different sessions
  are within this machine's noise.
- Memory figures (RSS) appear only where memory is the limit being measured (the inventory scale
  ladder).

Environment
~~~~~~~~~~~

=============================  =====================================================
CPU                            AMD Ryzen 5 5500U, 6 cores / 12 threads
CPU frequency                  ``amd-pstate-epp``, governor ``powersave``, EPP
                               ``balance_power``, boost on
Memory                         62 GiB, about 22 GiB available during the runs
OS / file system               Linux 7.0; every work directory on ZFS (``benchmarks/.work``),
                               never tmpfs
Python                         CPython 3.14.6, free-threaded build; GIL off (default) and
                               ``-X gil=1``
ProtoDB                        2.0.0.dev0
SQLite                         3.53.1 (bundled with the interpreter)
rqlite                         v10.6.6, official linux-amd64 release binary (run from a
                               private directory, never installed), rqlite's defaults
=============================  =====================================================

Results files and commits
~~~~~~~~~~~~~~~~~~~~~~~~~

Each figure below cites one of these files (``benchmarks/results/``). The two commits differ
only in the ``mixed_read_write`` workload (the rqlite catalog wait, see `Mixed reads and
writes`_); every other workload's code is identical in both.

==============  =========================================================  ===========  ======================================
Tag             File                                                       Commit       Content
==============  =========================================================  ===========  ======================================
[S0] / [S1]     ``2026-11-06-4aab0f2-suite-gil-off.json`` / ``-gil-on``    ``4aab0f2``  the full suite (23 workloads)
[SES]           ``2026-11-06-8b439cd-session.json``                        ``8b439cd``  interleaved session: cluster headline,
                                                                                        rqlite, mixed cells; 5 repetitions,
                                                                                        both GIL modes
[MX0] / [MX1]   ``2026-11-06-8b439cd-mixed-gil-off.json`` / ``-gil-on``    ``8b439cd``  ``mixed_read_write`` (and
                                                                                        ``cluster_peers`` with the GIL on)
[INV]           ``2026-11-06-8b439cd-inventory-*.json``                    ``8b439cd``  inventory scale ladder
[RES]           ``2026-11-06-1845993-residency-ladder.log``                ``1845993``  inventory ladder to 10^7 after Task
                                                                                        8.3m, with ``26f3b85`` at 10^6
==============  =========================================================  ===========  ======================================

Measured costs of Task 8.3m
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Task 8.3m bounded the decoded-atom cache by loaded atoms (D53 amendment, rulings R102-R105).
Measured against ``26f3b85``, the commit before it, in interleaved runs on this machine (Task 8.3m
report; ratios are speeds, below 1 slower):

- **Cluster commits: 9-11 % fewer with the GIL off, 3-13 % with it on.** The 3 x 4, 100 B
  headline gave 199.3 and 199.0 commits/s against 219.5 and 224.0 in two sessions of 5 paired runs
  (GIL on: 153.8 and 153.1 against 159.2 and 176.6). The critical-section stage that grew is the
  path copy (+0.3-0.4 ms per commit). Every ProtoDB figure of `Writes in a cluster`_ ([SES], [S0],
  [S1]) is from before this change.
- **Cold requests (a new instance per request): about 10 % slower** even when the cache holds
  every atom (inventory point lookup, equipment at a site and path trace 0.87-0.91x): each load
  is now admitted to the bounded cache and registered in its identity map (about 4.5 us per load).
- **Reads whose working set does not fit the default 50,000 entries reload,** and the loss is a
  cliff, not a slope: a reload costs a storage read and a decode, and a read pattern that cycles
  through more atoms than the cache holds reloads most of them on every pass. In the suite at the
  default capacity against ``26f3b85`` (5 interleaved runs per GIL mode; each figure is the median
  of the per-run ratios, the range in brackets): ``concurrent_reads`` ``get_at`` at 1 thread
  0.59x [0.38-0.66] with the GIL off and 0.62x [0.53-0.65] with it on, about 20 % over the
  capacity; inventory warm port lookup 0.95x [0.45-1.75] and 0.88x [0.78-1.09];
  ``query_cache`` hit 0.87x [0.84-0.90] and 0.90x [0.80-0.92]; inventory build 0.84x
  [0.82-0.89] and 0.81x [0.72-0.91]. On the 10^6-object inventory, 50 times over the capacity,
  reads are 8-34x slower (`Bulk load and scale`_, single runs). In a 20,000-record probe whose
  working set is 20 % over the capacity (``benchmarks/residency_probe.py``, median of 3 runs,
  log ``benchmarks/results/2026-11-06-8.3m-residency-probe.log``), a lookup takes 201 us against
  4.6 us when it fits with the GIL off (209 against 4.9 us with it on). A CLOCK second-chance
  bit was measured and not adopted: it softened that case (144 us; 148 with the GIL on) but cost
  7-39 % on point reads when the cache fits (medians of paired runs; D53 "Sizing"). Size
  ``atom_cache_entries`` to the
  working set (``ObjectSpace`` docstring, D53 "Sizing", which also tells how to read the thrash
  signal in ``atom_cache_stats()``).
- **When the working set fits,** the suite's other read paths are level with ``26f3b85`` or
  faster (Task 8.3m report). Part of that gain comes from two layout fixes made in the same task
  (no extra instance keys, no materialized instance dicts), not from residency itself.
- **Memory:** a resident atom costs about 0.4 KB of bookkeeping on top of its decoded state; with
  the default capacity, memory stays flat in the size of the database ([RES]).

The [SES] file's ``summary`` block predates fix round 1: for the one cell with failed runs, its
medians include them. This page takes that cell from the per-run ``records`` instead; every
other cell had no failed run.

The stray-process check (no ``rqlited``, no other benchmark process) passed before every
session. It is stored in [SES] and in ``2026-11-06-8b439cd-inventory-ladder.log``. For the suite
and mixed runs it is in ``2026-11-06-task9c-run-checks.log``, copied from the driver logs; those
JSON files predate the harness field. From Task 9c fix round 1 on, the harness stores the check
in every results file (``check``).

Profiles at a glance
--------------------

==========================================  ==========================================  ==========================================
Profile                                     Where ProtoDB fits                          Where the other system is the better choice
==========================================  ==========================================  ==========================================
`Writes in a cluster`_                      an embedded peer: no server hop, data       rqlite: 1.8-19x the rows/s on every
                                            built where it is written                   commit shape measured here
`Mixed reads and writes`_                   reads never wait and never leave the node:  rqlite when reads must be linearizable,
                                            p50 18-22 us with 1 reader per node, 50-64  and when readers share the writers'
                                            us with 4 (p99 up to 2.3 ms); but readers   CPU: its writers lose 5 % (1 reader
                                            take the writers' CPU: commits -34 % (1     per node) to 40 % (4), ProtoDB's 34 %
                                            reader per node), -79 % (4)                 to 79 %
`Point reads`_                              (on par for a primary key)                  SQLite: secondary-index and ad-hoc reads
                                                                                        3-60x faster
`Deep pages`_                               random jumps to any page: flat ~215 us      SQLite keyset for next-page browsing
`Repeated queries`_                         a repeated query on an unchanged root:      SQLite for a first, ad-hoc query
                                            served by pointer in ~3.5 ms
`History and "as of"`_                      any past version, every query, no           SQLite with an audit table when the
                                            schema work                                 history question is known in advance
`Object graphs`_                            (functional, not fast)                      SQLite joins and recursive CTEs
`Bulk load and scale`_                      -- (scale limited by a known defect, R102)  SQLite: 112-134x faster to load, 8.6-9.6x
                                                                                        smaller (allocated)
`Single-process commits`_                   ``none``/``flush`` with free threading      SQLite at matched durability
`Operations`_                               object-store latency off the commit path;   rqlite: a server any client can reach
                                            peers embedded in the application
==========================================  ==========================================  ==========================================

Writes in a cluster
-------------------

**Conceptual fit.** rqlite is a replicated SQLite on Raft: the leader orders every write, appends
it to a log that it fsyncs, replicates it, and applies it to SQLite. Each write request is one Raft
entry (our reading of the design; whether entries share a sync is not documented). ProtoDB peers are embedded in the
application: each node builds its own commits (path copies of persistent structures), asks the
owner of the term for a write permission, groups the transactions pending on that node into one
round, and replicates one self-describing append to its data replicas (D50, D54, D55). The
ProtoDB commit has no client-server hop, but it has more work per commit in Python (build,
merge, serialize) and a permission round between nodes.

**rqlite's durability, from its documentation.** The performance guide says "the Raft
subsystem calls fsync() after every write to its log", so "the change is safely persisted in
permanent storage before writing those changes to the SQLite database"
(https://rqlite.io/docs/guides/performance/). The design page calls the Raft log "the
authoritative record of every change that has happened to the system". It also says: "SQLite
runs in WAL mode and with SYNCHRONOUS=OFF"; "rqlite periodically switches SQLite to
SYNCHRONOUS=FULL mode, thereby fsync'ing the entire SQLite database to disk"; "When rqlite
restarts, it starts from the last known fsync'ed version of the SQLite database"; and, when no
such copy is available, "the SQLite database is completely rebuilt using the information stored
in the Raft log" (https://rqlite.io/docs/design/). On writes: "Normally rqlite waits until a
write request has been safely committed to a majority of SQLite databases before returning a
response to the client". Queued writes (``?queue``) instead return "after queuing the requests
but before the data is committed to the Raft log"; they are opt-in per request and are not used
here (https://rqlite.io/docs/api/queued-writes/).

*Our inference, not stated by the documentation:* an acknowledged rqlite write is on the synced
Raft log of a majority, which would put it in the same durability class as ProtoDB with
``durability="fsync"``. The documentation says the log is synced before the change is applied
and that the response waits for a majority commit. It does not say in one place that every
majority member has synced before the client is acknowledged.

The documentation does not say whether concurrent writes share one log fsync. rqlite's syncs
were not counted in this task. *History:* Task 9 counted 0.61 log syncs per request per node
with ``strace`` (``8ad6bf3``), which suggests concurrent requests share syncs; that is a
measurement, not documented behaviour.

*Storage mode.* rqlite v10.6.6 offers no in-memory or on-disk option (``rqlited -h`` lists none),
and the cited pages do not say where the SQLite file lives. The harness passes no storage flag.
The design page's WAL mode and its periodic fsync of "the entire SQLite database to disk" imply
an on-disk database; that is our reading.

3 members, 4 client threads per member, 3 s per run; ProtoDB TCP peers with
``durability="fsync"``; each commit (or rqlite request) writes one 100-byte value and one id, or
50 ids and one 100-byte value. Medians [range] of 5 interleaved runs [SES]:

=====================================  =============================  =============================  ===================
Work                                   ProtoDB commits/s              rqlite requests/s              p50 latency (ms)
                                       GIL off (GIL on)               GIL off (GIL on)               ProtoDB / rqlite
=====================================  =============================  =============================  ===================
1 id + 100 B, 3 members                285.8 [236.5-301.3] (223.8)    586.8 [244.4-617.3] (624.2)    37.7 / 20.8
50 ids + 100 B, 3 members              28.0 [26.5-29.2] (26.2)        535.6 [514.2-547.7] (511.4)    445 / 21.8
  rows/s                               1,399 (1,310)                  26,780 (25,569)
1 id + 100 B, 5 ProtoDB peers          265.6 [241.9-278.4] (215.8)    --                             71.4 / --
=====================================  =============================  =============================  ===================

*Measured before Task 8.3m* (``8b439cd``, ``4aab0f2``): the current code commits 9-11 % less with
the GIL off and 3-13 % less with it on in the 3 x 4, 100 B headline (`Measured costs of Task
8.3m`_); the ProtoDB columns of this section are not regenerated yet.

ProtoDB syncs per commit, counted in every peer (``benchmarks/fsync_probe.py``): 1.42 at 3 peers
and 1.96 at 5 (GIL off); 6.35 for the 50-id commit [SES].

Payload size, 3 members, one value per commit, measured side by side in ``rqlite_reference``
[S0], GIL off (GIL on [S1]):

==========  ==========================  ==========================  ===============================
payload     ProtoDB commits/s           rqlite requests/s           loopback bytes per commit
                                                                    ProtoDB / rqlite
==========  ==========================  ==========================  ===============================
100 B       298.0 (229.9)               592.9 (608.4)               14.1 KB / 1.8 KB
10 KB       286.7 (209.7)               589.3 (568.7)               33.2 KB / 18.6 KB
100 KB      233.0 (180.6)               410.1 (397.5)               217 KB / 172.5 KB
==========  ==========================  ==========================  ===============================

Peer scaling, TCP processes, 4 threads per peer, one id per commit, ``fsync`` [S0] / [S1]:
317 / 398 / 371 / 356 / 327 commits/s at 1 / 2 / 3 / 4 / 8 peers with the GIL off; 324 / 215 /
271 / 269 / 234 with it on. Syncs per commit, cluster-wide: 0.84 (1 peer), 1.56 (3), 2.19 (8).
At 3 peers the ``flush`` and ``none`` modes give 356 and 370 commits/s: they skip the data syncs
but, since Task 8.3e, still sync the acceptor log (1.3 syncs per commit), so the disk is no
longer what separates them from ``fsync`` on this machine.

*History, for orientation only:* Task 9 measured 61 commits/s for the 3-peer ``fsync`` cluster
(results of ``8ad6bf3``, dirty tree), and Task 8.3k measured 286.3 (GIL off) and 219.0 (GIL on)
at ``b11ef55``..``0ed9b9a``. The figures above confirm 8.3k's level at a clean commit.

**Where rqlite is the better choice.** On every commit shape measured here rqlite commits more:
about 2x at one row per commit, 19x in rows for 50-row commits, and 1.8x at 100 KB values, with
about half the latency. A 50-id ProtoDB commit spends most of its 445 ms building and
replicating 50 path copies in Python; rqlite ships 50 SQL statements in one log entry. A
workload of large transactions, or of many small writes from clients that are not Python
processes, belongs on rqlite.

**Where ProtoDB fits.** The writer is the application process itself, with no server to run or
reach. The 100 KB payload moves about 1.3x rqlite's bytes, not more. With the GIL off,
throughput holds from 1 to 8 peers (317 to 327 commits/s), although syncs per commit rise from
0.84 to 2.19. With the GIL on it falls from 324 to 234 over the same range. The design reason for
the flat GIL-off curve is that only ``f + 1`` replicas sync the data (D54); that cause was not
measured here.

Mixed reads and writes
----------------------

**Conceptual fit (ruling R97).** A ProtoDB read never waits: it opens a transaction on the root
the node last learned, an immutable value, and reads it with no lock and no message to any other
node. rqlite offers read consistency levels, quoted here from its documentation
(https://rqlite.io/docs/api/read-consistency/):

- ``none``: the node "does not perform any Leadership or other cluster-contact checks" and reads
  its local SQLite database: no freshness guarantee;
- ``weak`` (the default): the node checks locally that it is the leader and otherwise forwards
  the read to the leader; "not linearizable";
- ``linearizable``: the leader records the commit index, confirms its leadership with a quorum of
  heartbeats and waits until that index is applied, then reads; the Raft log is not involved;
- ``strong``: the read goes through the Raft log; the documentation advises against it in
  production;
- ``auto`` is not a level: a read replica uses ``none``, any other node ``weak``.

**Honest caveats.** A ProtoDB read is a *snapshot* read of the node's last known committed root.
It is not linearizable unless something on the reading node first learns from the term owner;
the only public way today is a barrier commit on the reading node before the read (its base is
at or above every acknowledged commit), which costs a write round and is not measured here. It is
a barrier only if it writes at least one root and returns normally: a transaction that writes
nothing returns at once and learns nothing, and an unknown outcome, a conflict or a timeout is no
barrier (see ``ObjectSpace.sync``). A read-only barrier has no API yet (Task 8.3n, ruling
proposal on R113). ``sync()`` is a durability barrier
and does not make another node's commits visible. Otherwise a ProtoDB read
is the same class as rqlite ``none`` with the difference that its snapshot is always a complete
committed version. A ProtoDB read whose records are not in the node's caches fetches them, from
the local tier or, on a node that is not a data replica of them, from a peer (D54): that read
waits for I/O. The ProtoDB readers are threads of the peer process; the rqlite readers are
Python HTTP clients in the benchmark process, so every rqlite read includes an HTTP round trip
that an embedded read does not.

**Setup** (``mixed_read_write``). 3 members; a catalog of 10,000 entries loaded first and
visible on every node before the window; then, for 3 s, 4 writer threads per node (one id and
one 100-byte value per commit, ``fsync``) and/or 1 or 4 reader threads per node reading random
catalog entries. Every read is checked against the catalog. Rates are totals over the 3 nodes;
latencies are over every read of every node. Medians of 5 interleaved runs [SES], GIL off:

=======================================  ===============  ===============  ==============  ===========================
Cell (threads per node)                  commits/s        reads/s          read p50 (us)   read p99 (us)
=======================================  ===============  ===============  ==============  ===========================
ProtoDB, 4 writers                       283.3            --               --              --
ProtoDB, 1 reader                        --               116,570          18              91
ProtoDB, 4 writers + 1 reader            188.0            74,984           22              181
ProtoDB, 4 readers                       --               101,913          50              965
ProtoDB, 4 writers + 4 readers           58.2             56,601           64              2,313
rqlite, 4 writers                        596.5            --               --              --
rqlite ``none``, 1 reader                --               6,844            407             783
rqlite ``none``, 4 writers + 1 reader    565.3            5,356            494             1,321
rqlite ``weak``, 1 reader                --               4,863            583             1,222
rqlite ``weak``, 4 writers + 1 reader    548.0            3,837            659             2,457
rqlite ``linearizable``, 1 reader        --               3,201            860             1,743
rqlite ``linearizable``, 4 w + 1 reader  592.9            268              10,795          27,804
rqlite ``strong``, 1 reader              --               191              15,660          23,887
rqlite ``strong``, 4 w + 1 reader        580.5            144              21,166          32,208
=======================================  ===============  ===============  ==============  ===========================

With 4 readers per node rqlite gives ``none`` 7,546 / 5,986 reads/s and 355 commits/s under
load, ``weak`` 6,036 / 6,677 and 488, ``linearizable`` 4,253 / 807 and 493, ``strong`` 576 /
503 and 501 (readers only / with writers, then commits/s with readers) [SES]. The ranges of every
cell are in [SES]. The suite's own two runs per cell [MX0] are of the same order but not always
inside the session's ranges (ProtoDB writers only 206 commits/s, 4 readers per node only 65,205
reads/s), which is the run-to-run variance of these cells on this machine. With the GIL on, the
rqlite readers (Python HTTP clients) read 25-40 % fewer rows per second at ``none`` and ``weak``
[SES]; that is the client, not rqlite.

**With the GIL on**, ProtoDB readers keep their latency, but the writers starve: they compete
with the readers for the interpreter. Medians of 5 verified runs [SES]: 211.4 commits/s with no
readers; 27.6 [25.7-28.6] with 1 reader per node (reads 90,263/s, p50 19 us, p99 134 us);
readers alone 117,739/s (1 per node) and 97,266/s (4 per node).

The cell with **4 writers and 4 readers per node, GIL on, is not measurable as a median: 4 of
its 5 session runs failed verification** [SES]:

====  ========  ==============  ==========  ============================================================
run   verified  commits/s       reads/s     outcome
====  ========  ==============  ==========  ============================================================
1     no        (12.0)          (91,424)    writer threads of one node died with
                                            ``ProtoOutcomeUnknownError``; 4 extra ids, none missing
2     no        (10.6)          (87,664)    the same; 4 extra ids, none missing
3     no        (8.1)           (91,133)    the same; 4 extra ids, none missing
4     yes       12.8            64,584      read p50 28 us, p99 336 us
5     no        (13.1)          (91,307)    ids exact, no thread error; an unhealthy member or a failed
                                            replication (the run did not record which)
====  ========  ==============  ==========  ============================================================

Rates in parentheses come from failed runs and are not figures: a run whose writer threads died
understates its rate by construction. The verified figures for this cell are the one session
run (12.8 commits/s, 64,584 reads/s) and the suite's two runs [MX1], which passed: 15.0
[14.1-15.8] commits/s and 90,395 reads/s. The failures are the conservative ones the protocol
allows (R86): an unknown outcome whose commit took effect, never a lost acknowledged commit.
They occur when reader threads hold the GIL past the commit path's timing margins (our reading
of the errors, which say the acknowledgement was counted after the permission or lease ended).
Since fix round 1 the session takes its medians over verified runs only and lists the failed
runs (``benchmarks/session.py``).

What the numbers show:

- **Readers never wait on writers in ProtoDB, but they share the CPU.** Read latency barely
  moves under writes (p50 18 -> 22 us with one reader per node). The writers lose throughput to
  the readers' CPU: -34 % with one reader per node and -79 % with four, because 12 reader
  threads plus 12 writers plus 3 nodes' protocol threads run on 6 cores. With the GIL on the
  readers win the interpreter and the writers nearly stop (27.6 and 12.8-15.8 commits/s, above).
  "Readers never wait" still holds; what readers cost is the writers' CPU. A deployment that must keep write
  throughput under heavy reads needs the reads on other cores or another member (a learner,
  D54), not more threads in the writer's process.
- **rqlite's writers lose less to its readers, but not nothing.** With one reader per node
  they lose 5 % at ``none`` (596.5 -> 565.3 commits/s), 8 % at ``weak``, 0.6 % at
  ``linearizable`` and 2.7 % at ``strong``. With four readers per node they lose 40 % at ``none``
  (355 commits/s), 18 % at ``weak`` (488), 17 % at ``linearizable`` (493) and 16 % at
  ``strong`` (501). The same steps cost ProtoDB's writers 34 % and 79 % (188 and 58 commits/s).
  Each rqlite read under write load costs 0.5-0.7 ms at ``none``/``weak`` with one reader per
  node (1.2-1.5 ms with four), and 11-21 ms at ``linearizable``/``strong``.
- Per read, an embedded ProtoDB snapshot read is about 22x faster than an rqlite ``none`` read
  through HTTP on this machine, and 500x faster than a linearizable one under load. The
  comparison is between an embedded read and a client-server read, which is the point of the
  profile, not a statement about rqlite's engine.

**Where rqlite is the better choice.** When every read must be linearizable, rqlite provides it
by configuration (``linearizable``), and ProtoDB has no measured equivalent here. When reads come
from many separate client processes that cannot embed a peer, rqlite serves them over HTTP.
When readers and writers share one Python process with the GIL on, rqlite keeps its write rate
and ProtoDB does not.

Point reads
-----------

**Conceptual fit.** A ProtoDB primary-key read is a few AVL node visits in decoded Python
objects; a secondary-index read goes through the query layer (plan construction on every call).
SQLite reads a C B-tree.

20,000 records, per query [S0] (GIL on [S1]):

=================================  ==================  =================  ==================
Query                              ProtoDB             Python dict/list   SQLite
=================================  ==================  =================  ==================
point, primary key                 4.0 us (4.6)        0.1 us             3.2 us (4.2)
point, secondary index             72.6 us (86.6)      0.1 us             4.1 us (4.9)
point, no index (scan)             41.8 ms (47.9)      0.9 ms             1.0 ms (1.1)
range 1 % (200 rows), indexed      463 us (511)        22.5 us (bisect)   138 us (205)
range 1 %, no index                52.1 ms (59.3)      1.6 ms             0.86 ms (1.0)
top-10 of ``score >= x``           175 us (197)        2.2 ms (heapq)     14.3 us (19.5)
top-10 of ``city == c`` by score   603 us (715)        1.4 ms (heapq)     79.5 us (96.1)
=================================  ==================  =================  ==================

**Where SQLite is the better choice:** everywhere but the primary key: 3-18x on indexed reads,
and an unindexed scan is 42x slower in ProtoDB for a point query and 60x for a range (52.1 ms
against 0.86 ms). *History:* top-10 of ``score >= x`` was 12.9 ms in Task 9 (R28
carry item, the ordered scan started at the lowest key); it is 175 us now.

Deep pages
----------

**Conceptual fit (D56).** A ProtoDB pager over an indexed ordered query seeks by rank (weights
kept in the index tree), on a snapshot that never changes, so page *n* costs the same as page 0
and a token resumes the same pages in any later transaction. SQLite's ``OFFSET`` reads and skips
every earlier row; its keyset pagination (``WHERE k > last``) is constant but only for the next
page.

10^6 rows, pages of 50, each request a new transaction (ProtoDB) on one connection (SQLite).
Per page [S0] (GIL on [S1]):

=============  ===============  ================  =================  ==================  ===================
offset         ProtoDB pager    ProtoDB islice    SQLite OFFSET      SQLite keyset,      SQLite keyset,
               (by rank)        (no rank)                            next page           chained jump
=============  ===============  ================  =================  ==================  ===================
0              212 us (213)     153 us            32 us (40)         34 us (41)          --
10,000         217 us (221)     15.6 ms           316 us (325)       37 us (46)          55 ms (58)
100,000        213 us (223)     12.3 s            3.0 ms (2.9)       38 us (45)          563 ms (580)
900,000        218 us (216)     --                37.7 ms (37.7)     35 us (42)          --
=============  ===============  ================  =================  ==================  ===================

With a range predicate (``k >= N/2``) the ProtoDB pager costs 376-510 us at every offset; SQLite
``OFFSET`` 34 us to 3.1 ms. A pager over an index without weights (built before D56)
materializes the whole result before its first page: 1.31 s. *History (8.4b, ``e9afb72``):* the
pager measured about 230-250 us flat and SQLite ``OFFSET`` 42.5 ms at 9 x 10^5.

**Where SQLite is the better choice:** browsing next page after next page (keyset: 35-38 us, 6x
faster), and any page within the first few thousand rows by ``OFFSET``. ProtoDB is the better choice
for random jumps deep into a large result (14x faster than ``OFFSET`` at 10^5 rows, 170x at
9 x 10^5) and for pages that must stay stable while writers commit.

Repeated queries
----------------

**Conceptual fit (D56, P1).** A ProtoDB query result is cached by the pointers it read: a root
that has not changed is the same pointer, so a repeated query is a lookup, shareable across
transactions and threads. SQLite has no result cache: every request runs. The first run of an
ad-hoc query pays the plan and, in ProtoDB, a cache miss costs more than an uncached run.

[S0] (GIL on [S1]), per request:

===================================================  ===============  ===============  ================  ===============
Query                                                ProtoDB          ProtoDB          ProtoDB           SQLite
                                                     no cache         cache miss       cache hit         (re-executes)
===================================================  ===============  ===============  ================  ===============
range + filter, ~10^4 rows, N = 10^5                 77.8 ms (77.3)   128 ms (130)     3.41 ms (3.48)    9.9 ms (11.4)
range + filter, ~10^4 rows, N = 10^6                 80.4 ms (82.1)   137 ms (135)     3.51 ms (3.67)    104.8 ms (108)
filter on an unindexed field, ordered, N = 10^5      12.8 s (13.1)    12.8 s (13.1)    0.79 ms (0.39)    33.4 ms (33.9)
===================================================  ===============  ===============  ================  ===============

**Where SQLite is the better choice:** a first, ad-hoc query is 8x faster in SQLite at 10^5 rows
and 380x faster on the unindexed ordered filter, and a cache miss on the range query costs ProtoDB
1.6-1.7x an uncached run. ProtoDB is the better choice when the same query repeats on a root that changes
less often than it is read: 3.5 ms against SQLite's 10-105 ms.

History and "as of"
-------------------

**Conceptual fit (P1).** Every ProtoDB commit is a new immutable root; an earlier root is read
with the same code as the current one (``Database.snapshot_at``), for any query, with no schema
work and no write cost beyond the commit itself. SQLite keeps history only if it is designed in:
here an ``equipment_audit`` table, filled by a trigger on status updates and stamped with a
version counter, and an "as of" query that undoes the later changes. The two do equal work for
the one question the audit table was designed for.

From the inventory workload (`Bulk load and scale`_): the status of every equipment of one
region in the version before the last bulk change, per equipment [S0] at 10^5 objects, [INV]
at 10^6:

=========================  =============================  =============================
Objects (equipment)        ProtoDB as of (us/equipment)   SQLite audit (us/equipment)
=========================  =============================  =============================
95,645 (20)                7.7 (7.9)                      1.2 (1.2)
958,665 (140)              10.3 (7.0)                     0.9 (1.0)
=========================  =============================  =============================

The bulk change itself (one commit, the same region) costs ProtoDB 663 / 424 us per equipment
and SQLite, trigger included, 80 / 15 us.

``versions`` [S0]: a new version per update costs ProtoDB 42 / 55 / 67 us at 10^3 / 10^4 / 10^5
entries; a Python ``dict`` copy costs 8 us / 158 us / 3.1 ms. Those ProtoDB figures are versions
in memory, not committed. A *committed* update (one transaction, ``durability="none"``) costs
1.14 / 1.50 / 1.52 ms at the same sizes (GIL on [S1]: 1.11 / 1.34 / 1.60 ms). So an uncommitted
version is cheaper than a ``dict`` copy from 10^4 entries on. A committed, durable version costs
more than a ``dict`` copy at 10^3 and 10^4 entries and about half of one at 10^5 (1.5 ms against
3.1 ms); unlike the copy, it is persisted, and every one stays readable.

**Where SQLite is the better choice:** a history question known in advance, served by an audit
table: 6-11x faster per row here. ProtoDB is the better choice when the question is not known in
advance: any query, any root, any field, without triggers, and without the audit table's write
cost on every change.

Object graphs
-------------

**Conceptual fit.** An inventory is a graph. ProtoDB stores each node as an object in a
dictionary per type with an equality index on the parent reference, and follows references by
dictionary reads; SQLite joins tables and walks paths with a recursive CTE. Both use the same
four indexes.

Telecom inventory (`Bulk load and scale`_), per request, [S0] at 10^5 objects and [INV] at 10^6
(GIL off; GIL on in brackets):

=============================================  ==========================  ==========================
Operation                                      10^5: ProtoDB / SQLite      10^6: ProtoDB / SQLite
=============================================  ==========================  ==========================
point lookup of a port                         37.7 / 3.7 us               45.6 (49.2) / 4.0 (4.4) us
equipment at a site (10 rows)                  116 / 6.4 us                132 (132) / 7.0 (8.2) us
ports at a site (3 index levels / 3-way join,  10.4 / 0.72 ms              10.3 (10.4) / 0.69 (0.96) ms
~2,900 rows)
path trace (up to 16 links, ~32 ports)         305 / 35 us                 407 (374) / 286 (290) us
=============================================  ==========================  ==========================

**Where SQLite is the better choice:** every graph operation measured here, by 1.3-19x. The
path trace narrows from 9x at 10^5 to 1.4x at 10^6, because SQLite's recursive CTE grows from
35 to 286 us while ProtoDB's walk grows from 305 to 407 us. ProtoDB's graph is plain Python
objects in the application, with no mapping layer, but it is not faster to traverse.

Bulk load and scale
-------------------

**The workload.** A network inventory: sites -> equipment -> cards -> ports, plus links between
ports, with history (the real system it models manages a few tens of millions of devices).
Fan-outs, chosen to be realistic and fixed: 10 equipment per site, 10 cards per equipment, 8-48
ports per card (a hash of the card id), and links on about half the ports (each joins two
sites; a cross-connect leads to the next equipment of the site, so a path trace crosses many
sites). Objects counts every persisted node (sites + equipment + cards + ports + links). Both
systems build the graph in one transaction per site, ProtoDB with ``durability="fsync"``,
SQLite in WAL with ``synchronous=FULL``. The answers are checked against a model computed from
pure functions of the ids.

===========================  ================  ================  =====================  ====================
Step (each a single run)     ProtoDB build     SQLite build      ProtoDB on disk        SQLite on disk
                             objects/s         objects/s         bytes / object         bytes / object
                             (commits/s)       (commits/s)       (alloc. / apparent)    (alloc. / apparent)
===========================  ================  ================  =====================  ====================
95,645 (28 sites) [S0]       3,458 (1.01)      386,009 (113)     147 / 423              15 / 31
958,665 (277 sites) [INV]    2,830 (0.82)      378,779 (109)     157 / 424              18 / 31
10^7 (2,769 sites)           did not fit [1]   --                --                     --
10^7 after Task 8.3m [RES]   2,051 [2]         --                --                     --
===========================  ================  ================  =====================  ====================

[1] Single run at ``8b439cd``, stopped at 5,921,408 objects by a retention defect of the
decoded-atom cache, fixed in Task 8.3m (see below).

[2] Single run at ``1845993``, ProtoDB only, default cache (50,000 loaded atoms), GIL off, under
``MemoryMax=8G``: 9,626,985 objects in 78 min (4,694 s), peak RSS 0.27 GB. Not directly
comparable with the rows above, which were measured in another session (the same session's
10^6 builds: ``26f3b85`` 1,974 objects/s, ``1845993`` 1,907).

Allocated is what ZFS reports after compression (``du``), apparent the file sizes. The 10^5
build is a single run; the 10^6 build a single run per GIL mode (GIL on: 2,775 and 322,266
objects/s).

**Cold against warm.** The first request of a new instance (open the storage, then the request;
the OS page cache and the ZFS ARC are not dropped, which needs root) against the same request
warm, at 10^6 [INV]: point lookup 1.68 ms against 46 us (SQLite 169 us against 4 us); equipment
at a site 2.35 ms against 132 us (SQLite 175 against 7 us); path trace 31.6 ms against 407 us
(SQLite 0.67 ms against 0.29 ms). ProtoDB's cold cost is decoding: a new instance decodes every
node it visits.

.. admonition:: Fixed in Task 8.3m: the cache now bounds loaded atoms

   The memory growth and the 10^7 limit described below came from a retention defect of the
   decoded-atom cache. Since Task 8.3m (D53 amendment, rulings R102-R104) the cache holds at most
   ``atom_cache_entries`` loaded atoms and unloads what it evicts. The same ladder [RES], default
   cache, GIL off (at 10^6 the last column gives both trees, ``26f3b85`` / ``1845993``):

   =================  =======================  =====================  =======================
   Objects            ``26f3b85`` peak RSS     ``1845993`` peak RSS   loaded atoms at the end
   =================  =======================  =====================  =======================
   958,665            2.61 GB                  0.26 GB                2,756,907 / 50,000
   2,972,442          --                       0.26 GB                50,000
   9,626,985          --                       0.27 GB                50,000
   =================  =======================  =====================  =======================

   The price is read speed when the working set does not fit: after the 10^6 build, with
   50,000 entries, a warm port lookup takes 0.93 ms (``26f3b85``: 78 us), the ports
   of a site 484 ms (15 ms), a path trace 29 ms (0.87 ms),
   because every walk reloads atoms. With ``atom_cache_entries=3_000_000`` (the working set
   fits) the reads take 75 us, 13.5 ms and 0.78 ms (single runs, as are the ``26f3b85`` figures:
   at or slightly below them, within this machine's run-to-run noise of about 8 %; part of any
   gain comes from the layout fixes of Task 8.3m, not from residency), and the build peaks at
   4.09 GB for 2.79 million loaded atoms: 1.57x the base's peak for the same atoms.
   The end RSS (3.31 against 2.18 GB) puts the residency bookkeeping (identity map, LRU entry,
   generation token) at about 0.4 KB per resident atom; the split among them was not measured. Size the cache to the working set when reads matter more than memory
   (``ObjectSpace`` docstring, D53 "Sizing"). The warm figures of `Object graphs`_ at 10^6
   were measured before the fix, when every atom stayed loaded.

**Memory, and the limit of the ladder.** The building process's peak RSS at 10^6 objects was 2.64 GB with the GIL
off and 2.59 GB with it on (``/usr/bin/time -v``, both systems in one process), about 2.7 KB per
object built.

The 10^7 step ran under ``MemoryMax=20G`` with swap barred. It was stopped deliberately after
35 minutes, when available memory fell under 8 GB (earlyoom on this machine acts at 10 % of RAM).
At that point it had committed 5,921,408 objects (1,496 of 2,769 sites) and its peak RSS was
14.87 GB, still growing linearly at about 2.5 KB per object. Its extrapolated peak (about 26 GB)
exceeds the cap. **At commit 8b439cd, on this machine, the ladder stopped between 10^6 and 10^7
objects** (``2026-11-06-8b439cd-inventory-ladder.log``), because of the defect described next
(fixed since; see the box above). The limit is not the data: the same database,
reopened by a new process, read its counts with a peak RSS of 32 MB.

**What holds the memory: the decoded-atom cache pins whole subtrees.** A deterministic
measurement (``benchmarks/memdiag.py``, gc object counts by type at 25 %, 50 % and 100 % of a
build, ``2026-11-06-8b439cd-inventory-memdiag.jsonl``) shows:

- With the default space (``atom_cache_entries=50_000``), the live ``DBObject`` count equals the
  number of objects built: 129,489 / 257,981 / 480,894 against 129,475 / 257,930 / 480,815. AVL
  ``KeyNode`` and ``CountedKeyNode`` counts grow with them. The cache holds exactly 50,000
  entries, and RSS grows 345 -> 652 -> 1,283 MB.
- The same build with ``atom_cache_entries=0`` stays at 62 -> 72 -> 88 MB and about 35,000 gc
  objects, and is not slower (70 sites in 71.5 s against 85.8 s).
- A referrer walk (``benchmarks/memdiag_refchain.py``) from an equipment object of site 0 at the
  end of a 20-site build finds 68,841 live ``DBObject`` instances. None of them is a cache
  entry, yet each is reachable from a cached AVL node through 6 ``KeyNode`` child links.

The mechanism: since Task 8.3i the cache receives the decoded twin of every atom a commit
saves (``_AtomCache._atom_saved``). A twin's children are resolved through the same cache, so a
cached interior node refers strongly to its decoded children, and the newest roots are always
cached. The cache bounds its *entries*, but eviction frees nothing that a cached ancestor still
reaches, so a writing process keeps every node it ever wrote decoded. This contradicts the
reading of D53 / 8.3l that resident memory is bounded by the decoded-atom cache. It is a
ProtoDB retention defect, not a property of the workload (the workload keeps only the current
site's rows). It is fixed by Task 8.3m (R102): evicted atoms are unloaded in place, so a cached
ancestor reaches its children as stubs (box above). The 9c inference that the same live heap
made ``parallel_scan`` stop scaling inside the full suite was not confirmed (`Parallel work`_).

**Where SQLite is the better choice:** loading data: 112x (10^5) and 134x (10^6) faster per
object, and 9.6x and 8.6x smaller on disk as allocated (13.6x in apparent size at both steps). A data set that is loaded in bulk and rarely changed belongs in SQLite.

Single-process commits
----------------------

One small read-modify-write commit (read a counter, increment it, insert one key) per durability
mode, one process, ``FileStorage`` against SQLite in WAL [S0] (GIL on [S1]):

==================  ====================  ======================  ====================
ProtoDB durability  commits/s             SQLite ``synchronous``  commits/s
==================  ====================  ======================  ====================
``fsync``           159.5 (157.0)         ``FULL``                590 (630)
``flush``           215.3 (214.5)         ``NORMAL``              12,405 (12,118)
``none``            786.1 (782.3)         ``OFF``                 21,476 (21,262)
==================  ====================  ======================  ====================

``fsync`` and ``FULL`` are the like-for-like pair (both survive power loss; ProtoDB's crash
matrix checks ``fsync``). ``flush`` and ``NORMAL`` are process-crash safe, not power-loss safe
(D19); ProtoDB ``flush`` still syncs its root file and directory, which buys no data safety.

Commits of one process, ``FileStorage`` ``none``, commits/s at 1 / 4 / 16 threads [S0]
(GIL on [S1]): conflicting counter 987 / 869 / 712 (1,040 / 691 / 300), with 1.0 / 1.47 / 3.94
attempts per commit; blind merges 583 / 693 / 726 (616 / 535 / 477); disjoint roots 1,053 /
1,162 / 1,164 (1,019 / 883 / 759).

**Where SQLite is the better choice:** single-process commits at every durability level, by
3.7x (``fsync``) to 58x (``flush``). Bulk insert of 10,000 records: ProtoDB 142 us per record
without indexes and 514 us with two, SQLite 2.7 and 4.7 us [S0]. Each index adds about 180-220 us
to a ProtoDB update at 10^4 rows.

Parallel work
-------------

``parallel_scan`` (2,000 items, a CPU-bound predicate, ``ParallelPool``), GIL off. Run
alone, it scales: 0.98 / 1.80 / 3.33 / 4.59 / 4.93x the sequential loop at 1 / 2 / 4 / 8 / 16
workers (``2026-11-06-4aab0f2-parallel-alone-gil-off.json``). Inside the full suite run [S0] the
same code gave 1.03 / 0.82 / 1.07 / 1.15 / 1.12x. The suite runs it after workloads that leave a
large live heap in the process (the 10^6-row pagination and query-cache data sets, kept decoded
as described under `Bulk load and scale`_). The free-threaded collector's stop-the-world passes
then grow with that heap. That cause was inferred from the alone/in-suite contrast and is not
confirmed: Task 8.3m ran ``parallel_scan`` after the 14 preceding workloads at reduced sizes, on
``26f3b85`` and with the residency fix, and it scaled in both (5.21x and 5.01x at 16 workers,
with 1.9-2.1 million live gc objects). The full-size collapse has not been reproduced since,
and the retention fix is not expected to explain it. *History:* the 2026-11-03 suite (``8ad6bf3``, dirty tree) reported 4.15x at 8
workers.

``concurrent_reads``, GIL off [S0], *measured before Task 8.3m* (``4aab0f2``): ``get_at`` 208k ->
345k ops/s at 8 threads (1.65x), EqualityIndex queries 4.26x at 16 threads, a Python ``dict``
4.71x. Alone it gives 1.53x and 3.48x, so the ``get_at`` ceiling is not a suite effect. With the
GIL on [S1] nothing scales (0.90-1.0x). Since Task 8.3m its working set (60,117 atoms) is about
20 % over the default capacity. Against the pre-8.3m code (``26f3b85``, 5 interleaved runs per
GIL mode, median of the per-run ratios and their range): the 1-thread ``get_at`` rate is 0.59x
[0.38-0.66] with the GIL off and 0.62x [0.53-0.65] with it on, with 67 % of its time in
reloads; at 2-16 threads, measured after it, it runs 1.46-2.18x [1.44-2.54] with the GIL off
but 0.99-1.18x [0.75-1.40] with it on, and from 4 threads on it reloads nothing (Task 8.3m
report).

Vector search [S0] (GIL on [S1]): 10,000 vectors of 32 dimensions, k = 10, recall 1.0 for every
backend; per query: numpy brute force 1.21 ms, ``exact`` 0.78 ms (0.74), ``ivf_flat`` 1.24 ms
(1.28), ``hnsw`` 0.66 ms (0.63), the last with a lazy graph build of 0.85 s on the first query.

Operations
----------

- **Object-store latency stays off the commit path** (D46). 3 in-process peers, blind adds:
  434 [429.7-436.5] commits/s with a 0 ms store and 391 [144.2-446.7] with a 30 ms store,
  ratio of medians 0.90 [S0]. The 30 ms runs include one at 144 commits/s, so on this evidence a
  slow store is not always free; three runs cannot say how often that happens. With the GIL on
  the figures are 209 [201.3-215.5] and 215 [213.4-216.6], ratio 1.03 [S1]. The CAS-only baseline (writers arbitrated by the store's
  conditional write, the reverted D5 design) falls to 6.6 commits/s with one writer and 6.2 with
  four at 30 ms (p95 3.6 s, 0.17-0.26 failed root CAS per commit) [S0].

  The GIL-on ratio of 0.74 that Task 9 reported (``8ad6bf3``, dirty tree) is **not reproduced**
  at a clean commit (1.03 [S1]); it is closed as noise of that run or as an effect of a commit
  path that has since been replaced (D50-D55), not diagnosed further.
- **Open and recovery** [S0]: open a 20,000-record space and read one record 1.47 ms; reopen a
  TieredStorage local tier with 500 commits 3.2 ms; a cold walk of every record 71 us per
  record, a warm walk 0.4 us.
- **In-process peers** [S0] (GIL on [MX1]): blind adds at 1 / 2 / 4 / 8 peers in one process
  337 / 429 / 390 / 255 commits/s (308 / 160 / 179 / 129). In the full GIL-on suite run [S1]
  ``cluster_peers`` failed: a commit on a one-peer cluster got ``ProtoOutcomeUnknownError``
  (the acknowledgement was counted after the lease ended), and the benchmark thread stopped.
  The GIL-on rerun at ``8b439cd`` [MX1] passed. The cause is the one described under `Mixed
  reads and writes`_: under the GIL a thread can be descheduled past the timing margin; the
  outcome is a conservative failure, not a lost commit.
- **Server against embedded peer.** rqlite is a server: any client that speaks HTTP reaches it,
  and its nodes can run on machines the application does not. A ProtoDB peer is a library inside
  a Python application: no server to deploy, no client round trip, but every writer and reader
  is that application's process.

Reproducing
-----------

::

    .venv314t/bin/python -m benchmarks.export <commit> benchmarks/.work/export-<commit>
    cd benchmarks/.work/export-<commit>
    ../../../.venv314t/bin/python -m benchmarks --rqlite <rqlited> --workdir ../run --json ../suite-gil-off.json
    ../../../.venv314t/bin/python -X gil=1 -m benchmarks --rqlite <rqlited> --workdir ../run --json ../suite-gil-on.json
    ../../../.venv314t/bin/python -m benchmarks.session --reps 5 --rqlite <rqlited> --workdir ../run --json ../session.json
    ../../../.venv314t/bin/python -m benchmarks --only inventory --param inventory.objects=1000000 --workdir ../run

Run the inventory ladder under a memory cap without swap, for example
``systemd-run --user --scope -p MemoryMax=20G -p MemorySwapMax=0 ...``. A full suite run took
31 minutes per GIL mode on the machine above, the session 23 minutes, a 10^6 inventory step 6
minutes.
