Performance
===========

This page reports the ``proto_db`` 2.0 performance suite (spec section 12, decision D49) by
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

- **Every current figure was measured at commit** ``57fd351``, from a clean ``git archive``
  export (``python -m benchmarks.export``). Each results file records the commit and a clean flag
  that the harness re-verifies against a SHA-256 of every exported file. ``57fd351`` includes
  Task 8.3m (the decoded-atom cache bounds loaded atoms), its fix rounds, the cluster fixes of
  Task 8.3n, the language-neutral atom type names (D57) and storage format 4 (D58). Figures from
  earlier commits appear only as labelled history, with their commit.
- **Every figure is verified, in every repetition.** Each workload checks its work against an
  independent computation: a brute-force scan, a model computed from pure functions, a read-back
  of the persisted state by a new instance, or a strongly consistent query on rqlite. One wrong
  repetition fails the run, and a failed run is reported, never averaged in. Every one of the
  400 runs of the session [SES] was verified. Two cluster workloads failed once in the suite
  with a conservative unknown outcome; they are reported where they belong (`Operations`_).
- **Medians and ranges.** The suite runs 5 timed repetitions after 1 warm-up (bulk loads 3; the
  cluster workloads 3 runs of 3 s; the rqlite workloads 2). The interleaved session runs every
  cell 5 times, alternating the GIL mode cell by cell. Figures are medians, with the range of the
  runs in brackets (``[min-max]``). A figure from one run is marked *single run*: the inventory
  builds (one per step and GIL mode) are single runs.
- **GIL off is the default**; figures with the GIL on (``-X gil=1``) follow in parentheses or in
  their own column.
- **One machine, one disk.** All cluster peers and all rqlite nodes share one laptop CPU and one
  ZFS pool, so their syncs queue on the same device and their threads on the same 6 cores. A real
  cluster has a machine per node.
- **Frequency scaling was on** (governor ``powersave``, ``amd-pstate-epp``, EPP
  ``balance_power``, boost on). Differences under about 15 % between runs of different sessions
  are within this machine's noise.
- Memory figures (RSS) appear only where memory is the limit being measured (the inventory at
  10^6 objects).

Environment
~~~~~~~~~~~

=============================  =====================================================
CPU                            AMD Ryzen 5 5500U, 6 cores / 12 threads
CPU frequency                  ``amd-pstate-epp``, governor ``powersave``, EPP
                               ``balance_power``, boost on
Memory                         62 GiB, 23-35 GiB available during the runs, swap unused
OS / file system               Linux 7.0; every work directory on ZFS, never tmpfs
Python                         CPython 3.14.6, free-threaded build; GIL off (default) and
                               ``-X gil=1``
proto_db                       2.0.0.dev0 at ``57fd351``
SQLite                         3.53.1 (bundled with the interpreter)
rqlite                         v10.6.6, official linux-amd64 release binary (run from a
                               private directory, never installed), rqlite's defaults
=============================  =====================================================

Every heavy run ran alone, under ``systemd-run --user --scope -p MemoryMax=12G
-p MemorySwapMax=0``, after a check that no other benchmark, test or ``rqlited`` process was
running; the check is stored in every results file (``check``). It is made before and after a
run, not during it, and the processes it lists as strays in some files are the harness's own
driver shells (the controller's, and the queued driver of the bisect reruns), not benchmarks.
Swap stayed at 0 MB.

Results files
~~~~~~~~~~~~~

Each figure below cites one of these files (``benchmarks/results/``), all measured at
``57fd351``:

==============  ===============================================================  ======================================
Tag             File                                                             Content
==============  ===============================================================  ======================================
[S0] / [S1]     ``2026-11-10-57fd351-suite-gil-off.json`` / ``-gil-on``          the full suite (25 workloads)
[SES]           ``2026-11-10-57fd351-session.json``                              interleaved session: cluster headline,
                                                                                 mixed and cache cells; 5 repetitions,
                                                                                 both GIL modes
[INV]           ``2026-11-10-57fd351-inventory-1000000-gil-off.json``,           inventory at 10^6 objects, default
                ``-gil-on``, ``-fit-gil-off``                                    cache and a cache that holds the
                                                                                 working set
[RR]            ``2026-11-11-57fd351-*.json``                                    workloads rerun alone (see below)
[BIS]           ``2026-11-11-bisect-concurrent-commits-*.json``,                 the bisection of the commit paths
                ``2026-11-11-bisect-cluster-tcp-*.json``                         (`Changes since the previous
                                                                                 measurement`_), from exports of
                                                                                 five commits
==============  ===============================================================  ======================================

**Reruns [RR].** A focused test run on the same machine (20:36-20:41) overlapped the GIL-on
suite while it ran ``deep_page`` and ``query_cache`` (the windows come from each record's
``elapsed_s``). Both were rerun alone, and this page takes their GIL-on figures from the rerun.
``parallel_scan``, ``cluster_store_latency`` and ``cluster_peers`` were also rerun alone, for
the reasons given in their sections. The driver log ``2026-11-10-task9c-carry-runs.log`` records
every run (including the superseded rounds at ``585ab3a``), its memory and swap before and after, and its scope's memory peak.

*History.* The previous measurement (``4aab0f2`` / ``8b439cd``, 2026-11-06, before Task 8.3m)
is kept in the same directory (``2026-11-06-*``). Its figures are not current.

Changes since the previous measurement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Against the full suite of ``4aab0f2`` (two single-session suite runs, not interleaved; the
SQLite, Python and rqlite figures of the two runs are level, which rules out a slower machine):

- **The suite figures of proto_db's commit paths fell to 0.41-0.85x**, most with the GIL off:
  single-process commits (``concurrent_commits`` 0.41-0.88x, ``commit_durability`` ``none``
  0.65x with the GIL on), cluster commits (``cluster_tcp`` 0.61-0.76x, ``rqlite_reference``'s
  ``proto_db`` side 0.61-0.73x), ``cold_open`` 0.42-0.80x and ``bulk_insert`` 0.78-0.81x. The 3-peer
  session headline went from 285.8 commits/s (``8b439cd``) to 202.5.
- **To separate code from context, the two workloads were rerun alone** from clean exports of
  five commits, interleaved commit by commit, 2 rounds, GIL off [BIS]. Speed against ``4aab0f2``,
  geometric mean over the workload's cells, per round:

  ====================  ===============================  ===============================
  Commit                ``concurrent_commits`` (20       ``cluster_tcp`` (10 cells)
                        cells)
  ====================  ===============================  ===============================
  ``1845993`` (8.3m)    0.92 / 0.95                      --
  ``550dde5`` (8.3m     1.06 / 0.85                      0.86 / 0.97
  fix rounds)
  ``5120cff`` (8.3n     0.95 / 0.88                      0.81 / 0.92
  cluster fixes, D57)
  ``57fd351``           1.03 / 0.84                      0.78 / 0.92
  ====================  ===============================  ===============================

- **Single-process commits: no regression is shown alone.** The two rounds disagree by up to 20
  points for the same commit (``4aab0f2`` itself gives 701 and 822 conflicting commits/s at 1
  thread), so a loss of up to about 15 % cannot be excluded, but nothing like the suite's
  0.41-0.88x. Alone, ``57fd351`` gives 857 / 782 conflicting commits/s at 1 thread, against 548 in
  its own suite run [S0]: **in the suite, these workloads run after others that leave the process
  slower, and that effect is larger at** ``57fd351`` **than it was at** ``4aab0f2``. Its cause was
  not measured; it is the same open question as ``parallel_scan`` (`Parallel work`_). Read the
  single-process commit rates of [S0] / [S1] on this page as in-suite figures: alone they are up
  to 1.6x higher.
- **Cluster commits: a regression of 8-22 % is shown alone**, in both rounds. It is largest for commits carrying a
  value (100 B: 191 / 209 commits/s against 288 / 244; 0.66 / 0.86x). The suite's 0.61-0.76x and
  the session's 0.71x exceed it; the rest is the in-suite and session-to-session effect above.
  *Before Task 8.3m closed*, its own report measured 9-11 % fewer cluster commits with the GIL
  off against ``26f3b85``.
- **Where the cluster-commit loss comes from (a later bisection, R136).** The two-round
  bisection above could not place it. A second one, 14 paired interleaved rounds per commit, run
  one at a time under a CPU gate on a machine shared with other work (driver
  ``benchmarks/bisect_commit_path.py``, ``5b50252``; its result files are not committed, so
  these figures are from a single session), found **two steps and nothing else**. Speed of
  ``c953d7e`` against ``4aab0f2``: 0.898x for a blind add and 0.846x for the 100 B cell. The
  steps are Task 8.3m's residency (``7ef1636`` and ``700d48f``, with its fix rounds up to ``550dde5``:
  0.967x blind, 0.919x at 100 B), and ``0eed600`` (the ``class_defines`` check of the
  pre-tag review fixes: 0.965x blind, 0.936x at 100 B). **Task 8.3n's cluster commits, including the
  safety fixes (the send-reserve check, the back-off change, the majority-ended recovery round,
  the C1 floor), cost nothing measurable**: 550dde5 to ec90c25 gave 0.993x blind and 0.987x at
  100 B, with sign counts at chance, and the per-commit counters (syncs, frame bytes, lease
  attempts, abandoned terms) did not move. What grows is the build inside the critical section
  (validation, replay and path copy: 2.82 to 3.71 ms per commit at 100 B), not the wire or the
  disk. With 14 rounds, an effect of a single commit below about 3 % cannot be excluded. The
  earlier attribution of a step to the 8.3n cluster commits (``550dde5`` to ``5120cff``) does
  not reproduce.

The read paths are level or faster (``point_lookup``, ``deep_page`` pager, ``concurrent_reads``
from 4 threads on 1.37-1.75x); exceptions under 0.85x are the unranked ``islice`` scan (0.84x),
``topk`` by city (0.80x) and the inventory build and warm port lookup at 10^5 (0.69x and 0.80x),
in-suite figures with the same caveat.

Profiles at a glance
--------------------

==========================================  ==========================================  ==========================================
Profile                                     Where proto_db fits                         Where the other system is the better choice
==========================================  ==========================================  ==========================================
`Reads across transactions in a cluster`_   readers on other nodes keep a valid cache   rqlite when proto_db's cache cannot hold
                                            across every commit: 0.07 decodes per       the working set (cache off: 2,112 reads/s
                                            read, 54,930 reads/s, p50 44 us             against rqlite ``none`` 5,183), and when
                                                                                        reads must be linearizable
`Writes in a cluster`_                      an embedded peer: no server hop, data       rqlite: 2.1-24x the rows/s on every
                                            built where it is written                   commit shape measured here
`Mixed reads and writes`_                   reads never wait and never leave the node:  rqlite when reads must be linearizable,
                                            p50 22 us (37 us under writes); but         and when readers share the writers'
                                            readers take the writers' CPU: commits      CPU: its writers lose 0-5 % (1 reader
                                            -34 % (1 reader per node), -72 % (4)        per node) to 34 % (4)
`Point reads`_                              (on par for a primary key)                  SQLite: secondary-index and ad-hoc reads
                                                                                        3-56x faster
`Deep pages`_                               random jumps to any page: flat ~225 us      SQLite keyset for next-page browsing
`Repeated queries`_                         a repeated query on an unchanged root:      SQLite for a first, ad-hoc query
                                            served by pointer in ~4 ms
`History and "as of"`_                      any past version, every query, no           SQLite with an audit table when the
                                            schema work; current queries do not slow    history question is known in advance
                                            down as history grows
`Object graphs`_                            (functional, not fast)                      SQLite joins and recursive CTEs
`Bulk load and scale`_                      memory flat in the database size            SQLite: 140-150x faster to load, 8.7x
                                                                                        smaller (allocated)
`Single-process commits`_                   ``none``/``flush`` with free threading      SQLite at matched durability
`Operations`_                               object-store latency off the commit path;   rqlite: a server any client can reach
                                            peers embedded in the application
==========================================  ==========================================  ==========================================

Reads across transactions in a cluster
--------------------------------------

**Conceptual fit (D53, D56, P1; ruling R110).** A ``proto_db`` storage pointer names one content
forever: records are appended, never rewritten. A node's decoded-atom cache is keyed by pointer,
and so is its query result cache, so neither ever holds a stale entry and neither needs an
invalidation, on that node or from any other node. When node A commits, a reader on node B
learns a new root; the next read on B decodes only the atoms that are new in that root and that
the read actually reaches (the path from the root to the changed entry). Everything else it
reaches is a cache hit, by pointer, exactly as before the commit. A query on a collection the
commit did not touch keeps its pointers, so its cached result is still the answer.

This is a consequence of immutability, not something only ``proto_db`` has. **Datomic's** peers rely
on the same property: "Datomic caches only immutable data, so all caches are valid forever"
(https://docs.datomic.com/operation/caching.html). **XTDB** keeps "a local-disk cache and
in-memory cache of the working set" over a remote object store
(https://docs.xtdb.com/ops/config/storage); its current storage page does not say whether those
cached files are immutable or how they are invalidated, so we make no claim about it.

Systems that store data mutably pay for the same knowledge differently. Each statement below is
taken from the system's current documentation:

- **SQLite** (and therefore each rqlite node): each process that reads a database keeps a page
  cache, and "A process will normally want to flush its database page cache when another
  process modified the database, since the cache has become stale." In rollback mode the file
  change counter tells it so; "In WAL mode, changes to the database are detected using the
  wal-index" (https://www.sqlite.org/fileformat2.html). rqlite runs one SQLite database per
  node. A follower may serve a read "directly and not contact the Leader", depending on the read
  consistency level, and a write sent to a follower is forwarded: "the receiving node
  transparently forwards the request to the Leader" (https://rqlite.io/docs/api/api/).
- **PostgreSQL** hot standby: changes become visible on a standby once their commit record "is
  replayed"; and because "cleanup on the primary will remove row versions that are still visible
  to a transaction on the standby", WAL replay can conflict with standby queries, which are then
  canceled after ``max_standby_streaming_delay`` unless ``hot_standby_feedback`` holds the cleanup
  back on the primary at the cost of "table bloat"
  (https://www.postgresql.org/docs/current/hot-standby.html, PostgreSQL 18). The primary does not
  know which old versions a standby's readers still need: that is the cost of not knowing who
  reads the history.
- **MongoDB** (WiredTiger): the cache belongs to the ``mongod`` instance ("WiredTiger allocates
  its cache to the entire mongod instance"), and reads use "a point-in-time snapshot" kept by
  MVCC for a bounded history window (https://www.mongodb.com/docs/manual/core/wiredtiger/);
  secondaries "copy and apply" the primary's oplog operations "in an asynchronous process"
  (https://www.mongodb.com/docs/manual/core/replica-set-oplog/), so each member keeps its own
  copy current by applying changes in place.

What these documents support is narrower than "nothing like it exists": in each mutable system a
node's cache is valid for that node's current state, and other nodes' writes reach it as changes
applied in place (replay, oplog application) or as a signal to flush; none of the pages cited
describes a cache that stays valid across nodes because its entries can never change. That is
the property measured here, and Datomic's documentation says it has it too. Only rqlite was
measured; the other systems are cited for their documented design, not compared by number.

**Setup** (``cluster_cache``). 3 members (TCP peers, ``TieredStorage`` ``fsync``). 4 writer
threads on node A update random keys of a 20,000-entry ``catalog`` (one key per commit); 2
reader threads on each of nodes B and C read, each read in a new transaction, either a random
catalog key (*point*) or ``products where cat == c`` (*query*: 50 of 5,000 products in 100
categories, an indexed collection the writers never touch). 3 s per run. Every read is checked;
the final catalog is read back from the store by a new instance; each reader node must report
that its reads crossed more than one database version (``versions_seen``): the reads did cross
roots written on another node. *Decodes per read* counts every atom decoded from stored bytes
on B and C (``benchmarks/decode_counter.py``), over the reads. rqlite runs the same work: 4
writer clients on node 0, 2 reader clients on each of nodes 1 and 2, the same point read and
the same query (``SELECT id FROM products WHERE cat = ? ORDER BY id``, index on ``cat``) at each
read consistency level. Medians [range] of 5 interleaved runs [SES], GIL off:

=========================================  ==========  ===============  =========  ==========  ============  ===========
Cell (2 readers on each of B and C)        commits/s   reads/s          read p50   read p99    decodes per   query cache
                                           on A                         (us)       (us)        read          hit rate
=========================================  ==========  ===============  =========  ==========  ============  ===========
proto_db point, atom cache on (50,000)     76.6        54,930           44         446         0.07          --
                                                       [48,830-70,990]
proto_db point, atom cache small (2,000)   63.3        4,755            738        2,729       4.56          --
proto_db point, atom cache off (0)         50.6        2,112            1,663      4,235       20.36         --
proto_db query, query cache on (64 MiB)    77.6        5,842            588        1,869       0.25          1.0
proto_db query, query cache off            73.4        5,048            723        2,018       0.26          --
rqlite point ``none``                      204.1       5,183            685        1,834       --            --
rqlite point ``weak``                      205.7       3,614            958        2,832       --            --
rqlite point ``linearizable``              230.7       452              9,425      23,930      --            --
rqlite point ``strong``                    208.3       192              20,520     32,190      --            --
rqlite query ``none``                      209.9       4,637            778        1,888       --            --
rqlite query ``weak``                      194.1       3,149            1,126      3,408       --            --
rqlite query ``linearizable``              209.6       401              10,170     25,380      --            --
rqlite query ``strong``                    204.3       199              20,230     29,440      --            --
=========================================  ==========  ===============  =========  ==========  ============  ===========

What the numbers show:

- **The cache stays valid across the writers' commits.** With the default cache a point read on
  B or C decodes 0.07 atoms: almost every atom it reaches is still in the cache although node A
  committed 77 times a second. Per commit, each reader node decoded about 27 atoms (``decodes
  per new version`` 26.8 [26.3-27.2]) of the 58 that each commit writes on A: only the new path
  that its reads reached. With the cache off the same read decodes 20.4 atoms (a path from the
  root of the catalog to the entry) and runs 26x slower (2,112 reads/s). The cache's effect is
  the difference between those two cells, on the same work.
- **The query cache holds under writes to another collection:** hit rate 1.0 in every run while
  A commits to ``catalog``. On this query the gain is small: +16 % reads/s and -19 % p50
  (5,842 against 5,048 reads/s). The query is cheap (50 rows from an index whose atoms are
  already in the atom cache: 0.25 decodes per read either way), so there is little left to save.
- **A cache below the working set thrashes.** At 2,000 entries (the catalog alone is about 20,000
  tree nodes) a read decodes 4.6 atoms, every load evicts another loaded atom (thrash signal 1.0,
  ``atom_cache_stats()``), and the rate falls to 4,755 reads/s, 11.5x below the default size.
  Size ``atom_cache_entries`` to the working set (D53 "Sizing").
- **Against rqlite, which reads through HTTP:** with its cache, a ``proto_db`` point read is 10.6x
  the rate of an rqlite ``none`` read (no freshness guarantee) and 15.6x faster at p50, and 120x
  the rate of a ``linearizable`` one. **Without its cache, proto_db is slower than rqlite**
  ``none`` (2,112 against 5,183 reads/s): it is the immutable cache, not the embedded read
  alone, that makes the difference. A ``proto_db`` read is a snapshot read, the same class as
  rqlite ``none`` (`Mixed reads and writes`_ gives the caveats).
- **The writers on A pay for the readers on B and C.** All 9 processes share 6 cores: A commits
  77 times a second with the readers running, while 4 rqlite writer clients on node 0 commit
  about 205 times. Fewer reads mean more commits (50.6 commits/s with the cache off, where reads
  are slowest, is the exception: decoding costs the reader nodes' CPU too).
- **With the GIL on, the writers on A nearly stop**: 10.1-11.8 commits/s in every ``proto_db`` cell
  (rqlite: 196-215), while the readers keep their rate (point, cache on: 55,450 reads/s, p50
  23 us, 0.006 decodes per read). The readers on B and C share their process's interpreter with
  that node's replication threads, which acknowledge A's commits; our reading is that the
  readers hold the interpreter and the acknowledgements wait (the same effect as in `Mixed
  reads and writes`_; its cause was not measured further).

The suite's own runs of this workload ([S0], [S1], 3 runs per cell) agree in order with the
session.

**Where rqlite is the better choice.** When reads must be linearizable, rqlite provides it by
configuration and ``proto_db`` has no measured equivalent. When the readers' working set does not fit
the memory a ``proto_db`` node can give its cache, its reads decode from storage and are slower than
rqlite's. When readers and writers share interpreters with the GIL on.

**Where proto_db fits.** Many readers on nodes other than the writer, reading a working set their
cache can hold: each reader's cache survives every commit without an invalidation message, and
pays only for the new atoms its reads reach.

Writes in a cluster
-------------------

**Conceptual fit.** rqlite is a replicated SQLite on Raft: the leader orders every write, appends
it to a log that it fsyncs, replicates it, and applies it to SQLite. Each write request is one
Raft entry (our reading of the design; whether entries share a sync is not documented). ``proto_db``
peers are embedded in the application: each node builds its own commits (path copies of
persistent structures), asks the owner of the term for a write permission, groups the
transactions pending on that node into one round, and replicates one self-describing append to
its data replicas (D50, D54, D55). The ``proto_db`` commit has no client-server hop, but it has more
work per commit in Python (build, merge, serialize) and a permission round between nodes.

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
Raft log of a majority, which would put it in the same durability class as ``proto_db`` with
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

3 members, 4 client threads per member, 3 s per run; ``proto_db`` TCP peers with
``durability="fsync"``; each commit (or rqlite request) writes one 100-byte value and one id, or
50 ids and one 100-byte value. Medians [range] of 5 interleaved runs [SES]:

=====================================  =============================  =============================  ===================
Work                                   proto_db commits/s             rqlite requests/s              p50 latency (ms)
                                       GIL off (GIL on)               GIL off (GIL on)               proto_db / rqlite
=====================================  =============================  =============================  ===================
1 id + 100 B, 3 members                202.5 [175.3-252.3] (153.5)    568.6 [559.8-594.8] (574.9)    55.8 / 21.2
50 ids + 100 B, 3 members              21.1 [19.0-24.5] (19.5)        515.7 [436.5-538.6] (506.9)    589 / 22.7
  rows/s                               1,057 (976)                    25,790 (25,350)
1 id + 100 B, 5 proto_db peers         190.9 [186.5-236.5] (157.4)    --                             96.5 / --
=====================================  =============================  =============================  ===================

``proto_db`` syncs per commit, counted in every peer (``benchmarks/fsync_probe.py``): 1.73 at 3 peers
and 2.24 at 5 (GIL off); 6.79 for the 50-id commit [SES].

Payload size, 3 members, one value per commit, measured side by side in ``rqlite_reference``
[S0], GIL off (GIL on [S1]):

==========  ==========================  ==========================  ===============================
payload     proto_db commits/s          rqlite requests/s           loopback bytes per commit
                                                                    ``proto_db`` / rqlite
==========  ==========================  ==========================  ===============================
100 B       182.6 (202.1)               565.4 (596.3)               13.2 KB / 1.8 KB
10 KB       179.5 (176.9)               526.9 (573.6)               32.8 KB / 18.6 KB
100 KB      159.5 (174.7)               328.8 (367.2)               217 KB / 172.4 KB
==========  ==========================  ==========================  ===============================

The same file's per-request cell (one id, no value): ``proto_db`` 290.9 (245.2) against rqlite 588.5
(590.4); batched, 50 rows per commit or request, 999 (1,218) rows/s against 24,750 (26,800).

Peer scaling, TCP processes, 4 threads per peer, one id per commit, ``fsync`` [S0] / [S1]:
224 / 246 / 249 / 259 / 201 commits/s at 1 / 2 / 3 / 4 / 8 peers with the GIL off; 302 / 187 /
246 / 264 / 217 with it on. Syncs per commit, cluster-wide: 0.97 (1 peer), 1.64 (3), 2.55 (8).
At 3 peers the ``flush`` and ``none`` modes give 265 and 257 commits/s: they skip the data syncs
but still sync the acceptor log, so the disk is not what separates them from ``fsync`` on this
machine.

*History:* the ``8b439cd`` session measured 285.8 commits/s for the 3-peer headline (GIL off);
the current 202.5 is the regression described under `Changes since the previous measurement`_.
Task 9 measured 61 commits/s (``8ad6bf3``, dirty tree).

**Where rqlite is the better choice.** On every commit shape measured here rqlite commits more:
2.8x at one row per commit, 24x in rows for 50-row commits, and 2.1x at 100 KB values, with
less than half the latency. A 50-id ``proto_db`` commit spends most of its 589 ms building and
replicating 50 path copies in Python; rqlite ships 50 SQL statements in one log entry. A
workload of large transactions, or of many small writes from clients that are not Python
processes, belongs on rqlite.

**Where proto_db fits.** The writer is the application process itself, with no server to run or
reach. The 100 KB payload moves about 1.26x rqlite's bytes, not more. With the GIL off,
throughput stays within 200-260 commits/s from 1 to 8 peers, although syncs per commit rise from
0.97 to 2.55. The design reason is that only ``f + 1`` replicas sync the data (D54); that cause
was not measured here.

Mixed reads and writes
----------------------

**Conceptual fit (ruling R97).** A ``proto_db`` read never waits: it opens a transaction on the root
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

**Honest caveats.** A ``proto_db`` read is a *snapshot* read of the node's last known committed root.
It is not linearizable unless something on the reading node first learns from the term owner;
the only public way today is a barrier commit on the reading node before the read (its base is
at or above every acknowledged commit), which costs a write round and is not measured here. It is
a barrier only if it writes at least one root and returns normally: a transaction that writes
nothing returns at once and learns nothing, and an unknown outcome, a conflict or a timeout is no
barrier (see ``ObjectSpace.sync``). A read-only barrier has no API yet (Task 8.3n, ruling
proposal on R113). ``sync()`` is a durability barrier and does not make another node's commits
visible. Otherwise a ``proto_db`` read is the same class as rqlite ``none`` with the difference that
its snapshot is always a complete committed version. A ``proto_db`` read whose records are not in the
node's caches fetches them, from the local tier or, on a node that is not a data replica of them,
from a peer (D54): that read waits for I/O. The ``proto_db`` readers are threads of the peer process;
the rqlite readers are Python HTTP clients in the benchmark process, so every rqlite read
includes an HTTP round trip that an embedded read does not.

**Setup** (``mixed_read_write``). 3 members; a catalog of 10,000 entries loaded first and
visible on every node before the window; then, for 3 s, 4 writer threads per node (one id and
one 100-byte value per commit, ``fsync``) and/or 1 or 4 reader threads per node reading random
catalog entries. Every read is checked against the catalog. Rates are totals over the 3 nodes;
latencies are over every read of every node. Medians of 5 interleaved runs [SES], GIL off:

=======================================  ===============  ===============  ==============  ===========================
Cell (threads per node)                  commits/s        reads/s          read p50 (us)   read p99 (us)
=======================================  ===============  ===============  ==============  ===========================
proto_db, 4 writers                      213.2            --               --              --
proto_db, 1 reader                       --               92,030           22              160
proto_db, 4 writers + 1 reader           139.9            50,190           37              347
proto_db, 4 readers                      --               89,330           58              993
proto_db, 4 writers + 4 readers          59.2             66,930           55              1,809
rqlite, 4 writers                        561.7            --               --              --
rqlite ``none``, 1 reader                --               5,494            493             1,074
rqlite ``none``, 4 writers + 1 reader    535.9            4,687            561             1,657
rqlite ``weak``, 1 reader                --               3,977            683             1,673
rqlite ``weak``, 4 writers + 1 reader    557.2            3,347            790             2,267
rqlite ``linearizable``, 1 reader        --               2,789            996             1,987
rqlite ``linearizable``, 4 w + 1 reader  593.6            266              10,960          26,660
rqlite ``strong``, 1 reader              --               185              15,830          23,720
rqlite ``strong``, 4 w + 1 reader        576.0            143              21,290          29,570
=======================================  ===============  ===============  ==============  ===========================

With 4 readers per node rqlite gives ``none`` 9,650 / 6,646 reads/s and 369 commits/s under
load, ``weak`` 5,728 / 5,385 and 426, ``linearizable`` 5,338 / 921 and 580, ``strong`` 593 /
548 and 550 (readers only / with writers, then commits/s with readers) [SES]. The ranges of every
cell are in [SES]. With the GIL on, the rqlite readers (Python HTTP clients) read 21-68 %
fewer rows per second at ``none``, ``weak`` and ``linearizable`` [SES]; that is the client,
not rqlite.

**With the GIL on**, ``proto_db`` readers keep their latency, but the writers starve: they compete
with the readers for the interpreter. Medians of 5 verified runs [SES]: 180.9 commits/s with no
readers; 26.0 [23.5-27.5] with 1 reader per node (reads 68,980/s, p50 24 us, p99 208 us); 17.0
[15.2-19.5] with 4 per node (reads 62,070/s, p50 25 us, p99 440 us); readers alone 85,110/s (1
per node) and 70,780/s (4 per node). Every run of every cell was verified. *History:* at
``8b439cd`` 4 of the 5 session runs of the 4-writers-4-readers GIL-on cell failed with a
conservative ``ProtoOutcomeUnknownError``; at ``57fd351`` none did, in the session or in the
suite.

*Why the writers starve, measured (R137b).* A probe of the acknowledgement path
(``benchmarks/gil_ack_probe.py``, ``7757fb3``; three interleaved repetitions of each variant in a
single session on a machine shared with other work, 5 s per run, the result files are not committed, so read
the figures as indicative) times every hand-off of a ``replicate`` on a reader node. With two reader threads per reader node
and the GIL on, A's four writers commit 7.5-9.1 commits/s (commit p50 319-453 ms); with no readers, 61-85/s; with
the GIL off and the same readers, 48-75/s. The cause is CPython's GIL: the readers never block, so every other thread on
the node waits for a forced switch, which another thread can request only after the switch interval (5 ms by
default) and may lose to the other reader. Each blocking call on the acknowledgement path gives the GIL away and pays that
wait again: the acceptor lane's wake-up, the return of each ``fsync`` (about 2 ms of disk become 6-10 ms), the hand-off of
the reply to the per-peer sender thread (about 11 ms) and the return of ``sendall`` (about 5 ms). A replicate
that takes 2.8-4.1 ms of lane time with the GIL off took 29-85 ms. Causal test: ``sys.setswitchinterval(0.0005)`` on the two
reader nodes only brought the writers from 7.5-9 to 27-37 commits/s (the readers' rate was lower in 1 of 3
repetitions, 22.6k reads/s, and similar in the others). These are latency and liveness effects: none touches a safety
argument. **Recommendation:** run reader-heavy cluster nodes on the free-threaded build, which does not have the effect;
where the GIL build is required, shorten the switch interval on the nodes that serve CPU-bound readers
(``sys.setswitchinterval``), and measure both the readers and the writers.

What the numbers show:

- **Readers never wait on writers in proto_db, but they share the CPU.** Read latency moves
  little under writes (p50 22 -> 37 us with one reader per node). The writers lose throughput to
  the readers' CPU: -34 % with one reader per node and -72 % with four, because 12 reader
  threads plus 12 writers plus 3 nodes' protocol threads run on 6 cores. With the GIL on the
  readers win the interpreter and the writers nearly stop (26.0 and 17.0 commits/s, above).
  "Readers never wait" still holds; what readers cost is the writers' CPU. A deployment that must
  keep write throughput under heavy reads needs the reads on other cores or another member (a
  learner, D54), not more threads in the writer's process.
- **rqlite's writers lose less to its readers.** With one reader per node they lose 5 % at
  ``none`` (561.7 -> 535.9 commits/s) and 1 % at ``weak``; at ``linearizable`` and ``strong``
  they commit as many as without readers (593.6 and 576.0), within the cells' ranges. With four
  readers per node they lose 34 % at ``none`` (369 commits/s) and 24 % at ``weak`` (426), and
  nothing measurable at ``linearizable`` (580) and ``strong`` (550). The same steps cost
  ``proto_db``'s writers 34 % and 72 % (140 and 59 commits/s).
- Per read, an embedded ``proto_db`` snapshot read under writes is about 15x faster at p50 than an
  rqlite ``none`` read through HTTP on this machine (37 against 561 us), and 300x faster than a
  linearizable one under load. The comparison is between an embedded read and a client-server
  read, which is the point of the profile, not a statement about rqlite's engine.

**Where rqlite is the better choice.** When every read must be linearizable, rqlite provides it
by configuration (``linearizable``), and ``proto_db`` has no measured equivalent here. When reads come
from many separate client processes that cannot embed a peer, rqlite serves them over HTTP.
When readers and writers share one Python process with the GIL on, rqlite keeps its write rate
and ``proto_db`` does not.

Point reads
-----------

**Conceptual fit.** A ``proto_db`` primary-key read is a few AVL node visits in decoded Python
objects; a secondary-index read goes through the query layer (plan construction on every call).
SQLite reads a C B-tree.

20,000 records, per query [S0] (GIL on [S1]):

=================================  ==================  =================  ==================
Query                              proto_db            Python dict/list   SQLite
=================================  ==================  =================  ==================
point, primary key                 3.8 us (3.5)        0.1 us             3.7 us (3.8)
point, secondary index             78.1 us (72.0)      0.1 us             4.7 us (4.6)
point, no index (scan)             47.1 ms (43.6)      1.0 ms             1.13 ms (1.03)
range 1 % (200 rows), indexed      515 us (499)        48 us (bisect)     174 us (195)
range 1 %, no index                59.5 ms (53.8)      2.9 ms             1.06 ms (0.96)
top-10 of ``score >= x``           205 us (182)        2.7 ms (heapq)     16.2 us (17.6)
top-10 of ``city == c`` by score   754 us (655)        1.8 ms (heapq)     94.6 us (85.5)
=================================  ==================  =================  ==================

**Where SQLite is the better choice:** everywhere but the primary key: 3-17x on indexed reads
and top-k, and an unindexed scan is 42x slower in ``proto_db`` for a point query and 56x for a range
(59.5 ms against 1.06 ms).

Deep pages
----------

**Conceptual fit (D56).** A ``proto_db`` pager over an indexed ordered query seeks by rank (weights
kept in the index tree), on a snapshot that never changes, so page *n* costs the same as page 0
and a token resumes the same pages in any later transaction. SQLite's ``OFFSET`` reads and skips
every earlier row; its keyset pagination (``WHERE k > last``) is constant but only for the next
page.

10^6 rows, pages of 50, each request a new transaction (``proto_db``) on one connection (SQLite).
Per page [S0] (GIL on [RR]):

=============  ===============  ================  =================  ==================  ===================
offset         proto_db pager   proto_db islice   SQLite OFFSET      SQLite keyset,      SQLite keyset,
               (by rank)        (no rank)                            next page           chained jump
=============  ===============  ================  =================  ==================  ===================
0              224 us (228)     166 us            36 us (42)         38 us (50)          --
10,000         222 us (215)     14.4 ms           365 us (347)       41 us (48)          61 ms (64)
100,000        226 us (218)     14.6 s            3.0 ms (4.1)       38 us (62)          597 ms (728)
900,000        251 us (248)     --                42.1 ms (45.3)     42 us (50)          --
=============  ===============  ================  =================  ==================  ===================

The GIL-on rerun alone [RR] is not faster than the GIL-on suite run it replaces (overlapped by
the test run; the pager 220-228 us there too): that overlap did not inflate these figures. Its
range-predicate pager at 10^5 took 803 us against 400 us in the suite run, which is this cell's
run-to-run variance on this machine.

With a range predicate (``k >= N/2``) the ``proto_db`` pager costs 374-447 us at every offset; SQLite
``OFFSET`` 36 us to 3.1 ms. A pager over an index without weights (built before D56)
materializes the whole result before its first page: 1.45 s.

**Where SQLite is the better choice:** browsing next page after next page (keyset: 38-42 us,
about 6x faster), and any page within the first few tens of thousands of rows by ``OFFSET``.
``proto_db`` is the better choice for random jumps deep into a large result (13x faster than
``OFFSET`` at 10^5 rows, 168x at 9 x 10^5) and for pages that must stay stable while writers
commit.

Repeated queries
----------------

**Conceptual fit (D56, P1).** A ``proto_db`` query result is cached by the pointers it read: a root
that has not changed is the same pointer, so a repeated query is a lookup, shareable across
transactions and threads (and, as `Reads across transactions in a cluster`_ shows, across the
commits of other nodes). SQLite has no result cache: every request runs. The first run of an
ad-hoc query pays the plan and, in ``proto_db``, a cache miss costs more than an uncached run.

[S0] (GIL on [RR]), per request:

===================================================  ===============  ===============  ================  ===============
Query                                                proto_db         proto_db         proto_db          SQLite
                                                     no cache         cache miss       cache hit         (re-executes)
===================================================  ===============  ===============  ================  ===============
range + filter, ~10^4 rows, N = 10^5                 79.6 ms (78.0)   140 ms (132)     3.99 ms (3.80)    11.8 ms (18.2)
range + filter, ~10^4 rows, N = 10^6                 78.4 ms (91.9)   135 ms (150)     4.21 ms (4.74)    120 ms (142)
filter on an unindexed field, ordered, N = 10^5      15.4 s (17.6)    16.2 s (18.4)    0.49 ms (0.98)    40.4 ms (41.4)
===================================================  ===============  ===============  ================  ===============

**Where SQLite is the better choice:** a first, ad-hoc query is 7x faster in SQLite at 10^5 rows
and 380x faster on the unindexed ordered filter, and a cache miss on the range query costs
``proto_db`` 1.7-1.8x an uncached run. SQLite's own figure at 10^5 differs by 54 % between the two
GIL modes (11.8 and 18.2 ms, both reruns), far outside the 15 % that this page treats as noise;
the cause was not investigated, so read it as an unexplained spread, not as an effect of the
GIL. ``proto_db`` is the better choice when the same query repeats on a
root that changes less often than it is read: about 4 ms against SQLite's 12-120 ms.

History and "as of"
-------------------

**Conceptual fit (P1).** Every ``proto_db`` commit is a new immutable root; an earlier root is read
with the same code as the current one (``Database.snapshot_at``), for any query, with no schema
work and no write cost beyond the commit itself. SQLite keeps history only if it is designed in:
here an ``equipment_audit`` table, filled by a trigger on status updates and stamped with a
version counter, and an "as of" query that undoes the later changes. The two do equal work for
the one question the audit table was designed for.

From the inventory workload (`Bulk load and scale`_): the status of every equipment of one
region in the version before the last bulk change, per equipment [S0] at 10^5 objects, [INV]
at 10^6:

=========================  =============================  =============================
Objects (equipment)        proto_db as of (us/equipment)  SQLite audit (us/equipment)
=========================  =============================  =============================
95,645 (20)                8.4 (7.7)                      1.17 (1.17)
958,665 (140)              9.9 (9.3)                      1.39 (1.38)
=========================  =============================  =============================

The bulk change itself (one commit, the same region) costs ``proto_db`` 796 / 620 us per equipment
and SQLite, trigger included, 79 / 22 us.

``versions`` [S0]: a new version per update costs ``proto_db`` 53 / 58 / 70 us at 10^3 / 10^4 / 10^5
entries; a Python ``dict`` copy costs 14 us / 137 us / 4.8 ms. Those ``proto_db`` figures are versions
in memory, not committed. A *committed* update (one transaction, ``durability="none"``) costs
1.28 / 1.53 / 2.52 ms at the same sizes (GIL on [S1]: 1.67 / 1.97 / 1.81 ms). So an uncommitted
version is cheaper than a ``dict`` copy from 10^4 entries on. A committed version costs more than
a ``dict`` copy at 10^3 and 10^4 entries and about half of one at 10^5 (2.5 ms against 4.8 ms);
unlike the copy, it is persisted, and every one stays readable.

**History depth does not slow current queries** (``history_depth``, ``proto_db`` only, ruling R112).
The documented pattern for versioned entities: each version holds its predecessor in a
``previous`` attribute; the current collection holds only the newest version of each entity, so
a superseded version leaves the current collection and its indexes and stays reachable through
``previous``. 1,000 orders in 100 customers, an EqualityIndex on ``customer``; at depth *D*
every order has *D* versions. The query ``orders where customer == c`` (10 rows) runs in a new
transaction each time, with the query result cache off; *warm* uses the default atom cache,
*uncached* a space with ``atom_cache_entries=0``, so its decodes per query are the work the query
does. Every answer is checked; one ``previous`` chain per depth is walked and must hold versions
*D* .. 1. [S0] (GIL on [S1]):

=======  ==========  ===============  ===============  ==========  =============  ==============
depth    versions    warm (us/query)  uncached         decodes     index entries  on disk (MB,
         stored                       (us/query)       per query                  apparent)
                                                       (uncached)
=======  ==========  ===============  ===============  ==========  =============  ==============
1        1,000       195 (164)        2,942 (2,410)    35.78       1,000          0.46
10       10,000      203 (170)        2,979 (2,632)    35.78       1,000          4.9
100      100,000     197 (167)        2,845 (2,570)    35.78       1,000          49.6
1,000    1,000,000   185 (177)        2,876 (2,481)    35.78       1,000          499
=======  ==========  ===============  ===============  ==========  =============  ==============

The current query decodes the same 35.78 atoms at every depth and its latency does not move
(warm: 0 decodes); the index holds 1,000 entries at every depth. What grows with history is the
database (about 0.5 KB per version, apparent size) and the time to write it (0.5 s for depth 1,
632 s for depth 1,000, one build per depth, a single run, with ``durability="none"``: one commit replaces all 1,000 orders). This
holds for this pattern, where history leaves the current collection; a design that keeps every
version in the queried collection (or in its index) would make the query pay for history, and
was not measured.

**Where SQLite is the better choice:** a history question known in advance, served by an audit
table: 7x faster per row here. ``proto_db`` is the better choice when the question is not known in
advance: any query, any root, any field, without triggers, and without the audit table's write
cost on every change.

Object graphs
-------------

**Conceptual fit.** An inventory is a graph. ``proto_db`` stores each node as an object in a
dictionary per type with an equality index on the parent reference, and follows references by
dictionary reads; SQLite joins tables and walks paths with a recursive CTE. Both use the same
four indexes.

Telecom inventory (`Bulk load and scale`_), per request, [S0] at 10^5 objects and [INV] at 10^6
(default cache; GIL off, GIL on in brackets):

=============================================  ==========================  ==========================
Operation                                      10^5: proto_db / SQLite     10^6: proto_db / SQLite
=============================================  ==========================  ==========================
point lookup of a port                         47.2 / 3.9 us               48.5 (63.4) / 4.7 (5.6) us
equipment at a site (10 rows)                  134 / 7.0 us                160 (172) / 15.8 (10.9) us
ports at a site (3 index levels / 3-way join,  11.3 / 0.73 ms              13.5 (13.1) / 0.84 (1.22) ms
~2,900 rows)
path trace (up to 16 links, ~32 ports)         281 / 35 us                 519 (510) / 363 (375) us
=============================================  ==========================  ==========================

**Where SQLite is the better choice:** every graph operation measured here, by 1.4-19x. The
path trace narrows from 8x at 10^5 to 1.4x at 10^6, because SQLite's recursive CTE grows from
35 to 363 us while ``proto_db``'s walk grows from 281 to 519 us. ``proto_db``'s graph is plain Python
objects in the application, with no mapping layer, but it is not faster to traverse.

Bulk load and scale
-------------------

**The workload.** A network inventory: sites -> equipment -> cards -> ports, plus links between
ports, with history (the real system it models manages a few tens of millions of devices).
Fan-outs, chosen to be realistic and fixed: 10 equipment per site, 10 cards per equipment, 8-48
ports per card (a hash of the card id), and links on about half the ports (each joins two
sites; a cross-connect leads to the next equipment of the site, so a path trace crosses many
sites). Objects counts every persisted node (sites + equipment + cards + ports + links). Both
systems build the graph in one transaction per site, ``proto_db`` with ``durability="fsync"``,
SQLite in WAL with ``synchronous=FULL``. The answers are checked against a model computed from
pure functions of the ids.

===================================  ================  ================  =====================  ====================
Step (each a single run)             proto_db build    SQLite build      proto_db on disk       SQLite on disk
                                     objects/s         objects/s         bytes / object         bytes / object
                                     (commits/s)       (commits/s)       (alloc. / apparent)    (alloc. / apparent)
===================================  ================  ================  =====================  ====================
95,645 (28 sites) [S0]               2,379 (0.70)      334,365 (97.9)    157 / 423              18 / 31
958,665 (277 sites) [INV]            2,080 (0.60)      315,111 (91.1)    157 / 424              18 / 31
958,665, cache holds every atom      2,327 (0.67)      340,471 (98.4)    157 / 424              18 / 31
===================================  ================  ================  =====================  ====================

Allocated is what ZFS reports after compression (``du``), apparent the file sizes. GIL on, 10^6
[INV]: 2,117 and 279,837 objects/s. The last row sets ``atom_cache_entries=3_000_000``, which
holds every atom the build writes.

**Memory.** Peak RSS of the process that builds and queries both databases (``/usr/bin/time -v``),
at 10^6 objects [INV]:

=====================================  ==============  =====================================
Cache                                  peak RSS        loaded atoms at the end
=====================================  ==============  =====================================
default (50,000 loaded atoms)          0.23 GB         50,000 (2.79 million saved, 2.79
                                                       million unloaded)
GIL on, default                        0.23 GB         50,000
``atom_cache_entries=3_000_000``       3.87 GB         2.79 million
=====================================  ==============  =====================================

With the default cache, memory does not grow with the database: the cache holds at most
``atom_cache_entries`` loaded atoms and unloads what it evicts (Task 8.3m, D53 amendment). A
cache that holds every atom costs about 1.3 KB per loaded atom (decoded state and residency
bookkeeping together; ``(3.87 - 0.23) GB / 2.79 million``). At ``57fd351`` that cache buys
little on this workload: the warm reads with the default cache take 0.88-1.2x the time of those
with the fitting cache (port lookup 48.5 against 55.1 us, ports at a site 13.5 against 12.2 ms, path
trace 519 against 434 us, single runs), and the build is 11 % slower.

*History.* The 10^7 step (2,769 sites) was not run in this measurement; it is deferred to the
final verification pass (maintainer decision). At ``1845993`` (Task 8.3m), ``proto_db`` alone built
9,626,985 objects in 78 min with a peak RSS of 0.27 GB, default cache, GIL off
(``2026-11-06-1845993-residency-ladder.log``); at ``8b439cd``, before Task 8.3m, the same step
stopped at 5.9 million objects with 14.9 GB RSS, because the cache retained every decoded node a
commit wrote (``2026-11-06-8b439cd-inventory-ladder.log``, ``-memdiag.jsonl``). Task 8.3m also
measured warm reads at 10^6 with the default cache 12-36x slower than with a fitting cache (port
lookup 0.93 ms); at ``57fd351`` they are not (above). The change was not isolated: the residency
fix rounds (``c9f60bb``, ``236a228``, ``550dde5``) lie between the two.

**Cold against warm.** The first request of a new instance (open the storage, then the request;
the OS page cache and the ZFS ARC are not dropped, which needs root) against the same request
warm, at 10^6 [INV]: point lookup 2.90 ms against 48.5 us (SQLite 215 us against 4.7 us);
equipment at a site 3.77 ms against 160 us (SQLite 231 against 16 us); path trace 47.6 ms
against 519 us (SQLite 0.99 ms against 0.36 ms). ``proto_db``'s cold cost is decoding: a new
instance decodes every node it visits.

**Where SQLite is the better choice:** loading data: 141x (10^5) and 151x (10^6) faster per
object, and 8.7x smaller on disk as allocated (13.6x in apparent size). A data set that is
loaded in bulk and rarely changed belongs in SQLite.

*Why.* A ``proto_db`` object is a persistent immutable atom: each one is encoded on its own and
appended to a segment, and placing it in the persistent trees that index it copies the path from
the new node to the root, because nothing is modified in place (D8, structural sharing; see
:doc:`concepts`). The 157 against 18 allocated bytes per object and the 423 against
31 apparent bytes are that cost on disk: SQLite writes rows into B-tree pages that it updates in
place, packed in a page, with no per-object identity or encoding. The build is also committed
once per site with ``durability="fsync"`` (D6, D19), which syncs segment data, the directory and
the root file; SQLite syncs its WAL. Neither the encoding nor the path copying is a defect: they
are the price of immutable versions that readers share without locks, which this workload
does not use while loading.

Single-process commits
----------------------

One small read-modify-write commit (read a counter, increment it, insert one key) per durability
mode, one process, ``FileStorage`` against SQLite in WAL [S0] (GIL on [S1]):

===================  ====================  ======================  ====================
proto_db durability  commits/s             SQLite ``synchronous``  commits/s
===================  ====================  ======================  ====================
``fsync``            149.3 (142.5)         ``FULL``                585 (588)
``flush``            192.4 (184.3)         ``NORMAL``              12,060 (11,580)
``none``             683.7 (504.7)         ``OFF``                 21,210 (17,900)
===================  ====================  ======================  ====================

``fsync`` and ``FULL`` are the like-for-like pair (both survive power loss; ``proto_db``'s crash
matrix checks ``fsync``). ``flush`` and ``NORMAL`` are process-crash safe, not power-loss safe
(D19); ``proto_db`` ``flush`` still syncs its root file and directory, which buys no data safety.

Commits of one process, ``FileStorage`` ``none``, commits/s at 1 / 4 / 16 threads [S0]
(GIL on [S1]): conflicting counter 548 / 494 / 471 (923 / 651 / 216), with 1.0 / 2.02 / 5.95
attempts per commit; blind merges 396 / 517 / 484 (547 / 482 / 417); disjoint roots 537 / 897 /
1,028 (873 / 823 / 624). These are in-suite figures: run alone [BIS], ``57fd351`` gives 857 / 782
conflicting commits/s at 1 thread in two runs, and the in-suite GIL-on figures are higher than the
GIL-off ones at 1 thread (see `Changes since the previous measurement`_).

**Where SQLite is the better choice:** single-process commits at every durability level, by
3.9x (``fsync``) to 63x (``flush``). Bulk insert of 10,000 records: ``proto_db`` 183 us per record
without indexes and 632 us with two, SQLite 2.8 and 4.7 us [S0]. Each index adds about 170-210 us
to a ``proto_db`` update at 10^4 rows (52 / 261 / 453 / 622 us with 0 / 1 / 2 / 3 indexes).

*Why.* A commit is a new immutable root, so even one key written copies a path of tree nodes,
serialises the new nodes as atoms and then publishes the root atomically (D19): in ``fsync``
mode that is a sync of segment data, of the directory, of a temporary root file and of the
directory again, where SQLite in WAL syncs one log. The gap shrinks as the durability falls
(3.9x at ``fsync``, 63x at ``flush``, where SQLite does almost no sync work and ``proto_db`` still
syncs its root file and directory) because the fixed cost of a SQLite commit is then a page
append. Every index is one more tree to path-copy, which is why each adds the 170-210 us above;
SQLite updates the index pages in place. The ``proto_db`` design buys snapshots and conflict
detection by comparing roots (D9), which SQLite's single writer does not need.

Parallel work
-------------

``parallel_scan`` (2,000 items, a CPU-bound predicate, ``ParallelPool``), GIL off. Run alone
[RR], it scales: 1.06 / 1.73 / 3.04 / 4.00 / 4.14x the sequential loop at 1 / 2 / 4 / 8 / 16
workers. Inside the full suite run [S0] the same code gave 1.07 / 1.06 / 1.18 / 1.11 / 1.10x:
**the in-suite collapse reproduced at** ``57fd351``, as it did at ``4aab0f2`` (alone 4.93x,
in suite 1.12x at 16 workers). **It no longer reproduces at the current head** (``c953d7e``, a later single
session, GIL off, from a clean export, with ``benchmarks/scan_state_probe.py``): 4.13x at 16 workers
after the suite's 14 preceding workloads, against 4.28x alone, although the process then held a 1.8 GB heap
with 2.08 million GC-tracked objects. No collection ran during the timed scans, and freezing or disabling the GC
changed nothing beyond noise, so the heap size, the GC and leftover threads are excluded as causes at this
head. The earlier collapse is not explained (its shape, every worker count at the sequential time, is what a process
held to about one CPU would show, and a transient outside the process fits it); if it reappears,
the probe placed after ``parallel_scan`` would separate the explanations. The paragraph below describes the
measurement at ``57fd351``. The suite runs it after workloads that build large data sets
(the 10^6-row pagination and query-cache sets); at ``57fd351`` the process's peak RSS in the
suite was 9.0 GB, reached in those workloads; how much of it stays live when ``parallel_scan``
runs was not measured. The cause is not known. The inference of the previous measurement (the
free-threaded collector's passes grow with a large live heap) is not confirmed: Task 8.3m saw
the scan scale after the preceding workloads at reduced sizes. Read the in-suite figures of ``57fd351`` as a suite artefact, not as the
scaling of ``ParallelPool``.

*Where SQLite is the better choice, and why.* Parallel work is where ``proto_db``'s model pays, and
the only loss is the one measured: reads scale because they walk immutable nodes that no writer
changes (4.15x and 4.62x above), but with the GIL on nothing scales (0.9-1.2x); the in-suite
``parallel_scan`` collapse of the earlier suites is unexplained, and did not reproduce later. The cost of the model is paid elsewhere: every write
copies a path instead of updating a page in place, which is why the commit rates of `Single-process
commits`_ are lower than SQLite's even though the readers scale. Parallel writers do not scale
either: they all publish one root, so conflicting commits retry (5.95 attempts per commit at 16
threads) and the rate stays near 500 commits/s.

``concurrent_reads``, GIL off [S0]: ``get_at`` 141k -> 587k ops/s at 16 threads (4.15x),
EqualityIndex queries 3,289 -> 15,190 (4.62x), a Python ``dict`` 1.15M -> 3.57M (3.10x). With
the GIL on [S1] nothing scales (0.9-1.2x).

Vector search [S0] (GIL on [S1]): 10,000 vectors of 32 dimensions, k = 10, recall 1.0 for every
backend; per query: numpy brute force 1.24 ms, ``exact`` 0.78 ms (0.76), ``ivf_flat`` 1.28 ms
(1.26), ``hnsw`` 0.73 ms (0.62), the last with a lazy graph build of 1.0 s (0.82) on the first
query.

Operations
----------

- **Object-store latency stays off the commit path** (D46). 3 in-process peers, blind adds, run
  alone [RR], GIL off: 292.6 [292.6-303.9] commits/s with a 0 ms store and 294.8 [285.4-303.1]
  with a 30 ms store, ratio 1.01. With the GIL on [S1]: 186.7 and 200.4, ratio 1.07. In the
  GIL-off suite run [S0] this workload failed: a commit on node ``n2`` raised
  ``ProtoOutcomeUnknownError`` ("fewer than 1 other members stored the commit's records before
  the lease ended; the root (epoch 453) may or may not take effect"). That is the conservative
  outcome the protocol allows (R86): the client is told it does not know, never told a lost
  commit succeeded. The rerun alone passed. The CAS-only baseline (writers arbitrated by the
  store's conditional write, the reverted D5 design) falls to 6.55 commits/s with one writer and
  6.19 with four at 30 ms (p95 3.5 s, 0.22-0.30 failed root CAS per commit) [S0].

- **Open and recovery** [S0] (GIL on [S1]): open a 20,000-record space and read one record
  3.2 ms (2.5); reopen a TieredStorage local tier with 500 commits 5.0 ms (4.2); a cold walk of
  every record 168 us per record (93), a warm walk 0.69 us (0.55).

- **In-process peers** [S0]: blind adds at 1 / 2 / 4 / 8 peers in one process 250 / 353 / 281 /
  256 commits/s; increments (a read-modify-write on one key) 183 / 113 / 94 / 88, with 3.1-4.4
  attempts per commit from 1 peer on. In the GIL-on suite run [S1] ``cluster_peers`` failed: a
  commit on node ``n2`` of the 2-peer cluster got ``ProtoOutcomeUnknownError`` ("only 1 of the 2
  acknowledgements needed arrived before the lease ended"). The GIL-on rerun alone [RR] passed:
  blind adds 252 / 130 / 144 / 101, increments 166 / 76 / 51 / 43. Under the GIL a thread can be
  descheduled past the commit path's timing margin; the outcome is a conservative failure, not
  a lost commit. These are the only two failed runs of this measurement; both workloads passed
  when rerun alone, and a single rerun cannot say how often the failure occurs.

- **Where SQLite is the better choice, and why.** Opening and recovery: a ``proto_db`` space decodes
  every node it visits on demand, so the cold walk costs 168 us per record against 0.69 us warm,
  where SQLite reads a page that holds many rows with one read (the same decoding cost as in
  `Bulk load and scale`_). Running a cluster: a commit across peers waits for acknowledgements
  from other members before it returns (D46), and that coordination is work a single-file SQLite
  database never does; it is the price of a commit that survives the loss of a node, which a
  single SQLite file does not offer (see the rqlite comparison for the server alternative).

- **Server against embedded peer.** rqlite is a server: any client that speaks HTTP reaches it,
  and its nodes can run on machines the application does not. A ``proto_db`` peer is a library inside
  a Python application: no server to deploy, no client round trip, but every writer and reader
  is that application's process.

Reproducing
-----------

::

    .venv314t/bin/python -m benchmarks.export <commit> <dir>/export-<commit>
    cd <dir>/export-<commit>
    $PY -m benchmarks --rqlite <rqlited> --workdir ../run --json ../suite-gil-off.json
    $PY -X gil=1 -m benchmarks --rqlite <rqlited> --workdir ../run --json ../suite-gil-on.json
    $PY -m benchmarks.session --reps 5 --rqlite <rqlited> --workdir ../run --json ../session.json
    $PY -m benchmarks --only inventory --param inventory.objects=1000000 --workdir ../run
    $PY -m benchmarks --only inventory --param inventory.objects=1000000 \
        --param inventory.atom_cache_entries=3000000 --workdir ../run

``$PY`` is the free-threaded interpreter of the project's virtual environment. The session's
default groups are ``headline``, ``mixed`` and ``cache``. Run every step alone, under a memory
cap without swap, for example ``systemd-run --user --scope -p MemoryMax=12G -p MemorySwapMax=0
...``. On the machine above a full suite run took 53 minutes with the GIL off (peak 8.5 GB, in
the 10^6-row pagination and query-cache data sets) and 43 minutes with it on, the session 42
minutes, a 10^6 inventory step 7-8 minutes.
