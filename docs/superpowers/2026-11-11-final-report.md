# proto_db 2.0: final report of the protoDB v2 branch

Branch `fix/dict-merge-and-wal-durability` (repository protoBase), final verification from `d29075e`
to the release-candidate head (Task 11.1, 2026-11-11 to 2026-11-14; each run names its commit). This
report is for the maintainer. The plan is `docs/superpowers/plans/2026-11-01-protodb-v2.md`; the
decisions are in `docs/decisions.md`.

## What was done

The branch replaces the 0.1 implementation of `proto_db` with 2.0 (D48):

- **Values and atoms.** A total value order, canonical frozen values, process-stable hashes and a
  normative nesting limit (D17, D24, D25, D60); lazily identified atoms (D21).
- **Persistent collections and indexes.** AVL-based List, Dictionary, HashDictionary, Set,
  CountedSet and RepeatedKeysDictionary; equality, range and vector indexes; content equality and
  net-effect merges (D20, D22, D23, D26-D30, D33, D39).
- **Transactions and storage.** The commit protocol with unknown outcomes, mutable cells, FileStorage
  durability modes, group commit, and a decoded-atom cache with bounded residency (D19, D32-D35,
  D52, D53).
- **Queries.** An expression layer, a planner, LINQ, query result caching by pointer and stable
  pagers (D36-D38, D56).
- **Cluster.** A peer cluster with a majority lease, majority-acknowledged replication, bounded
  replication, pipelined permissions, and the C1 fix that keeps a commit base at or above the
  lease's phase-1 root (D43-D46, D50, D54, D55).
- **Format.** Format 4, language-neutral and frozen for protoBaseST, with conformance vectors, a
  verifier and opaque atoms (D51, D57-D60). Tags v2.0.0rc1 and v2.0.0rc2.
- **Performance and documentation.** A self-verifying benchmark suite with results by workload
  profile (D49); tutorials, design guide, coordinator protocol, format specification and the
  course kit.

Task 11.1 (this pass):

- **R126, the final verification.** Every safety proof that 8.3n deferred was run on clean exports:
  first of `7ad847a`, then of `d29075e`, and A4 and the full suites of `7786974` (rulings R145, R146 on
  the shared machine). No safety assert failed in any unplanted run.
- **Plan items.** The static checker, coverage, Python 3.12-3.13, CI and the docs build.
- **Part C.** The carried test-isolation defects were fixed.
- **P1, R141.** The first full run on Python 3.11 found a product defect: values at the nesting limit
  cannot be compared there. The minimum Python became 3.12 (D61).
- **R142.** The cluster tests gained a stress mode in which liveness floors are recorded instead of
  asserted, so that each test under the R125 stress load goes on to its safety asserts; the stress
  task now counts how many executions reached them.

## Measured results

`S` is the scratch directory of the pass, `/home/gamarino/Documentos/proyectos/scratch-8.3n/final`. Every
heavy run used a clean export (`benchmarks.export`) under a 12 GiB memory scope, one at a time. The Task 11.1
report (`.superpowers/sdd/2026-11-01-protodb-v2/task-11.1-report.md`, local, not committed) has every
figure with its log.

| Item | Commit | Result | Source |
|---|---|---|---|
| Static checker (Part B) | 7ad847a | all 57 `proto_db` files covered, 0 offenders | `S/logs/b-static.log` |
| Docs build `sphinx -W` | f33069e | rc 0 | `S/logs/docs-build-r141.log` |
| Test isolation (Part C) | e814252 | trigger order 80/80 runs; all unit cluster tests first 10/10 | `S/logs/c40.log`, `S/logs/c5all.log` |
| A1 deferred proofs, runs 1-12 per mode | 7ad847a | 23 of 24 passed; the failure was a test defect (fixed e5a74ee) | `S/logs/a1.log` |
| A1 runs 13-40 per mode | d29075e | GIL off 27/28, GIL on 22/28 runs passed; 12 failing executions: 11 load-induced liveness (progress floors, timing bounds, unknown outcomes, most while another session's builds ran), 1 test defect (fixed fa84d2d); no safety assert failed | `S/logs/a1f.log`, `S/logs/a1f.log.note` |
| A1 rare rejoin stall, probed alone | d29075e | 200/200 runs passed | `S/logs/rejoin-probe.log` |
| Fairness tests runs 1-12, forced-failures test 40 per mode | fa84d2d | fairness 24/24, forced 80/80 runs passed | `S/logs/a1x-fair.log`, `S/logs/a1x-forced.log` |
| A2 plants | 7ad847a | 28 of 29 plants detected (112/116 runs); the 29th was a stale plant (retired 3a254e0) | `S/logs/a2-plants.log` |
| A2 plants, catalogue again | fa84d2d | every plant detected: 116/116 planted runs (29 plants, 2 runs per mode) | `S/logs/a2f-plants.log` |
| Kit, provenance, harness and stress-mode tests (export) | fa84d2d | 28 passed, 3 skipped (need a git checkout) per mode | `S/logs/kit-g0.log`, `S/logs/kit-g1.log` |
| A2 C1 course exercise | 7ad847a | 4/4 runs fail as the exercise requires | `S/logs/a2-c1-*.log` |
| A3 R3b set (R145: 20 clean runs per mode) | d29075e, d3aa0ce | 20 clean runs per mode, all passed (313 passed, 7 xfailed each); the failures were all in runs another session's builds overlapped (R146), replaced: load-induced liveness, and three test defects (dead-branch lineage, fixed d3aa0ce; exact epoch, fixed b9b5fc1; power-loss capture race, fixed 241b467) | `S/logs/a3.log`, `S/logs/a3.log.note` |
| A4 stress, R125 (before R142) | 7ad847a | 75/150 passed; 0 safety failures; 75 liveness (tests that failed a liveness floor never ran their safety asserts) | `benchmarks/results/2026-11-12-7ad847a-cluster-stress.json` |
| A4 stress, stress mode (R142), 3 attempts | 7786974 | no attempt clean: all three overlapped another session's C++ builds (peaks 57, 63, 49). 150 executions each; 134, 136 and 134 reached their safety asserts; no safety assert failed. The rest: load-induced liveness and four test defects (exact epoch, b9b5fc1; a zero-commit counter in the crash verifier, the paused-holder total counter and the setup-database schedule check, fixed in the working tree, see below) | `S/logs/a4f-stress-a*.log`, `S/logs/a4f.note` |
| A5 full suite 3.14t, GIL off / on | 7ad847a | 2 failed, 6212 passed, 3 skipped, 12 xfailed in each mode; both failures export-only test defects (fixed 3a0ac47) | `S/logs/a5-full-g0.log`, `S/logs/a5-full-g1.log` |
| Full suite 3.14t, GIL off / on | 7786974 | 6214 passed, 1 failed, 4 skipped, 12 xfailed in each mode (clean windows); the failure is the version mismatch (product defect, fixed 01e3452) | `S/logs/full-g0-a1.log`, `S/logs/full-g1-a1.log` |
| Python 3.11 full suite | 7ad847a | 8 failed: 2 export-only, 3 test defects (fixed 231d62d), 3 = P1 | `S/logs/py3.11-full.log` |
| Python 3.12 full suite | 7ad847a | 4 failed, 6206 passed: all four test defects fixed in 3a0ac47 and 231d62d | `S/logs/py3.12-full.log` |
| Python 3.12 / 3.13 full suites | 7786974 | 3.12: 6209 passed, 1 failed (version), 9 skipped, 12 xfailed in each of 3 attempts, all overlapped by other sessions' work; 3.13: 6210 passed, 1 failed (version), 8 skipped, 12 xfailed, clean | `S/logs/full-py312-a*.log`, `S/logs/full-py313-a1.log` |
| Depth tests after R141 | f33069e | 114 passed on 3.12, 3.13, 3.14t GIL off and GIL on | `S/logs/r141-depth.log` |
| A6 inventory, 10^7 objects, GIL off | 7ad847a | 9,626,985 objects verified; build 3966 s (2,427 objects/s; SQLite 326,182/s); peak RSS 256 MB; warm port lookup 52 µs (SQLite 4.1 µs); 158/426 B per object on disk, allocated/apparent (SQLite 18.7/34.5) | `benchmarks/results/2026-11-12-7ad847a-inventory-10000000-gil-off.json` |
| Coverage, statements, GIL on, in-process | 7ad847a | 95 % (16,400 statements, 882 missed); lowest: cluster 93.1 %, storage 93.4 % | `S/logs/cov.log`, `S/cov.json` |
| R142 quiet-mode strictness | d29075e (tree) | changed files 27 passed in each mode; 3 plants each fail quietly and pass, recorded, in stress mode | `S/logs/r142-proof.log` |

### P1: values at the nesting limit on Python 3.11 (product defect, closed by R141)

On CPython 3.11.10, `CountedSet.from_iterable([v, v])` with `v` 1018 nested tuples deep (the deepest element
D60 lets a CountedSet hold) raised a bare `RecursionError` from the bucket lookup. The key, hash and equality
walks and the query-result cache failed the same way at the limit: 3 tests failed and 111 passed in the two
depth test files. The same tests pass on 3.12 and 3.13, as they do on 3.14t. CPython 3.11 counts the C recursion of a
nested-container comparison against the Python recursion limit; 3.12 gives it a budget of its own. The limit
stays a property of the format (R139), so the floor moved to 3.12 (D61, commit f33069e). Source:
`S/logs/depth311-repro.log`.

### R142: safety evidence under stress

In the R125 stress run at 7ad847a, every failing execution failed a liveness check. 24 crash-test executions
and 4 fencing executions stopped at a progress floor before their safety asserts, so those runs proved nothing
about safety. With `PROTO_STRESS=1` the liveness floors are recorded instead (`tests/support/stress.py`), and
`benchmarks/cluster_stress.py` reports, per test, how many executions reached their safety asserts. Quiet mode
asserts exactly what it asserted before.

## Release candidates and the defects found since 2.0.0rc3

2.0.0rc3 was published to PyPI early, from master `9c589fb` (which merges branch commit `7786974`), at the
maintainer's request, before this verification had ended. Everything found after that publication, with
its status at the end of the pass:

| Defect | Kind | Status |
|---|---|---|
| `proto_db.__version__` is `2.0.0.dev0` in the 2.0.0rc3 package while `pyproject` says `2.0.0rc3` (the only failure of every full suite at `7786974`) | product (release) | fixed in `01e3452` (both 2.0.0rc4) |
| CI-314: on CPython 3.14 an exception raised by a signal handler can land between a `with` statement's `__enter__` and its protected range, so a lock is never released (upstream CPython gh-148874, fixed in 3.15); the pager stability test then overran its timeout and its non-daemon writers loaded every later test | interpreter hazard; test defect | test fixed `8499c5b`; the hazard documented in the concurrency model, README and CHANGELOG (`263e0d4`, `66e1eff`); pytest-timeout uses the thread method |
| The power-loss test model keyed durable contents by inode number, which ext4 reuses: "Own segment ... holds other data" in the owner, holder, voter, learner and data-replica power-loss tests; and its write-back image was not one point in time (FileNotFoundError on `roots.log.tmp`) | test support | fixed `241b467` (two deterministic regression tests: the old model fails 3/3 per mode, the new passes); CI run 43557516383 at `e9c067b`, which contains it, has no power-loss failure |
| CI flakes: renewal, bounded replication, pipelined permits, the cluster tutorial's unknown-outcome handling | test and docs defects | classified and fixed on fix/ci-flakes, merged `e9c067b` after an adversarial review, a fix round and a re-review |
| CloudStorage reported an unknown outcome for a refused root write with no attempt in doubt once more than `root_history` roots had followed, against D43 (a refusal is a conflict); the fix's review found that trusting every refusal would double-apply through a client with hidden retries | product | ruling R147a: a refusal is a conflict only from a store that declares `refusals_are_definitive` (`8cd745e`, docs `0291e86`, nits `9f1977a`) |
| The S3 adapter's client was built with botocore `max_attempts: 1`, which botocore reads as one retry: every write could be sent twice | product | fixed in `8cd745e`: `total_max_attempts: 1` |
| A permission kept for a retry (D50) was ended outside `group.cond` after being taken out of `group.kept`; a runner in that window got `ProtoUserError("... already holds a write permission")` from a correct commit (CI idle-release test; reproduced 20/20 with the window widened) | product (availability, error truthfulness; not safety) | ruling R148: an "ending" marker in `group.kept`, `permit()` stays strict; being fixed on fix/kept-permit-window |
| Exact committed-epoch assertions (a base re-commit adds an epoch) | test | fixed `b9b5fc1` |
| The quick-run harness test cut off the failing workload's error (3.13, `cluster_cas_baseline`, not reproduced locally in 15 runs) | test diagnostics | `afab4a2` prints it; the cause is still unknown |
| The A4 test defects: the crash verifier's missing counter with zero commits, the paused-holder test's total failure counter, the setup-database test's schedule check | test | fixed `0b261ec`, `861f3a2`, `eb05af5` (20 runs per GIL mode each, stress runs safety held; the paused-holder plant fails 10/10) |

Confirmation runs of these classifications (on `7786974` plus the fixed tests; another session's test runs
overlapped some windows, which adds load only): owner/holder power-loss test 40/40 runs, the
`[reliable]` cloud counter 40/40 and the cluster tutorial 20/20 (none reproduced locally), idle-release
40/40 (the R148 window is narrow without widening), the quick-run harness test on 3.13 5/5.

2.0.0rc4 is not published. It waits on green CI and one local full run at the final head, which the
controller schedules after this task. The full suites of this pass ran on `7786974`; since then product
code changed in `proto_db/storage/cloud.py` (R147a, S3 `total_max_attempts`), `proto_db/__init__.py`
(version) and, with R148, `proto_db/transactions.py`, so they do not stand in for that run.

## Decisions D17 and later

- D17: one total value order across types; lists and dicts storable, not orderable keys.
- D18: `DBObject.with_` returns a new atom (a new version, a new oid); absent and None attributes are distinct.
- D19: FileStorage durability modes `fsync`, `flush` and `none`, with their failure and locking rules.
- D20: persistent AVL trees of `PosNode` and `KeyNode` atoms.
- D21: an atom gets its oid lazily, at first need; atoms are locked by stripes.
- D22: `IndexSet` and net-effect replay of index hooks on merge.
- D23: List, Dictionary and HashDictionary semantics and errors.
- D24: `hash_key` is a process-stable 64-bit BLAKE2b digest of the order key.
- D25: lists and dicts are stored only as frozen values.
- D26: Set, CountedSet and RepeatedKeysDictionary; counter merges compose their transforms.
- D27: `content_equals` compares exact type and content.
- D28: a minimal term model, with a scan oracle that every index result must match.
- D29: the index framework: KeySpec, hooks, `IndexPlan`; the package is `proto_db.indexing`.
- D30: equality and range indexes over shared key groups, with rank-bounded ranges and a cost model.
- D31: built-in atom types resolve without a prior import.
- D32: the space root layout and per-database history.
- D33: merge only the values whose op log is complete.
- D34: the commit protocol, unknown publish outcomes and transaction threading.
- D35: `MutableObject`, a mutable table beside the roots; handles are values.
- D36: the expression layer and physical plan nodes.
- D37: the query planner: normalisation, access path choice, union and intersection.
- D38: the LINQ front end.
- D39: VectorIndex with exact, IVF-Flat and optional hnsw backends.
- D40: `parallel_scan` with shared-cursor chunks; the caller participates; the first exception is re-raised.
- D41: FSM, one worker per machine.
- D42: the Arrow/Parquet bridge with lazily imported, optional pyarrow.
- D43: cloud storage: immutable segment objects and a root written by compare-and-swap.
- D44: the cluster resolves root-write contention among processes; storage CAS only fences.
- D45: the cluster coordinator: a majority lease with terms.
- D46: the commit point is the majority-acknowledged root replication.
- D47: one lazily resolved public export table.
- D48: the 0.1 implementation was removed.
- D49: a self-verifying performance suite with matched references.
- D50: an amortized owner grants short fenced write permissions.
- D51: format 3: compact atom references; large node values out of line.
- D52: group commit on a standalone storage.
- D53: decoded atoms are shared across transactions by pointer, in a bounded cache.
- D54: bounded replication: a fixed voter set and `f` data replicas per commit.
- D55: pipelined permissions: the next holder builds on a proposed root.
- D56: query results cached by pointer, rank access on key-group indexes, stable pagers.
- D57: language-neutral persisted type names; the format specification is the polyglot contract.
- D58: format 4: tokens, keys and hashes are functions of the canonical bytes alone.
- D59: polyglot freeze: opaque atoms for unknown types, accessors, a verifier, pinned vectors.
- D60: a normative value nesting limit of 1022 containers; walks never depend on the Python stack.
- D61: the minimum Python is 3.12, because 3.11 cannot compare values at the D60 limit.

## Known gaps

Every item the ledger (`.superpowers/sdd/2026-11-01-protodb-v2/progress.md`) carried, with its status at the
end of this pass. "Open" means not addressed on this branch. A minor item marked "not re-checked" was not
re-verified in Task 11.1.

Observed in Task 11.1, open:

- Under the A4 load, a reader's barrier commit with the object store in outage often failed "store
  unreachable" (3 to 4 of 6 executions per attempt, against 1 of 150 at `7ad847a`): the record was on a
  peer that did not answer in time. Liveness, but it surfaces as `ProtoUnexpectedError`, not as a
  timeout-class error; and a record no reachable member holds looks the same as slow peers.
- One A4 execution ran into pytest's 360 s timeout with the test thread polling for a permission (not
  blocked). The log cannot tell a long livelock from slow progress.
- `cluster_cas_baseline` fails the quick-run harness test on 3.13 in CI only; `afab4a2` prints the cause
  at the next failure.
- The development disk is ZFS, which does not reuse inode numbers: the power-loss tests there pass with
  the old model too; ext4 CI is the confirmation of `241b467`.
- Coverage counts in-process code only; the subprocess peers of the crash tests are not measured.

Cluster and protocol:

- R124: keep the term on a reserve refusal when no successor permission was issued. Open: parked for a
  whole-branch review, which this pass did not include.
- C1 re-review 4 minors m1-m4: timeout-based detection cost, docstring wording, no end-to-end O6 test, an
  obsolete probe. Open.
- The TCP transport has no authentication or TLS. Open; documented.
- Fairness is observed, not guaranteed; the straggler constants are fixed (5 ms, 4x). Open.
- 8.3d residuals: the availability cost of a failed tier, power-loss reordering that keeps a node down until
  repair, hard-link refusals, the `lock_timeout=0` refusal. Open; documented.
- 8.3l parked: pruning of `_own`/`_remote`/`_sealed`, `segments.log` compaction, a lazy `history()` iterator,
  about 2 % unexplained standalone cost. Open.
- `history()` keeps one atom per version forever (needs a limit or compaction). Open.
- The 10.P focused suites had one unidentified GIL-on failure in 13 runs. Not seen again in the 11.1 full
  suites or loops.
- Stale product docstrings (`_phase_1_floor`, `_rejoin_answers`, `_commit_under_lease`) went to the R128
  docstring pass; not re-checked. No `TODO(C1-fix)` marker remains in `proto_db/` or `docs/source/`.

Storage, format and values:

- Cloud reads download whole segments; there is no segment garbage collection. Open.
- Smoke tests of the object-store adapters against MinIO or a GCS emulator (carried to 11.1). Open: no emulator
  is installed; the adapters were never run against a real service.
- The rule that readers accept the lowest format version lives only in the specification until the first
  version-5 feature. Open by design.
- A process that does not import a model reads its records as opaque atoms; the explicit error comes only when
  behaviour is used. By design (D59).
- The cost of the verifier's double decode is unmeasured. Open.
- msgpack's pure-Python fallback recurses once per container and fails near the recursion limit. Open (D60
  requires msgpack with its C extension for the full depth).
- Indexed unorderable values are limited to 254 levels until the next `FORMAT_VERSION`. Open (D60, deferred).
- protoBaseST must confirm that it reads 1024 levels. Open (external).
- `ProtoBase/.venv` (Python 3.12) has msgpack 1.1.1, below the import gate. Open (environment; not used).
- With R141 the floor is 3.12. Values at the D60 limit cannot be compared on 3.11 (P1). Closed (D61).

Performance:

- Phase 9 carries were measured and reported, not optimised: R28 top-k, the canonical re-walk, index update
  cost, the blind-merge queue on the root lock, the R41 critical section. Open.
- 9c concerns: cluster commits 8-22 % slower than at 4aab0f2 (not isolated); `parallel_scan` 1.10x in the
  suite against 4.14x alone; GIL-on writer starvation. Open; reported in `performance.rst`.
- Vector indexes: rebuild cost at the first search of each version (exact and IVF O(n), hnsw O(n log n));
  IVF build time; hnsw cross-process nondeterminism. Open.
- `parallel_filter` reads its source in one thread; the Arrow export holds the whole result in memory. Open.

Tests, documentation and tooling:

- Process-wide object-count tests and the inline-values layout test (Part C). Closed (e814252; 231d62d skips
  the layout check before CPython 3.13).
- The system `sphinx-build` ran on a Python whose msgpack was too old (R140 carry). Closed (18eed34: the docs
  build with the project's environment).
- Docs pages could cite `benchmarks/results/regression-2026-11/` instead of the bench script. Open.
- A docs sentence on classes defined in functions or `exec` staying alive through the type registry. Not
  re-checked.
- The C1 course exercise relies on the honour system, and `make_exercise.py` edits exact text. Open by
  design.
- The product name (ProtoBase in the one-pager, ProtoDB in the Sphinx title). Addressed by the R140 rename
  (9fbecc8); not re-checked.
- Scratch git worktrees under `scratch-8.3n/` and prunable session worktrees remain (`wt-f0822d9` is gone).
  Open housekeeping.
- Plan item 11.1, a whole-branch review by a fresh reviewer. Not part of this pass; open.

Minor items deferred by earlier reviews (not re-checked in 11.1):

- The static tests duplicate the forbidden-import check, and `_is_broad` misses builtins.
- `ProtoDBError` loses its `code` keyword on pickle, and `BASE_ERROR` is undocumented.
- `canonical()` raises a raw `TypeError` for unhashable dict keys, and an aware time whose `utcoffset` is
  None ranks as naive.
- The hex `@int` decoder is lenient.
- The `root_lock` timeout is not validated (inf, -1).
- The "flush" durability mode may leave a space unopenable after a power loss (to document).
- AVL: a cold `pos_get` loads sibling stubs; `key_put` with an identical value copies the path; and
  `check_invariants` is exponential on a forged shared-subtree DAG.
- A corrupt stub keeps its `_payload` pinned, and `__repr__` shows `oid=None` for fresh atoms.
- No guard checks that every mutator is registered in the contract; an index remove on rebase may drop a
  concurrent redefinition; and `check_rebase` never adds or removes indexes.
- Nested CountedSets in a RepeatedKeysDictionary carry an empty IndexSet atom per key.
- The vector index's check (f) is skipped when `collection_key_spec` is None.
- Range estimates assume uniform occurrences per key.
- Saves made before the root lock orphan records for rebased values.
- Planner tests still to add: a union of two filters, a guard that an intersection never wins, and
  validation of limit and offset before `where`.
- The planner does not use other indexes to preselect the candidates of `near(k) AND P`.
- Callers must handle `ProtoOutcomeUnknownError`. Documented (the unknown-outcomes page).
