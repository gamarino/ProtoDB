# ProtoDB performance suite

A self-verifying benchmark suite (spec section 12, Tasks 9.1-9.3, decision D49). Every
workload prints the work it did and checks its results against an independent computation
(a brute-force scan, a Python model, the baseline itself, a read-back of the persisted state)
before it reports a single timing; a mismatch fails the run with exit status 1. Baselines
(Python dict/list, sqlite3, rqlite) do the same work and are verified the same way.

The report with the current numbers and their interpretation is
`docs/source/performance.rst`; raw results are under `benchmarks/results/`.

## Running

From the repository root:

```bash
# full run on the free-threaded build (GIL off), then with the GIL enabled
.venv314t/bin/python -m benchmarks --json benchmarks/results/$(date +%F)-py314t-gil-off.json
.venv314t/bin/python -X gil=1 -m benchmarks --json benchmarks/results/$(date +%F)-py314t-gil-on.json

# a quick smoke run (tiny parameters: checks the harness, not speed)
python -m benchmarks --quick

# selected workloads, other thread counts, comparison with an earlier run
python -m benchmarks --only point_lookup range_query --threads 1,4,12
python -m benchmarks --compare benchmarks/results/2026-11-03-py314t-gil-off.json --threshold 0.15
python -m benchmarks --list
```

Probes outside the suite: `benchmarks/residency_probe.py` (Task 8.3m) measures point lookups
with a decoded-atom cache that fits the working set and with one 20 % smaller, and the thrash
signal; it prints one JSON line (`python -X gil=0 -m benchmarks.residency_probe WORKDIR`, from the
root of the tree to measure). Its log is `results/2026-11-06-8.3m-residency-probe.log`.

Measurements that are reported are taken from a clean export of a commit, never from a working
tree (Task 9c):

```bash
.venv314t/bin/python -m benchmarks.export <commit> benchmarks/.work/export-<commit>   # git archive + digests
cd benchmarks/.work/export-<commit>
../../../.venv314t/bin/python -m benchmarks --workdir ../run --json ../gil-off.json
```

The export carries `BENCH_SOURCE.json` (the full commit hash and a SHA-256 per file); the harness
re-verifies it at start and records `git.commit` and `git.clean` in the results JSON (a file
edited or a Python file added after the export makes it `clean: false`). A run from a working
tree records the tree's HEAD and whether it was dirty; a directory that is not the top of a git
work tree records no commit.

Interleaved sessions (`python -m benchmarks.session --reps 5 --rqlite PATH --json OUT`) run the
cluster headline cells (3 peers x 4 threads at 100 B, 50 ids per commit, 5 peers; rqlite with the
same work), the mixed read/write cells and the cluster cache cells (`--only headline mixed cache`
selects groups) once per repetition each, alternating `-X gil=0` and
`-X gil=1` for every cell, and refuse to start while an `rqlited` or another benchmark process is
running (the check is recorded in the JSON). A session's per-cell medians and ranges cover its
verified runs only; failed runs are counted (`failed`) and listed (`failed_runs`), never averaged in.

Workloads added by the Task 9c CARRY:

- `cluster_cache` (ruling R110): 3 TCP peers (`benchmarks/cache_peer.py`), writers on node A
  updating a catalog, readers on B and C, each read in a new transaction: point reads with the
  decoded-atom cache at its default size, below the working set and off; a query on a collection
  the writers never touch, with the query result cache on and off; rqlite with the same shape at
  each read consistency level. Atom decodes per read are counted in each reader process
  (`benchmarks/decode_counter.py`); every read, the final catalog and the advance of every reader
  node's versions are verified.
- `history_depth` (ProtoDB only, not a comparison): versioned entities built with the `previous`
  pattern at depths 1/10/100/1000 versions per entity, the same number of current entities each
  time; a current-state indexed query, warm and with no atom cache, with its latency and its atom
  decodes per query.
- `inventory` takes `--param inventory.atom_cache_entries=N` and records the peak RSS and the
  atom-cache counters after the build and at the end; `mixed_read_write` asserts that every
  ProtoDB node's readers saw the versions advance while writers committed.

Every suite run also records the stray-process check (`check.stray_processes`, `checked_at`,
`workdir_free_bytes`) in its results JSON and warns when the list is not empty.

Options:

| Option | Meaning |
|--------|---------|
| `--quick` | tiny parameter sets (used by `tests/perf/test_benchmark_harness.py`) |
| `--json OUT` | write environment, options and every result (all samples) to `OUT` |
| `--compare BASE` | flag medians worse than `BASE` by more than `--threshold` (default 15 %) and outside `BASE`'s own sample range; exit status 3 when any. Refused (exit 4, "NOT COMPARABLE") when the environments differ: GIL state, quick/full, CPU model or count, Python |
| `--allow-env-mismatch` | compare anyway across a different environment |
| `--plugin MODULE` | import an extra module of workloads (repeatable; passed on to isolated children) |
| `--param NAME.KEY=JSON` | override one parameter of one workload (repeatable), e.g. `inventory.objects=1000000` for a step of the inventory scale ladder |
| `--threads 1,2,4,8,16` | thread counts of the scaling workloads |
| `--only NAME ...` | run only these workloads |
| `--repeat N`, `--warmup N` | timed and untimed repetitions per variant (default 5 and 1; quick 2 and 1) |
| `--workdir DIR` | where databases are created (default `benchmarks/.work`, ignored by git) |
| `--rqlite PATH` | the `rqlited` binary (or `PROTODB_BENCH_RQLITED`); without it `rqlite_reference` is reported as pending |

The work directory must be on a real disk for the durability workloads: on `tmpfs` an
`fsync` costs nothing (the runner prints a warning, and the file system type is recorded
with the results).

## Methodology

- Each timed variant runs `warmup` untimed repetitions, then `repeat` timed ones, and
  **every** repetition is verified (`measure()` requires `expect=` or `check=`), warm-ups
  included: one wrong sample fails the run.
- Results are reported as the median and a tail per batch and per operation: p95 (nearest
  rank) with at least 20 samples, otherwise the maximum, labelled `max` (for throughput: the
  5th percentile, or the minimum below 20 samples). A figure from a single sample is flagged
  `SINGLE SAMPLE`.
- A workload that fails or crashes (in-process or in its isolated child) is recorded as
  failed with its traceback; the run goes on, writes the JSON and exits 1.
- The environment is captured with the results: CPU model, core count and affinity, Python
  version and build (`Py_GIL_DISABLED`), `sys._is_gil_enabled()`, `-X` options, the CPU
  frequency governor, driver, EPP and boost, library versions, the git revision and the work
  directory's file system.
- `sys._is_gil_enabled()` is checked after every workload. hnswlib re-enables the GIL when
  imported on a free-threaded interpreter, so `vector_knn_hnsw` is flagged `isolate` and runs
  in a child interpreter with the same `-X` options; any other workload that changes the GIL
  state fails the run.
- SQLite is a single-process reference only, at equal durability: ProtoDB `fsync` / `flush`
  / `none` against SQLite (WAL) `synchronous=FULL` / `NORMAL` / `OFF`. It is never used as a
  cluster reference (rulings R37, R45).
- Cluster mode is compared with the CAS-only baseline (storage-arbitrated writers, the
  reverted D5 approach), with itself at 1/2/4/8 peers (in-process and TCP subprocess peers),
  with itself at 0 and 30 ms object-store latency, with itself at 3 peers in `fsync`,
  `flush` and `none` durability (data not synced: acknowledged commits survive process crashes
  and a minority power loss, not a majority one; since Task 8.3e the acceptor log is synced in
  every mode, so `flush`/`none` differ from `fsync` in data syncs only), and with rqlite (3 nodes on loopback).
- Every TCP peer counts its `os.fsync`/`os.fdatasync` calls by role and call site
  (`benchmarks/fsync_probe.py`); `cluster_tcp` reports syncs per commit. To cross-check with
  strace, wrap the peers:
  `PROTODB_BENCH_PEER_WRAPPER="strace -f -qq -c -e trace=fsync,fdatasync -o /path/{node}.txt"`.
- Memory figures (RSS) are reported only where memory is what is measured: the inventory scale
  ladder (peak RSS of the building process).

## rqlite

rqlite is not a dependency. To include it, download an official release binary, verify its
published SHA-256 and point the suite at it (never install it system-wide for this):

```bash
mkdir -p /tmp/rqlite && cd /tmp/rqlite
curl -LO https://github.com/rqlite/rqlite/releases/download/v10.6.6/rqlite-v10.6.6-linux-amd64.tar.gz
sha256sum rqlite-v10.6.6-linux-amd64.tar.gz   # compare with the digest published on the release page
tar xzf rqlite-v10.6.6-linux-amd64.tar.gz
PROTODB_BENCH_RQLITED=/tmp/rqlite/rqlite-v10.6.6-linux-amd64/rqlited python -m benchmarks --only rqlite_reference
```

## Adding a workload

```python
from benchmarks.harness import BenchResult, benchmark, measure

@benchmark("my_workload", quick={"n": 100}, full={"n": 10_000})
def my_workload(ctx, p):
    """One-line description shown by --list."""
    expected = ...                                  # computed independently of ProtoDB
    result = BenchResult({"rows": p["n"]}, None, expected)
    samples, got = measure(lambda: ..., ctx.repeat, ctx.warmup, expect=expected)  # every run checked
    result.check("same rows as the model", got == expected, f"{got} != {expected}")
    result.add_timing("variant label", samples, per=p["n"])
    result.checksum = got
    return result
```

Import the module from `benchmarks/workloads/__init__.py`. `tests/perf/test_benchmark_harness.py`
runs every registered workload in quick mode and fails if one does not verify.
