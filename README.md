# ProtoBase

[![PyPI version](https://img.shields.io/pypi/v/proto_db.svg)](https://pypi.org/project/proto_db/)
[![Python Version](https://img.shields.io/pypi/pyversions/proto_db.svg)](https://pypi.org/project/proto_db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

ProtoBase is an embedded, transactional, object‑oriented database for Python. It unifies structured data and vectors under a Pythonic, LINQ‑like query API with immutable secondary indexes, range‑aware optimization, and optional vector similarity search. New in 2025, ProtoBase adds an atom‑level caching layer that reduces tail latency for hot reads and an optional parallel scanning module with adaptive chunking and a lightweight work‑stealing scheduler. Defaults preserve current behavior; new features are opt‑in and dependency‑free.

## Why ProtoBase?

- Lightweight, in‑process engine: no external server to deploy or operate.
- Transactional object model: copy‑on‑write with transactional rebase keeps data and indexes consistent.
- Rich, Pythonic API: compose queries fluently (where/select/order_by/group_by) and call explain() to inspect plans.
- Hybrid capabilities: structured filters plus vector search in the same engine.
- Extensible and testable: clean abstractions, thorough unit tests, and clear docs.

## Key Features

- Transactions and persistence over multiple storage backends:
  - MemoryStorage (in‑memory)
  - StandaloneFileStorage (file‑based with WAL)
  - Cluster/Cloud storage variants (S3/GCS‑compatible) under a unified API
- LINQ‑like query API with pushdown and explain()
- Immutable secondary indexes; range operators with inclusive/exclusive bounds
- Advanced index-aware optimizer (2025): AndMerge for AND intersections, OrMerge for OR unions, and single-term index rewrites with efficient range traversal and reference-set intersection to minimize materialization
- Vector search: exact and ANN (IVF‑Flat; optional HNSW when available)
- Atom‑level caches (new in 2025):
  - Write-through on save: newly persisted atoms are immediately available in AtomObjectCache and AtomBytesCache
  - AtomObjectCache (deserialized objects) and AtomBytesCache (raw bytes) with 2Q policy and single‑flight de‑duplication
- Parallel scans (optional, new in 2025):
  - Adaptive chunking with EMA and clamped bounds
  - Work‑stealing scheduler with per‑worker local deques and metrics hooks
  - Backward‑compatible fallback to a simple thread‑pool/sequential mode
- Rich built‑ins: Dictionary, List, Set, HashDictionary

See docs for details: API reference, user guides, performance notes, and an ADR for the parallel module.

## Quick Examples

LINQ‑style filtering and projection:

```python
from proto_db.linq import from_collection, F

users = [
    {"id": 1, "first_name": "Alice", "last_name": "Zeus", "age": 30, "country": "ES", "status": "active", "email": "a@example.com", "last_login": 5},
    {"id": 2, "first_name": "Bob", "last_name": "Young", "age": 17, "country": "AR", "status": "inactive", "email": "b@example.com", "last_login": 10},
]

q = (from_collection(users)
     .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
     .order_by(F.last_login, ascending=False)
     .select({"id": lambda x: x["id"], "name": F.first_name + " " + F.last_name})
     .take(20))

res = q.to_list()
```

Optional parallel scan with adaptive chunking and work‑stealing:

```python
from proto_db.parallel import parallel_scan, ParallelConfig

data = list(range(100000))

def fetch(off, cnt):
    return data[off:off+cnt]

def process(x):
    return x*2 if x % 2 == 0 else None

cfg = ParallelConfig(max_workers=4, scheduler='work_stealing')
results = parallel_scan(len(data), fetch, process, config=cfg)
```

## Installation

ProtoBase requires Python 3.11 or higher. Install from PyPI:

```bash
pip install proto_db
```

Optional features like Arrow/Parquet bridging use pyarrow if you already have it installed; ProtoBase does not force any heavy dependencies.

## Documentation

- Sphinx docs (docs/source): introduction, quickstart, API reference
- Parallel Scans guide: docs/source/parallel_scans.rst
- API reference for the parallel module: docs/source/api/parallel.rst
- Performance suite: docs/performance.md
- ADR: docs/adr_work_stealing.md

To build the docs locally:

```bash
cd docs && make html
```

Open docs/_build/html/index.html in your browser.

## Testing

Run the full test suite:

```bash
python -m unittest discover proto_db/tests
```

Or run a specific file:

```bash
python -m unittest proto_db.tests.test_parallel
```

## Benchmarks

Indexed queries (including PK lookup) and vector ANN microbenchmarks are provided:

```bash
# Indexed benchmark (secondary indexes with higher selectivity and warmup)
python examples/indexed_benchmark.py \
  --items 100000 --queries 200 --window 100 --warmup 10 \
  --categories 50 --statuses 20 \
  --out examples/benchmark_results_indexed.json

# Vector ANN benchmark (exact vs IVF‑Flat vs optional HNSW)
python examples/vector_ann_benchmark.py --n 20000 --dim 64 --queries 50 --k 10 --out examples/benchmark_results_vectors.json
```

Notes:
- The indexed benchmark now includes a Primary Key (PK) lookup path comparing indexed vs linear and Python list baselines.
- See docs/performance.md for guidance, caveats on small datasets, and how to interpret results.

## Compatibility

- Works on standard CPython today; design is GIL‑agnostic and scales on free‑threaded Python builds without code changes.
- No third‑party runtime dependency is required for core features.

## License

MIT License. See LICENSE for details.



## Latest indexed query results (2025-09-13)

A recent run of the indexed benchmark on in-memory data produced these highlights (50k items, 200 queries, window=100, warmup=10):
- python_list_baseline: avg ≈ 5.27 ms; p95 ≈ 9.48 ms; QPS ≈ 189.60
- protodb_linear_where: avg ≈ 31.11 ms; p95 ≈ 42.35 ms; QPS ≈ 32.14
- protodb_indexed_where: avg ≈ 5.27 ms; p95 ≈ 6.63 ms; QPS ≈ 189.70
- Speedup indexed_over_linear: ≈ 5.90×

Primary Key (PK) lookup on the same dataset:
- python_list_pk_lookup: avg ≈ 5.72 ms; p95 ≈ 8.50 ms; QPS ≈ 174.70
- protodb_linear_pk_lookup: avg ≈ 26.82 ms; p95 ≈ 38.63 ms; QPS ≈ 37.28
- protodb_indexed_pk_lookup: avg ≈ 34.69 ms; p95 ≈ 40.07 ms; QPS ≈ 28.82
- Speedup indexed_pk_over_linear: ≈ 0.77× (indexed PK slower than linear in this configuration; a native PK map would remove overhead)

Artifacts: examples/benchmark_results_indexed.json

For guidance, tuning tips, and additional scenarios, see docs/performance.md.
