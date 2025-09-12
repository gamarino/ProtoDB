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
- Vector search: exact and ANN (IVF‑Flat; optional HNSW when available)
- Atom‑level caches (new in 2025):
  - AtomObjectCache (deserialized objects) and AtomBytesCache (raw bytes)
  - 2Q policy and single‑flight to cut repeated reads/deserializations
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

Indexed queries and vector ANN microbenchmarks are provided:

```bash
# Indexed benchmark (secondary indexes)
python examples/indexed_benchmark.py --items 50000 --queries 200 --out examples/benchmark_results_indexed.json

# Vector ANN benchmark (exact vs IVF‑Flat vs optional HNSW)
python examples/vector_ann_benchmark.py --n 20000 --dim 64 --queries 50 --k 10 --out examples/benchmark_results_vectors.json
```

See docs/performance.md for guidance, caveats on small datasets, and how to interpret results.

## Compatibility

- Works on standard CPython today; design is GIL‑agnostic and scales on free‑threaded Python builds without code changes.
- No third‑party runtime dependency is required for core features.

## License

MIT License. See LICENSE for details.



## Latest performance snapshot (2025-09-12)

A recent run of the comprehensive benchmark on in-memory storage with a small dataset (1000 items, 50 queries) produced the following results:
- insert: total_time=3.5555 s; time_per_item=3.5555 ms; items_per_second=281.26
- read: total_time=0.1555 s; time_per_item=0.1555 ms; items_per_second=6429.15
- update: total_time=2.9889 s; time_per_item=2.9889 ms; items_per_second=334.57
- delete: total_time=0.4149 s; time_per_item=0.4149 ms; items_per_second=2410.42
- query (50 queries): total_time=23.2793 s; time_per_query=465.5861 ms; queries_per_second=2.148

Reproduce locally:

```bash
python examples/comprehensive_benchmark.py --storage memory --size small --benchmark all --output benchmark_results.json
```

See docs/performance.md for details and caveats. The raw JSON for the latest run is in benchmark_results.json at the repository root.
