# ProtoBase

[![PyPI version](https://img.shields.io/pypi/v/proto_db.svg)](https://pypi.org/project/proto_db/)
[![Python Version](https://img.shields.io/pypi/pyversions/proto_db.svg)](https://pypi.org/project/proto_db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Project Status](https://img.shields.io/badge/status-0.1%20alpha-orange.svg)](#status)

> Status: 0.1 alpha (2025-09-16). Early preview — APIs may change before the first stable release. See CHANGELOG.md for details.

ProtoBase is an embedded, transactional, object‑oriented database for Python. It provides immutable collections, secondary indexes, and a simple, index‑aware query planner — all running in‑process without a separate server.

This README reflects the current state of the codebase (2025‑09‑14). For deeper docs, see the Sphinx site under docs/ (Concepts and Cookbook included).

## Why ProtoBase?

- Lightweight, in‑process engine: no external server to deploy or operate.
- Transactional object model: copy‑on‑write with safe concurrent commits and rebase.
- Pythonic collections: List, Dictionary, Set, CountedSet.
- Secondary indexes with an optimizer that can use them transparently.
- Clear docs and a growing test suite (including concurrency and property‑based tests).

## Key Features (Implemented)

- Transactions and persistence over multiple storage backends:
  - MemoryStorage (in‑memory)
  - StandaloneFileStorage (file‑based with WAL)
- Immutable secondary indexes on collections that support indexing
- Query system:
  - WherePlan with Expression/Term DSL
  - IndexedQueryPlan with basic index‑aware optimization
  - AndMerge and GroupByPlan utilities for composition and aggregation
  - explain() to inspect plans
- Collections: Dictionary, RepeatedKeysDictionary, List (AVL), Set, CountedSet
- Ephemeral vs. persistent behavior in Set/CountedSet to avoid unintended writes when hashing Atoms
- LINQ‑like query API (proto_db.linq) with lazy pipelines, grouping/ordering, and explain(); integrates with collection indexes when applicable
- Parallel hybrid executor with work‑stealing (HybridExecutor) to overlap I/O and CPU tasks across pipelines and storage operations
- Vector similarity search components: exact index and optional HNSW ANN via hnswlib (install with `pip install "proto_db[vectors]"`)

## Installation

Requirements: Python 3.11+

From PyPI:

```bash
pip install proto_db
```

Optional extras (declared in pyproject.toml):

- Parquet/Arrow integration (future/optional):
  ```bash
  pip install "proto_db[parquet]"
  ```
- Vector helpers (experimental placeholder):
  ```bash
  pip install "proto_db[vectors]"
  ```
- Development tooling (tests, docs, build):
  ```bash
  pip install "proto_db[dev]"
  ```

These extras are optional; core features do not require third‑party packages.

## Quickstart

Create a database, store a collection as a root, and read it back:

```python
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage

# Create an in‑memory ObjectSpace and a database
space = ObjectSpace(MemoryStorage())
db = space.new_database("ExampleDB")

# Write
tr = db.new_transaction()
nums = tr.new_list().append_last(1).append_last(2).append_last(3)
tr.set_root_object("numbers", nums)
tr.commit()

# Read
tr2 = db.new_transaction()
loaded = tr2.get_root_object("numbers")
print(list(loaded.as_iterable()))  # [1, 2, 3]
tr2.commit()
space.close()
```

### Indexing and Querying

Add an index to a list and run a filter that the optimizer can use:

```python
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import WherePlan, Expression

# Minimal setup (separate from the previous snippet)
space = ObjectSpace(MemoryStorage())
db = space.new_database("ExampleDB")

tr = db.new_transaction()
people = tr.new_list()
people = people.append_last({"id": 1, "city": "NY"})
people = people.append_last({"id": 2, "city": "SF"})

people = people.add_index("city")  # enable secondary index

plan = WherePlan(
    based_on=people.as_query_plan(),
    filter=Expression(field="city", op="==", value="NY"),
    transaction=tr,
)
print(plan.explain())  # should indicate an IndexedSearchPlan
results = list(plan.execute())
```

See also the runnable example at examples/collection_indexing_example.py.

### LINQ-like Queries (Phase 1)

ProtoBase includes a lazy, composable LINQ-like API that works over Python iterables as well as ProtoBase collections. It can optimize when the source is an indexed collection.

Example:

```python
from proto_db.linq import from_collection, F

# Over a plain Python list
people = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 22},
    {"name": "Carol", "age": 27},
]

q = (
    from_collection(people)
    .where(F.age >= 25)
    .order_by(F.age.desc())
    .select({"name": F.name, "age": F.age})
    .take(2)
)
print(list(q))  # [{'name': 'Alice', 'age': 30}, {'name': 'Carol', 'age': 27}]
```

See docs for using the same API over ProtoBase collections and how indexes are used automatically when available.

### Vector similarity (ANN)

ProtoBase provides a small vector search module with an exact index and an optional HNSW ANN index based on hnswlib. Install extras with:

```bash
pip install "proto_db[vectors]"
```

Minimal usage:

```python
from proto_db.vector_index import HNSWVectorIndex, ExactVectorIndex

# Build an index (HNSW requires hnswlib + numpy; falls back to exact when unavailable)
idx = HNSWVectorIndex(metric="cosine", M=16, efConstruction=200, efSearch=64)
idx.build(vectors=[[0.1, 0.2], [0.0, 0.9], [0.4, 0.4]], ids=["a", "b", "c"])  # toy example

# k-NN query
neighbors = idx.search(query=[0.1, 0.25], k=2)
print(neighbors)  # [(id, score), ...]
```

See examples/vector_ann_benchmark.py and docs/performance.md for benchmarks and configuration tips.

## Documentation

Sphinx documentation lives under docs/ and includes:

- Concepts guide: docs/source/concepts.(md|rst)
- Cookbook recipes: docs/source/cookbook.(md|rst)
- LINQ-like API: docs/source/api/linq.rst
- Vector API: docs/source/api/vectors.rst
- Performance notes: docs/performance.md
- API reference (autodoc) and additional guides listed in docs/source/index.rst

Build locally:

```bash
cd docs && make html
```

Open docs/_build/html/index.html in your browser.

## Testing

Run the full test suite:

```bash
python -m unittest discover proto_db/tests
```

Run a specific test module:

```bash
python -m unittest proto_db.tests.test_db_access_with_standalone_file_storage
```

The suite includes concurrency stress tests, property‑based tests (Hypothesis, optional), and integration round‑trip tests.

## Examples

- examples/collection_indexing_example.py — show indexing and query speedup on a simple dataset.

## Compatibility

- CPython 3.11+ supported.
- Core features have no mandatory third‑party dependencies.

## License

MIT License. See LICENSE for details.
