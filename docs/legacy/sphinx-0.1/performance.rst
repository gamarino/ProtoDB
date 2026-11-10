Performance
===========

This page summarizes how to run ProtoBase's performance/benchmark suite and how to interpret results. It covers three areas:

- Indexed query benchmark (examples/indexed_benchmark.py)
- Simple CRUD benchmark (examples/simple_performance_benchmark.py)
- LINQ-like query performance (examples/linq_performance.py)

Prerequisites
-------------
- Python 3.11+
- From the project root, run benchmarks directly with the provided scripts (no package install required).

Indexed queries benchmark
-------------------------
The indexed_benchmark.py script compares three query paths over a synthetic dataset of Python dicts:

- python_list_baseline: a pure Python list comprehension filtering by two equalities + a numeric range
- protodb_linear_where: a WherePlan over a ListPlan (no secondary indexes)
- protodb_indexed_where: a WherePlan over an IndexedQueryPlan with secondary indexes on r.category, r.status, r.value (and a point-lookup path for r.id)

Run (example sizes):

.. code-block:: bash

    python examples/indexed_benchmark.py \
      --items 100000 --queries 50 --window 100 --warmup 10 \
      --categories 50 --statuses 20 \
      --out examples/benchmark_results_indexed.json

Parameters:
- --items: number of synthetic rows to generate
- --queries: number of random queries executed (each query: category == C AND status == S AND value BETWEEN (lo, hi))
- --window: numeric range window size for value; smaller → more selective
- --warmup: warmup iterations before timing
- --categories: domain cardinality for category (increase for higher selectivity)
- --statuses: domain cardinality for status (increase for higher selectivity)
- --out: output JSON file with results

Output JSON contains:
- timings_seconds: total wall clock per path
- latency_ms: per-query stats per path (avg_ms, p50_ms, p95_ms, p99_ms, qps)
- speedups: indexed_over_linear, indexed_over_python, and indexed_pk_over_linear (for point lookup path)

Recent observations (50k rows, 50 queries, window=100):
- Indexed over linear: ~6.4× speedup on selective AND+range queries at this scale
- Indexed over Python baseline: ~1.0× (similar) due to Python overheads at moderate sizes
- PK lookup path: current ad‑hoc wrapper can be slower than linear for single-point lookups at this size; a native primary-key map/collection would remove this overhead

Notes on engine behavior:
- Equality/IN/contains terms are served by secondary indexes using candidate-set intersections by selectivity
- Ranges (Between, <, <=, >, >=) are pushed down via IndexedRangeSearchPlan when available; otherwise applied as residual filters after intersections
- Indexes use native-type keys (no string normalization); RepeatedKeysDictionary maps key → Set(records)

Simple CRUD benchmark
---------------------
The simple_performance_benchmark.py script measures basic CRUD and a simple query loop over durable structures using MemoryStorage.

Run a full cycle on a small size:

.. code-block:: bash

    python examples/simple_performance_benchmark.py --count 100 --queries 20 --benchmark all

Outputs per phase:
- Insert N: total seconds and ms/item
- Read N: total seconds and ms/item
- Update N: total seconds and ms/item
- Delete N: total seconds and ms/item
- Query Q over N items: total seconds and ms/query

Use cases:
- Sanity-check that operations complete and to get ballpark latencies on your machine
- Compare MemoryStorage vs file/cluster/cloud backends by adapting the example patterns to those storages

LINQ-like performance
---------------------
The linq_performance.py script compares typical query shapes executed over:

- list: from_collection(list) — local evaluation
- plan: from_collection(ListPlan(list)) — pushdown for where/select to QueryPlan; ordering/distinct/grouping still evaluated locally in this phase

Run:

.. code-block:: bash

    python examples/linq_performance.py --size 50000 --runs 5 --out examples/benchmark_results_linq.json

Measured pipelines:
- filter_order_take: where + order_by + select + take
- distinct: distinct over a projection
- count_active: where + count (pushdown helps)
- between: range filter
- groupby: group_by + aggregates (heavier)

Output JSON records avg_ms, p50_ms, p95_ms, std_ms and qps per pipeline for both modes (list/plan).

Recent observations (50k rows, 5 runs):
- Filter+order+take: list and plan are similar; where/select pushdown helps but order/take run locally
- Distinct: similar between list and plan
- Count active: plan slightly faster due to filter pushdown
- Between and GroupBy: similar at this scale; variance dominated by Python runtime

Tips and troubleshooting
------------------------
- Run from the repository root so that proto_db is importable by the scripts.
- Keep window sizes modest (e.g., 50–200) to emphasize selectivity in indexed benchmarks.
- Warm up a few iterations before timing to stabilize JIT and cache effects.
- Persist results to JSON (--out ...) so you can compare across runs or environments.
- If you add native indexes to collections, expose a mapping field_name → RepeatedKeysDictionary via an IndexedQueryPlan so WherePlan can leverage them.

Artifacts
---------
Typical output locations created by the scripts:

- examples/benchmark_results_indexed.json
- examples/benchmark_results_linq.json

See also
--------
- Examples page for how to run the scripts: :doc:`examples`
- Query system details and optimizer plans: :doc:`api/queries`
- Cloud and cluster storage details (for persistence benchmarks): :doc:`storage_cloud`
