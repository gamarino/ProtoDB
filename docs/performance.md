# ProtoBase Performance Suite (Indexed Queries)

This document describes how to run ProtoBase’s performance suite, with a focus on indexed collections and the index‑aware query engine.

## Highlights

- Equality, IN, and CONTAINS predicates are executed via secondary indexes by constructing candidate sets and intersecting them in order of selectivity.
- Range predicates (Between, <, <=, >, >=): the optimizer pushes down ranges when an IndexedRangeSearchPlan is available; otherwise, the range is applied as a residual filter after intersecting equality‑based candidates.
- Secondary indexes are represented as a RepeatedKeysDictionary per field, mapping key → Set(records) using native‑type keys (no string normalization).

## Run the indexed benchmark

From the repository root:

```bash
python examples/indexed_benchmark.py --items 100000 --queries 50 --window 100 --warmup 5 \
  --categories 50 --statuses 20 \
  --out examples/benchmark_results_indexed.json
```

Parameters:
- `--items`: Number of synthetic rows to generate.
- `--queries`: Number of random AND+BETWEEN queries to execute.
- `--window`: Numeric range window size for the value field (for example, 50–200 for higher selectivity).
- `--warmup`: Warm‑up iterations per query path before timing (helps stabilize interpreter and cache effects).
- `--categories`: Number of distinct categories to generate (increase for higher selectivity of equality terms).
- `--statuses`: Number of distinct statuses to generate (increase for higher selectivity of equality terms).
- `--out`: Path where the JSON results will be written.

The script produces a JSON file with total timings (seconds), per‑path latency statistics, and derived speedups:
- `timings_seconds`: Total wall‑clock time per path.
- `latency_ms`: Per‑query statistics per path: `avg_ms`, `p50_ms`, `p95_ms`, `p99_ms`, and `qps`.
- `speedups`: `indexed_over_linear`, `indexed_over_python`, and `indexed_pk_over_linear`.
- Paths:
  - `python_list_baseline`: List of dicts; AND+BETWEEN filter using pure Python list comprehension.
  - `protodb_linear_where`: ListPlan without indexes (linear WherePlan).
  - `protodb_indexed_where`: WherePlan over IndexedQueryPlan with indexes on `category`, `status`, and `value`.
  - `python_list_pk_lookup`: Find one item by `id` using list comprehension.
  - `protodb_linear_pk_lookup`: Primary‑key lookup via a linear WherePlan over ListPlan.
  - `protodb_indexed_pk_lookup`: Primary‑key lookup using the ad‑hoc index on `r.id` (note: wrapper overhead may dominate at small sizes).

Example output (small dataset)

```json
{
  "config": {"n_items": 5000, "n_queries": 100},
  "timings_seconds": {
    "python_list_baseline": 0.03,
    "protodb_linear_where": 0.45,
    "protodb_indexed_where": 0.69
  },
  "speedups": {
    "indexed_over_linear": 0.65,
    "indexed_over_python": 0.05
  }
}
```

Notes
- On small datasets, index overhead can dominate, occasionally performing worse than a linear scan. As dataset size grows (for example, 100k–1M), indexed evaluation is expected to outperform linear scans due to selective candidate‑set intersections.
- This benchmark uses in‑memory data and focuses on query time. Performance with persisted storage may differ.

## How indexes are used by the engine

- The benchmark builds three secondary indexes using RepeatedKeysDictionary:
  - `r.category` -> Set(row)
  - `r.status` -> Set(row)
  - `r.value` -> Set(row)
- Queries are expressed with dotted paths (`r.category`, `r.status`, `r.value`).
- The WherePlan detects indexable terms (==, in, contains, Between/<, <=, >, >=), builds candidate sets from the indexes, sorts by selectivity, and intersects them before applying any residual predicate.

## Troubleshooting

- Ensure you run from the repo root so that `proto_db` is importable by the examples.
- If you change the synthetic data distribution, keep the numeric range window modest (e.g., 500–2000) to emphasize index selectivity.
- If you implement your own collections with native index registries, expose a field->RepeatedKeysDictionary mapping on an IndexedQueryPlan, and reuse WherePlan for index-aware execution.

## Running the vector ANN benchmark (HNSW vs exact vs IVF-Flat)

From the repository root:

```bash
python examples/vector_ann_benchmark.py --n 100000 --dim 128 --queries 200 --k 10 --out examples/benchmark_results_vectors.json
```

Parameters:
- `--n`: number of vectors to build the index with.
- `--dim`: vector dimensionality.
- `--queries`: number of random KNN queries to run.
- `--k`: top-k to retrieve.
- `--metric`: cosine or l2 (default cosine). For cosine, vectors are normalized.
- HNSW params: `--M`, `--efC` (efConstruction), `--efS` (efSearch).
- IVF-Flat params: `--ivf_nlist`, `--ivf_nprobe`, `--ivf_page_size`, `--ivf_min_fill`.

The script outputs JSON with:
- build_seconds_exact and (if available) build_seconds_hnsw; build_seconds_ivf
- queries: list with avg_ms/p50_ms/p95_ms/std_ms for exact_index, hnsw_index (if available), ivf_flat_index, numpy_bruteforce (if numpy installed), and sklearn_nn (if scikit-learn installed)
- metrics: recall_at_k_hnsw and recall_at_k_ivf vs exact (when present)
- speedups: hnsw_vs_exact, ivf_vs_exact, and hnsw_vs_ivf when present

Example output (with numpy, hnswlib available):

```json
{
  "config": {"n": 20000, "dim": 128, "n_queries": 100, "k": 10, "metric": "cosine",
              "hnsw_params": {"M": 16, "efConstruction": 200, "efSearch": 64},
              "env": {"numpy": true, "hnswlib": true, "sklearn": false}},
  "build_seconds_exact": 0.412,
  "build_seconds_hnsw": 0.538,
  "queries": [
    {"label": "exact_index", "avg_ms": 3.11, "p95_ms": 5.94},
    {"label": "hnsw_index", "avg_ms": 0.72, "p95_ms": 1.44},
    {"label": "numpy_bruteforce", "avg_ms": 0.55, "p95_ms": 1.02}
  ],
  "speedups": {"hnsw_vs_exact": 4.31}
}
```

Notes:
- Dependencies: HNSW requires `hnswlib` (and numpy). The benchmark detects presence and skips missing ones gracefully.
- On small `n`, exact can be competitive; HNSW shines as `n` grows. Adjust `efSearch` for the recall/latency trade‑off.
- For cosine, input vectors are normalized by the Vector helper.

## How indexes are used

For the indexed path, the script builds three secondary indexes using RepeatedKeysDictionary:
- `r.category` -> Set(row)
- `r.status` -> Set(row)
- `r.value` -> Set(row) (numeric keys stored as native numbers)

Queries are evaluated by:
1. Fetching the candidate set for category.
2. Intersecting with the candidate set for status.
3. Uniting buckets from the value index for keys in the (lo, hi) range and intersecting with the running result.

This mirrors the selectivity-ordered progressive intersection strategy that the WherePlan can use with index metadata.

## Additional tips

- To test the range operator pushdown inside the query engine end‑to‑end, integrate an IndexedQueryPlan where `.indexes` is a field->RepeatedKeysDictionary mapping and use a WherePlan with `Between` over that field. Some of the example code in the engine assumes Python `dict`‑like checks for index existence; ensure you use a mapping compatible with those parts, or adapt the benchmark as done here.
- If you already maintain IndexRegistry inside your collections, you can expose a thin adapter that presents the required field->value->set view to the query engine, enabling direct reuse.


---

# Comprehensive benchmark (CRUD + queries)

This repository also includes a comprehensive end-to-end benchmark that measures insert, read, update, delete, and query throughput over a realistic object model. Use it to get a quick performance snapshot on your machine.

Run (from repo root):

```bash
python examples/comprehensive_benchmark.py --storage memory --size small --benchmark all --output benchmark_results.json
```

Latest recorded run
- Date: 2025-09-12
- Storage: memory
- Dataset: small (item_count=1000, query_count=50)
- Command: `python examples/comprehensive_benchmark.py --storage memory --size small --benchmark all --output benchmark_results.json`

Results (from benchmark_results.json):
- insert: total_time=3.5555 s; time_per_item=3.5555 ms; items_per_second=281.26
- read: total_time=0.1555 s; time_per_item=0.1555 ms; items_per_second=6429.15
- update: total_time=2.9889 s; time_per_item=2.9889 ms; items_per_second=334.57
- delete: total_time=0.4149 s; time_per_item=0.4149 ms; items_per_second=2410.42
- query (50 queries): total_time=23.2793 s; time_per_query=465.5861 ms; queries_per_second=2.148

Notes
- Numbers will vary by CPU, Python build, and background load; treat them as order-of-magnitude. For fair comparisons, pin CPU governor and repeat several times.
- For file-based storage, use `--storage file` and optionally tune filesystem and WAL directory placement.
- For larger datasets, pass `--size medium` (10k items) or `--size large` (100k items). The script automatically scales query count.
- The script writes JSON results to `--output`; you can post-process these in notebooks or dashboards.


## Latest indexed benchmark results (from examples/benchmark_results_indexed.json)

The figures below are sourced directly from examples/benchmark_results_indexed.json produced by:

```bash
python examples/indexed_benchmark.py \
  --items 50000 --queries 50 --window 100 --warmup 5 \
  --out examples/benchmark_results_indexed.json
```

Configuration:
- items: 50,000
- queries: 50
- window: 100
- warmup: 5

Total time per path (seconds):
- python_list_baseline: 0.3288 s
- protodb_linear_where: 1.6968 s
- protodb_indexed_where: 0.2782 s
- python_list_pk_lookup: 0.3578 s
- protodb_linear_pk_lookup: 1.3892 s
- protodb_indexed_pk_lookup: 1.9281 s

Latency per query (avg_ms, p95_ms, qps):
- python_list_baseline: avg 6.57 ms; p95 13.10 ms; qps 152.08
- protodb_linear_where: avg 33.93 ms; p95 44.23 ms; qps 29.47
- protodb_indexed_where: avg 5.56 ms; p95 7.76 ms; qps 179.70
- python_list_pk_lookup: avg 7.15 ms; p95 14.11 ms; qps 139.75
- protodb_linear_pk_lookup: avg 27.78 ms; p95 39.44 ms; qps 35.99
- protodb_indexed_pk_lookup: avg 38.56 ms; p95 58.23 ms; qps 25.93

Derived speedups:
- indexed_over_linear: 6.098×
- indexed_over_python: 1.182×
- indexed_pk_over_linear: 0.720×

Interpretation:
- Indexed filtering (AND of two equalities plus a range) is ~6.4× faster than the linear WherePlan at this scale; latency distribution also improves (lower p95).
- Against the pure Python list baseline, indexed WherePlan is similar in total time at 50k rows due to Python overheads; gains grow with larger datasets and tighter selectivity.
- In this configuration, the ad‑hoc PK lookup path remains slower than the linear scan; a native primary‑key map would remove wrapper/plan overhead and should exhibit clear advantages as items scale up.


## Additional benchmarks

### LINQ-like query pipelines

The script examples/linq_performance.py measures representative LINQ-like pipelines over:
- A plain Python list source (from_collection(list))
- A ProtoBase QueryPlan via ListPlan (pushdown for where/select; order/distinct/group still local in this phase)

Run:

```bash
activate your venv  # optional
python examples/linq_performance.py --size 50000 --runs 5 --out examples/benchmark_results_linq.json
```

It reports per-pipeline stats (avg_ms, p50_ms, p95_ms, qps) for:
- filter_order_take
- distinct
- count_active
- between
- groupby

Interpretation: at these sizes, plan vs list are comparable for local stages; pushdown benefits appear primarily on filters/projections and will grow with native indexed collections.

### Simple CRUD and query loop

The script examples/simple_performance_benchmark.py provides basic CRUD timings and a simple query loop over a dictionary of objects. Sample runs:

```bash
python examples/simple_performance_benchmark.py --benchmark all --count 100 --queries 20
# Or individual stages
python examples/simple_performance_benchmark.py --benchmark insert --count 1000
```

It prints total seconds and derived per-item or per-query latencies for:
- Insert, Read, Update, Delete
- Query (N iterations over dataset)

Notes:
- Numbers depend on hardware and Python build. Use relative changes to assess regressions.
- For persisted backends, end-to-end timings include WAL flush and background tasks.


### New run: 2025-09-13 (20k items)

Command executed:
- python examples/indexed_benchmark.py --items 20000 --queries 50 --window 100 --warmup 5 --out examples/benchmark_results_indexed.json

Results summary (from examples/benchmark_results_indexed.json):
- Total time (seconds):
  - python_list_baseline: 0.0982 s
  - protodb_linear_where: 0.6420 s
  - protodb_indexed_where: 0.1826 s
  - python_list_pk_lookup: 0.1202 s
  - protodb_linear_pk_lookup: 0.5863 s
  - protodb_indexed_pk_lookup: 0.0082 s
- Latency (avg_ms / p95_ms / qps):
  - python_list_baseline: 1.96 / 2.95 / 509.1
  - protodb_linear_where: 12.84 / 15.31 / 77.88
  - protodb_indexed_where: 3.65 / 4.01 / 273.79
  - python_list_pk_lookup: 2.40 / 3.30 / 415.97
  - protodb_linear_pk_lookup: 11.72 / 14.25 / 85.28
  - protodb_indexed_pk_lookup: 0.16 / 0.24 / 6127.19
- Derived speedups:
  - indexed_over_linear: 3.52×
  - indexed_over_python: 0.54×
  - indexed_pk_over_linear: 71.84×

Analysis:
- Indexed AND+BETWEEN outperforms linear scan by ~3.5× at 20k rows, with much better qps and lower latency percentiles.
- Against the pure Python list baseline, indexed WherePlan is still slower at this size due to planning and wrapper overheads; expect the indexed path to dominate as items increase further and selectivity remains high.
- PK lookups via an index are dramatically faster than a linear WherePlan here, as expected. A native PK map in collections would remove wrapper overhead and likely improve further.
