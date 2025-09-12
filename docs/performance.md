# ProtoBase Performance Suite (Indexed)

This document describes the updated performance suite that exercises ProtoBase with secondary indexes and compares results with a simple competitor baseline.

## What’s new

- Range operators and index-aware query planning were added to the query engine.
- Core collections now support immutable secondary indexes (IndexRegistry in lists/sets/dictionaries for internal maintenance).
- A new benchmark script demonstrates indexed query performance vs linear scan and vs a Python list baseline.

## Running the indexed benchmark

From the repository root:

```bash
python examples/indexed_benchmark.py --items 50000 --queries 200 --out examples/benchmark_results_indexed.json
```

Parameters:
- `--items`: number of synthetic rows to generate.
- `--queries`: number of random AND+BETWEEN queries to execute.
- `--out`: path to write the JSON results.

The script produces a JSON file with timings (seconds) and derived speedups:
- `python_list_baseline`: list comprehension over plain Python list of dicts.
- `protodb_linear_where`: ProtoBase WherePlan over a ListPlan (no indexes, linear scan).
- `protodb_indexed_where`: Uses prebuilt RepeatedKeysDictionary indexes to intersect candidate sets by `category`, `status`, and a numeric `value` range.

Example output (small dataset):

```json
{
  "config": {"n_items": 1000, "n_queries": 10},
  "timings_seconds": {
    "python_list_baseline": 0.00074,
    "protodb_linear_where": 0.00975,
    "protodb_indexed_where": 0.01621
  },
  "speedups": {
    "indexed_over_linear": 0.60,
    "indexed_over_python": 0.046
  }
}
```

Notes:
- On very small datasets, index overhead can dominate and perform worse than a linear scan. For larger `--items` (e.g., 50k–500k), the indexed path should outperform the linear scan significantly.
- The competitor baseline here is a Python list comprehension, serving as a simple, dependency‑free baseline. You can extend the benchmark to compare with sqlite3 or another store if desired.

## How indexes are used

For the indexed path, the script builds three secondary indexes using RepeatedKeysDictionary:
- `r.category` -> Set(row)
- `r.status` -> Set(row)
- `r.value` -> Set(row), with numeric keys stored as strings

Queries are evaluated by:
1. Fetching the candidate set for category.
2. Intersecting with the candidate set for status.
3. Uniting buckets from the value index for keys in the (lo, hi) range and intersecting with the running result.

This mirrors the selectivity-ordered progressive intersection strategy that the WherePlan can use with index metadata.

## Additional tips

- To test the range operator pushdown inside the query engine end‑to‑end, integrate an IndexedQueryPlan where `.indexes` is a field->RepeatedKeysDictionary mapping and use a WherePlan with `Between` over that field. Some of the example code in the engine assumes Python `dict`‑like checks for index existence; ensure you use a mapping compatible with those parts, or adapt the benchmark as done here.
- If you already maintain IndexRegistry inside your collections, you can expose a thin adapter that presents the required field->value->set view to the query engine, enabling direct reuse.

# ProtoBase Performance Suite (Indexed)

This document describes the updated performance suite that exercises ProtoBase with secondary indexes and compares results with a simple competitor baseline.

## What’s new

- Range operators and index-aware query planning were added to the query engine.
- Core collections now support immutable secondary indexes (IndexRegistry in lists/sets/dictionaries for internal maintenance).
- A new benchmark script demonstrates indexed query performance vs linear scan and vs a Python list baseline.
- A new vector ANN benchmark script evaluates HNSW and IVF-Flat vs exact and other optional baselines.

## Running the indexed benchmark

From the repository root:

```bash
python examples/indexed_benchmark.py --items 50000 --queries 200 --out examples/benchmark_results_indexed.json
```

Parameters:
- `--items`: number of synthetic rows to generate.
- `--queries`: number of random AND+BETWEEN queries to execute.
- `--out`: path to write the JSON results.

The script produces a JSON file with timings (seconds) and derived speedups:
- `python_list_baseline`: list comprehension over plain Python list of dicts.
- `protodb_linear_where`: ProtoBase WherePlan over a ListPlan (no indexes, linear scan).
- `protodb_indexed_where`: Uses prebuilt RepeatedKeysDictionary indexes to intersect candidate sets by `category`, `status`, and a numeric `value` range.

Example output (small dataset):

```json
{
  "config": {"n_items": 1000, "n_queries": 10},
  "timings_seconds": {
    "python_list_baseline": 0.00074,
    "protodb_linear_where": 0.00975,
    "protodb_indexed_where": 0.01621
  },
  "speedups": {
    "indexed_over_linear": 0.60,
    "indexed_over_python": 0.046
  }
}
```

Notes:
- On very small datasets, index overhead can dominate and perform worse than a linear scan. For larger `--items` (e.g., 50k–500k), the indexed path should outperform the linear scan significantly.
- The competitor baseline here is a Python list comprehension, serving as a simple, dependency‑free baseline. You can extend the benchmark to compare with sqlite3 or another store if desired.

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
- `r.value` -> Set(row), with numeric keys stored as strings

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
