# ProtoBase Performance Suite (Indexed Queries)

This document explains how to run the performance suite with emphasis on indexed collections and the index-aware query engine.

## Highlights

- Equality, IN, and CONTAINS predicates are served via secondary indexes by building candidate sets and intersecting them by selectivity.
- Range predicates (Between, <, <=, >, >=): the optimizer can push down ranges when using an IndexedRangeSearchPlan; otherwise they are applied as residual filters after intersecting equality-based candidates.
- Secondary indexes are represented as RepeatedKeysDictionary per field, mapping key -> Set(records) with native-type keys (no string normalization).

## Run the indexed benchmark

From the repository root:

```bash
python examples/indexed_benchmark.py --items 100000 --queries 200 --window 100 --warmup 10 \
  --categories 50 --statuses 20 \
  --out examples/benchmark_results_indexed.json
```

Parameters:
- `--items`: number of synthetic rows to generate.
- `--queries`: number of random AND+BETWEEN queries to execute.
- `--window`: numeric range window size for the value field (e.g., 50–200 for higher selectivity).
- `--warmup`: warmup iterations per query path before timing (JIT/cache effects).
- `--categories`: number of distinct categories to generate (increase for higher selectivity on equality terms).
- `--statuses`: number of distinct statuses to generate (increase for higher selectivity on equality terms).
- `--out`: path to write the JSON results.

The script writes a JSON with total timings (seconds), per-path latency stats, and derived speedups:
- `timings_seconds`: total wall-clock per path
- `latency_ms`: per-query stats per path: `avg_ms`, `p50_ms`, `p95_ms`, `p99_ms`, and `qps`
- `speedups`: `indexed_over_linear` and `indexed_over_python`
- Paths: `python_list_baseline` (list of dicts), `protodb_linear_where` (ListPlan, no indexes), `protodb_indexed_where` (WherePlan over IndexedQueryPlan with indexes on `category`, `status`, `value`).

Example output (small dataset):

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

Notes:
- On small datasets, index overhead can dominate, sometimes performing worse than a linear scan. As dataset size grows (e.g., 100k–1M), indexed evaluation is designed to outperform linear scans thanks to selective candidate set intersections.
- The current benchmark uses in-memory data and focuses on query time. Persisted storage performance may vary.

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


## Latest indexed benchmark results (2025-09-13)

We ran the indexed benchmark multiple times to capture behavior across sizes and query counts. Results below were produced with the current implementation and default settings.

- Run A — 5k items, 100 queries
  - python_list_baseline: 0.0448 s
  - protodb_linear_where: 0.4522 s
  - protodb_indexed_where: 0.5776 s
  - indexed_over_linear: 0.783×
  - indexed_over_python: 0.078×

- Run B — 50k items, 100 queries
  - python_list_baseline: 0.5671 s
  - protodb_linear_where: 4.4256 s
  - protodb_indexed_where: 5.6779 s
  - indexed_over_linear: 0.779×
  - indexed_over_python: 0.100×

- Run C — 50k items, 200 queries
  - python_list_baseline: 1.5030 s
  - protodb_linear_where: 11.9670 s
  - protodb_indexed_where: 11.2799 s
  - indexed_over_linear: 1.061×
  - indexed_over_python: 0.133×

Interpretation and tips:
- On small datasets (5k) or wider numeric ranges, index overhead can outweigh gains; linear scans may be faster.
- With more queries and tighter range windows, indexed evaluation starts to overtake linear scans as equality terms (category/status) drive down candidate sets.
- To accentuate index selectivity and increase QPS:
  - Use narrower numeric ranges (e.g., window of 200–1000 instead of 5000+).
  - Add additional equality predicates on indexed fields where possible.
  - Ensure your IndexedQueryPlan exposes a field->RepeatedKeysDictionary mapping; equality terms benefit the most today.
  - When you need range pushdown, prefer a plan that produces an IndexedRangeSearchPlan (the optimizer may do this when it can prove the field is indexed).

Reproduce any run, for example Run C:

```bash
python examples/indexed_benchmark.py --items 50000 --queries 200 --window 500 --warmup 10 --out examples/benchmark_results_indexed.json
```

The script writes a JSON alongside the parameters; see "Run the indexed benchmark" above for details.


## Nuevo Benchmark: Búsqueda por Clave Primaria (Point Query)

Para evaluar el rendimiento en el caso de uso más favorable para los índices, se ha añadido un benchmark de "búsqueda por clave primaria". Este test mide el tiempo necesario para encontrar un único registro utilizando su campo `id`.

- `protodb_indexed_pk_lookup`: Ejecuta una consulta `WHERE r.id == ?` sobre una colección con un índice en el campo `id`. Se espera que esta sea la operación más rápida, con un coste de `O(log N)`.
- `protodb_linear_pk_lookup`: Ejecuta la misma consulta sobre una colección sin índices, forzando un escaneo lineal de todos los elementos (`O(N)`).
- `python_list_pk_lookup`: Línea base en Python, realizando la búsqueda en una lista de diccionarios.

Interpretación de resultados:
- El `speedup` `indexed_pk_over_linear` debería crecer de forma significativa a medida que aumenta `--items`. Un valor muy bajo sugiere sobrecarga en el motor de consultas o ineficiencia en la búsqueda por índice.

Ejemplo de ejecución (añadido al script `examples/indexed_benchmark.py`):

```bash
python examples/indexed_benchmark.py --items 50000 --queries 200 --window 500 --warmup 10 --out examples/benchmark_results_indexed.json
```

Los nuevos campos se incorporan en el JSON de salida: `python_list_pk_lookup`, `protodb_linear_pk_lookup`, `protodb_indexed_pk_lookup` y `speedups.indexed_pk_over_linear`.

## Latest Results (2025-09-13)

This section summarizes the most recent benchmark run using the indexed query engine with reference-set intersection and efficient range scans, including the new Primary Key (PK) lookup paths.

Commands executed from the repository root:

```bash
# Run: 50k items, 200 queries, window=500, warmup=10
python examples/indexed_benchmark.py \
  --items 50000 --queries 200 --window 500 --warmup 10 \
  --out examples/benchmark_results_indexed.json
```

Artifacts written:
- examples/benchmark_results_indexed.json

Highlights (items=50k, window=500):
- python_list_baseline: avg ≈ 5.85 ms; p95 ≈ 7.97 ms; QPS ≈ 170.76
- protodb_linear_where: avg ≈ 42.70 ms; p95 ≈ 49.39 ms; QPS ≈ 23.41
- protodb_indexed_where: avg ≈ 28.62 ms; p95 ≈ 36.57 ms; QPS ≈ 34.94
- Speedup indexed_over_linear: ≈ 1.49x

PK lookup (point query) results on the same dataset:
- python_list_pk_lookup: avg ≈ 5.85 ms; p95 ≈ 7.80 ms; QPS ≈ 170.94
- protodb_linear_pk_lookup: avg ≈ 28.05 ms; p95 ≈ 31.69 ms; QPS ≈ 35.65
- protodb_indexed_pk_lookup: avg ≈ 29.55 ms; p95 ≈ 34.94 ms; QPS ≈ 33.84
- Speedup indexed_pk_over_linear: ≈ 0.95x (indexed PK slightly slower than linear in this configuration)

Interpretation:
- Indexed query execution is faster than the linear WherePlan by ~1.5x for the AND+range workload at 50k items and window 500. Lower p95 on the indexed path aligns with candidate-set intersection before residual filtering.
- For PK lookup, the indexed path remains slightly slower than the linear scan in this in-memory, small-object benchmark. This suggests setup overheads (wrapper records, plan/expression construction, index traversal) dominate the O(log N) advantage for point queries at this scale. The linear path benefits from tight loops over a Python list of dicts.

Recommendations to observe the expected large PK speedup:
- Increase dataset size substantially (e.g., 500k–5M items) so linear scan cost grows O(N) while indexed PK remains near O(log N) + small constant.
- Reuse compiled expressions and plans across iterations to amortize construction overhead.
- Ensure the index on `r.id` maps directly from native key to the record reference without extra wrapping; avoid per-lookup iteration over all keys.
- Optionally switch storage to StandaloneFileStorage and persist objects to leverage stable AtomPointer hashing and caches.

Tuning tips:
- Increase selectivity to accentuate index benefits:
  - Reduce the range `--window` (e.g., 50–200) so fewer candidates fall within value ranges.
  - Increase domain cardinality for equality predicates (more categories/statuses) to shrink candidate sets before intersection.
- Increase dataset size (`--items` 100k–1M+) to observe larger gains from indexes as linear scan cost grows O(N) while index operations remain near O(log K) + intersection of small sets.
- Warmup (`--warmup`) allows caches and Python internals to stabilize before timing.

Engine notes:
- Indexes map native-type keys (no str() coercion) to sets of row references.
- AND queries are evaluated by collecting per-term reference frozensets, sorting by size, intersecting them, then materializing only the small final candidate set and applying residual filters.
- Range predicates (Between, <, <=, >, >=) use lower-bound binary search followed by a sequential in-order walk until the upper bound is exceeded, collecting references without materializing objects.
