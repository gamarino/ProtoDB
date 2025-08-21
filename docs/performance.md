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
