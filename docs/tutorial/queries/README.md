# The queries tutorial

An in-depth tutorial on querying `proto_db`: how a query is built, how the planner chooses an access path and
how to read its `explain()`, the indexes it chooses from, pagination, what a query declares to concurrent
commits, and the pitfalls that cost the most. It uses the ERP of the [complete tutorial](../README.md)
(users, groups, persons and opportunities, seeded by `examples/tutorial/erp_app.py`) and assumes its first
five chapters.

Every chapter is executed by the test suite, and every output shown is the one it printed. Each chapter also
exists as a self-checking script in `examples/tutorial/queries/`, which asserts what it prints:

```bash
python examples/tutorial/queries/01_building_queries.py
```

## Chapters

1. [Building queries](01-building-queries.md): `from_collection`, conditions with `F`, projections, order,
   windows, and the conditions only Python can evaluate.
2. [Aggregates, joins and relations](02-aggregates-and-joins.md): `group_by` with `Agg`, groups as rows,
   equi-joins, following references, flattening.
3. [Indexes and the planner](03-indexes-and-the-planner.md): choice by cost, reading `explain()` node by node,
   ordered reads, computed keys, unique indexes, and the index-equals-scan invariant.
4. [Pagination](04-pagination.md): offset windows, pagers and their tokens, key-ordered access on a dictionary.
5. [Range, vector and custom indexes](05-range-vector-and-custom-indexes.md): range bounds, a prefix as a
   range, filtered vector search, composite keys through extractors, and the `Index` hooks.
6. [Queries under concurrency](06-queries-under-concurrency.md): what each plan declares (`INDEX_GROUP`,
   `RANGE`, `WHOLE`), windows and key walks, escalation, and snapshots as the way out.
7. [Performance pitfalls](07-performance-pitfalls.md): scans, sorts, counts, index write costs, conflict
   width and deep offsets, with measured atom counts and cited suite figures.

## Reference

* [Indexing and querying](../../source/tutorials/indexing-querying.rst), the topic tutorial
* [The query planner](../../source/query-planner.rst) and [the index framework](../../source/indexing-framework.rst)
* [Writing a custom index](../../source/tutorials/custom-index.rst)
* [The concurrency model](../../source/concurrency-model.rst)
* [Performance](../../source/performance.rst)
