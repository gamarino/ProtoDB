# The proto_db tutorial

A complete, progressive tutorial of `proto_db` 2.0.0rc5, to read from start to finish. It builds one small
application chapter by chapter: a modelled ERP with **users, groups, persons and opportunities**. It is a
model only, with no ERP features: enough to show every part of the database on data that means something.

Every chapter is runnable. Its code blocks run in order, like the cells of a notebook, and every `Output` shown
is what the code printed: the test suite runs each chapter (`tests/docs/test_markdown_tutorials.py`) and fails
when a figure changes. Each chapter with code also has a script in [`examples/tutorial/`](../../examples/tutorial),
generated from the chapter, that checks every output and prints `<script>: OK` at the end:

```text
pip install --pre proto_db                 # chapter 9 also needs: pip install --pre "proto_db[vectors]"
python examples/tutorial/05_concurrency.py
```

The chapters share a few modules in `examples/tutorial/` (`erp_app.py`, the plain model; `erp_model.py`, the
semantic model; `erp_labels.py` and `erp_crm.py`, two extension modules), and the scripts put that directory
on `sys.path`. To run a chapter's code by hand, do the same.

## Chapters

1. [Install and first database](01-first-database.md): memory and file storage, and the life of a transaction
   from begin to commit or abort.
2. [Objects and immutable collections](02-values-and-collections.md): records, `Dictionary`, `List`, `Set`,
   `HashDictionary` and `CountedSet`; structural sharing measured on disk, and why values are immutable.
3. [Modelling](03-modelling.md): values against entities, versions that point at their predecessor, and the
   semantic model with relations, roles, `Computed` and maintained aggregates.
4. [Indexes and queries](04-indexes-and-queries.md): equality and range indexes, the planner, `explain()`, LINQ
   queries, grouping and a stable pager.
5. [Concurrency: preconditions and intentions](05-concurrency.md): the headline of 2.0.0rc5. Whole-root
   optimistic locking and where it fails, what a transaction records, why writers of different keys both
   commit, the conflict units, and `Database.run` retries.
6. [Time](06-time.md): as-of reads, an audit without an audit table, and stable pages while others write.
7. [Durability and operations](07-durability-and-operations.md): durability modes, a process killed mid-commit,
   inspection with `verify`, backup and restore, and the format 7 upgrade note.
8. [Extensible models and the runtime registry](08-extensible-models.md): parents, registry modules that add
   behaviour without rewriting data, `_super`, an explicit reload, and as-of reads with the modules of their time.
9. [Vector search](09-vector-search.md): a vector index on the opportunities, nearest neighbours, and `near`
   combined with ordinary conditions.
10. [Cluster](10-cluster.md): what the coordinator gives you, local reads that may lag, and linearizable reads.
11. [Where to go next](11-where-next.md): every topic tutorial, the reference, the design guide and the examples.

## Two tutorials in depth

* [The queries tutorial](queries/README.md): every part of querying, from filters to the planner, pagination,
  custom indexes, queries under concurrency and the performance pitfalls.
* [The semantic model tutorial](semantic-model/README.md): declaring a model that describes itself, relations
  and roles, computed attributes, validation, modules, schema evolution and history.

The complete tutorial stays introductory where these two go deep, and links to them.

## Conventions

* Every figure is printed by the code beside it. Timing-dependent figures are shown as a `Sample output` of one
  run, and the code asserts the property they illustrate instead.
* The product is `proto_db` (`pip install proto_db`, `import proto_db`); the repository is protoBase.
* The rules behind the behaviour are in the reference pages under [`docs/source`](../source), which the
  chapters link. There is no hosted documentation site: the links open the reStructuredText sources, which
  GitHub renders, and `python -m sphinx -b html docs/source docs/build/html` builds the site locally.
