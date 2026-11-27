# 11. Where to go next

You have built the ERP's model, queried it, written it from several threads, read it as of last week, crashed
and restored it, extended it with modules, searched it by meaning and run it on a cluster. This chapter has no
code: it is the map of what to read next.

## The two in-depth tutorials

* [The queries tutorial](queries/README.md): building queries, filters, projections, ordering, joins,
  aggregates, how the planner chooses an index and how to read `explain()`, pagination, range, vector and
  custom indexes, what a query declares under concurrency, and the performance pitfalls, measured.
* [The semantic model tutorial](semantic-model/README.md): declaring a model, fields, relations and roles,
  computed attributes and aggregates, validation, intents, modules and composition, parents and C3, schema
  evolution, and as-of reads with the modules of their time.

## The topic tutorials

Each one is a page of the Sphinx documentation whose code blocks all run in the test suite:

| Tutorial | Read it for |
|---|---|
| [Getting started](../source/tutorials/getting-started.rst) | the four nouns, values that do not change, history, files |
| [Modelling data](../source/tutorials/modelling-data.rst) | records, collections, values against entities, a versioned order, the relational contrast |
| [Indexing and querying](../source/tutorials/indexing-querying.rst) | indexes, several conditions, computed keys, what a query declares |
| [Concurrency and retries](../source/tutorials/concurrency.rst) | units of every collection, check and add, unique indexes, identifiers without a counter, lock cells |
| [Vector search](../source/tutorials/vector-search.rst) | the exact and IVF-Flat backends, recall measured |
| [Writing a custom index](../source/tutorials/custom-index.rst) | a trigram index, and how to prove it correct |
| [Operations](../source/tutorials/operations.rst) | durability modes measured, inspection, backup of a live space, damage |
| [The ERP example](../source/tutorials/erp.rst) | as-of queries, audits, stable reports, the storage cost of history |
| [The CAD example](../source/tutorials/cad.rst) | reusing decoded objects across transactions, and its limit |
| [Cluster](../source/tutorials/cluster.rst) | local and linearizable reads, the GIL and acknowledgements |
| [Extensible models](../source/tutorials/extensible-models.rst) | parents, modules, catalog objects, C3, every registry error |

## The reference

* [Concepts](../source/concepts.rst): values, atoms, collections, transactions, indexes and queries.
* [The concurrency and isolation model](../source/concurrency-model.rst): every rule of chapter 5, the commit
  protocol and the known limits.
* [The query planner](../source/query-planner.rst) and [the index framework](../source/indexing-framework.rst).
* [Durability and recovery](../source/durability.rst) and [the storage format](../source/format-spec.rst).
* [Objects across transactions](../source/objects-across-transactions.rst) and
  [the architecture](../source/architecture.rst).
* [Performance](../source/performance.rst): the measured results by workload, with their sources.
* [The API](../source/api.rst), generated from the docstrings of the `proto_db` package. With no hosted site,
  build it locally: `python -m sphinx -b html docs/source docs/build/html`.

## The design guide

[The design guide](../source/design-guide.rst) explains why the design is what it is, each choice against
the obvious alternative: [immutability](../source/design-guide/immutability.rst),
[modelling](../source/design-guide/modelling.rst), [history links](../source/design-guide/history-links.rst),
[referential integrity](../source/design-guide/referential-integrity.rst),
[the pager and rank](../source/design-guide/pager-and-rank.rst),
[cache layers](../source/design-guide/cache-layers.rst), [the registry](../source/design-guide/registry.rst),
[the coordinator](../source/design-guide/coordinator.rst) and
[unknown outcomes](../source/design-guide/unknown-outcomes.rst). The principles every decision cites are in
[design principles](../design-principles.md), and the decisions themselves in [the decision log](../decisions.md).

## The examples

* The tutorials' scripts: [`examples/tutorial/`](../../examples/tutorial), one per chapter of each tutorial,
  each generated from its chapter and checking every output the chapter shows.
* The short examples: [`examples/`](../../examples/README.md), self-checking scripts for files, indexed and
  LINQ queries, concurrency and vector search.
* The demos: [the ERP demo](../kit/erp_demo.py) and [the semantic model demo](../kit/semantic/README.md).

Back to [the tutorial's contents](README.md).
