# The semantic model tutorial

An in-depth tutorial on the semantic model of `proto_db`: a database that describes itself. A registry module
declares what each entity is, its fields, its relations in both directions, what is computed and what is
checked; installing the module writes that description, the catalog, into the database, where any program,
tool or agent can read it. The tutorial declares the ERP of the [complete tutorial](../README.md) (users,
groups, persons, opportunities, quotes) in `examples/tutorial/erp_model.py`, and extends it with the modules
`erp_forecast.py`, `erp_regions.py`, `erp_labels.py` and `erp_crm.py` in the same directory.

It assumes chapters 1 and 3 of the complete tutorial; chapter 3 here also relies on its chapter 5
(concurrency), and chapters 5 and 7 on its chapter 8 (the runtime registry). Every chapter is executed by the
test suite, and every output shown is the one it printed. Each chapter also exists as a self-checking script
in `examples/tutorial/semantic/`:

```bash
python examples/tutorial/semantic/01_declaring_a_model.py
```

## Chapters

1. [Declaring a model](01-declaring-a-model.md): registry modules, type names, the descriptors, installing,
   and what the catalog says (and what it does not enforce).
2. [Entities and relations](02-entities-and-relations.md): cells and versions, both ends of a relation,
   relationships that carry data, binding to a version, and query trees.
3. [Computed attributes and aggregates](03-computed-and-aggregates.md): the expression language, maintained
   aggregates moved by deltas, and the contention they create.
4. [Validation and intents](04-validation-and-intents.md): what an operation refuses, what a commit refuses,
   invariants, and intents with preconditions.
5. [Modules and composition](05-modules-and-composition.md): extensions, provenance, a subtype from another
   module, behaviour through the registry, and the rules of composition.
6. [Schema evolution](06-schema-evolution.md): a new aggregate and a new required field over existing
   entities, migrating the data, and what a module cannot change.
7. [History](07-history.md): as-of reads with the model and the modules of their time, and rereading the
   version a report read.

## Reference

* [Extensible models](../../source/tutorials/extensible-models.rst): parents, the runtime registry, C3 and its
  errors
* [Modelling data](../../source/tutorials/modelling-data.rst): values and entities, versioned records
* [The semantic-model kit](../../kit/semantic/README.md): the same ideas on a sales domain, with an agent over MCP
* [The registry chapter of the design guide](../../source/design-guide/registry.rst)
* [The semantic model design](../../superpowers/specs/2026-11-15-semantic-model-mvp-design.md) and the
  [format spec](../../source/format-spec.rst), "Semantic catalog (format 6)"
