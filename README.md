# proto_db

[![PyPI version](https://img.shields.io/pypi/v/proto_db.svg)](https://pypi.org/project/proto_db/)
[![Python Version](https://img.shields.io/pypi/pyversions/proto_db.svg)](https://pypi.org/project/proto_db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Project Status](https://img.shields.io/badge/status-2.0.0rc5_(unreleased)-orange.svg)](#status)

`proto_db` is an embedded, transactional, object-oriented database for Python. It stores immutable,
persistent collections with secondary indexes, runs queries through an index-aware planner, and
commits serializable transactions whose conflicts are as narrow as a dictionary key, a set element or a
cell, all in-process, without a separate server.

### Status

`2.0.0rc4` is the published release candidate of the 2.0 rebuild (design:
`docs/superpowers/specs/2026-11-01-protodb-v2-design.md`, decisions: `docs/decisions.md`). This tree is the
next one, `2.0.0rc5` (unreleased): it replaces conflict detection per root by the **precondition concurrency
model** (decision D73, `docs/source/concurrency-model.rst`), a behaviour-incompatible change listed in the
CHANGELOG, and the spaces it writes cannot be opened by earlier releases (format 7). The 0.1 implementation has
been removed; the API below is the 2.0 API.
Everything this page lists is implemented and covered by the test suite, which runs on CPython 3.14t
(free-threaded) and with the GIL. The cluster is new: it is documented, tested and measured in this
repository and has no production history. Open defects are listed where they apply (see the cluster item
under Features). The performance suite has run (results by workload in `docs/source/performance.rst`);
the API may still change before 2.0.0 is released.

### A note on names

The product is named after its import. The repository has its own name, and the sibling
Smalltalk project keeps its name too:

| | Name |
|---|---|
| The product (the library, the database) and its documentation | **`proto_db`** — `pip install proto_db`, `import proto_db` |
| The repository | **protoBase** — `github.com/gamarino/ProtoDB` |
| The Smalltalk sibling project | **protoBaseST** |

Earlier documentation called the product *ProtoDB* or *ProtoBase*; both are the same
product, now named `proto_db` everywhere so that the name matches the import. The old
spelling survives in `ProtoBaseException`, a deprecated alias of `ProtoDBError` (the base
of the exception hierarchy) that is removed in 2.1.

## Tutorial

Start with the [complete tutorial](docs/tutorial/README.md): eleven chapters that build one modelled ERP
(users, groups, persons and opportunities) from a first database to a cluster. Every code block runs in the test
suite, every output shown is the one it printed, and each chapter is also a self-checking script in
`examples/tutorial/`.

1. [Install and first database](docs/tutorial/01-first-database.md): memory and file storage, and the life of a
   transaction.
2. [Objects and immutable collections](docs/tutorial/02-values-and-collections.md): records and the collections,
   with structural sharing measured on disk.
3. [Modelling](docs/tutorial/03-modelling.md): values against entities, versions, and an introduction to the
   semantic model.
4. [Indexes and queries](docs/tutorial/04-indexes-and-queries.md): indexes, the planner, `explain()`, LINQ
   queries and a stable pager.
5. [Concurrency: preconditions and intentions](docs/tutorial/05-concurrency.md): whole-root locking and where it
   fails, what a transaction records, the conflict units, and `Database.run` retries.
6. [Time](docs/tutorial/06-time.md): as-of reads, an audit without an audit table, stable pages while others
   write.
7. [Durability and operations](docs/tutorial/07-durability-and-operations.md): durability modes, a process killed
   mid-commit, `verify`, backup, and the format 7 upgrade.
8. [Extensible models](docs/tutorial/08-extensible-models.md): parents, registry modules, `_super`, an explicit
   reload, and as-of reads with the modules of their time.
9. [Vector search](docs/tutorial/09-vector-search.md): nearest neighbours, alone and with ordinary conditions.
10. [Cluster](docs/tutorial/10-cluster.md): the coordinator, local reads that may lag, linearizable reads.
11. [Where to go next](docs/tutorial/11-where-next.md): the topic tutorials, the reference and the design guide.

Two tutorials go deeper on the same ERP:

- [The queries tutorial](docs/tutorial/queries/README.md):
  [building queries](docs/tutorial/queries/01-building-queries.md),
  [aggregates and joins](docs/tutorial/queries/02-aggregates-and-joins.md),
  [indexes and the planner](docs/tutorial/queries/03-indexes-and-the-planner.md),
  [pagination](docs/tutorial/queries/04-pagination.md),
  [range, vector and custom indexes](docs/tutorial/queries/05-range-vector-and-custom-indexes.md),
  [queries under concurrency](docs/tutorial/queries/06-queries-under-concurrency.md) (what each plan declares to
  a concurrent commit) and [performance pitfalls](docs/tutorial/queries/07-performance-pitfalls.md), measured.
- [The semantic model tutorial](docs/tutorial/semantic-model/README.md):
  [declaring a model](docs/tutorial/semantic-model/01-declaring-a-model.md),
  [entities and relations](docs/tutorial/semantic-model/02-entities-and-relations.md),
  [computed attributes and aggregates](docs/tutorial/semantic-model/03-computed-and-aggregates.md),
  [validation and intents](docs/tutorial/semantic-model/04-validation-and-intents.md),
  [modules and composition](docs/tutorial/semantic-model/05-modules-and-composition.md),
  [schema evolution](docs/tutorial/semantic-model/06-schema-evolution.md) and
  [history](docs/tutorial/semantic-model/07-history.md), with the model of each version.

## Design principles

`proto_db` is built from a small set of principles. Every design decision cites them, and
a change that contradicts one is treated as conceptual debt, resolved before it is built.
If you read only one thing before the code, read
[docs/design-principles.md](docs/design-principles.md).

1. **Everything is immutable.** A pointer names one content forever. History is never lost,
   caches keyed by pointer never go stale, and snapshots are stable.
2. **A transaction is an explicit object.** It is not bound to a thread, and its commit
   concerns only its own reads and writes.
3. **Each node owns its work.** Commits are built where they happen, local contention is
   resolved first, and replication is bounded.
4. **Contention is resolved among the processes, never by the storage.** The storage only
   fences, and the object store is written after the commit.
5. **Safety never depends on time.** Timers drive liveness only, and delays can cause only
   conservative failures.
6. **Known techniques, honestly measured.** Established mechanisms are adopted and cited, and
   every performance claim carries its measurement.
7. **Correct before fast.**

## Features

- **Transactions**: snapshot isolation and serializable commits under the precondition model: every read
  and write declares what it saw of the transaction's snapshot (a dictionary key, a set element, a list
  position, a cell, an index group, a key range, a whole root), and the commit checks exactly that before it
  reapplies the transaction's changes on the newest state. Writers of different keys of one collection do not
  conflict, counters compose, writes made without reading are compared too (no last writer wins), and a
  conflict (`ProtoConflictError`, which `Database.run` retries) names the unit that failed. Named mutable
  cells (`MutableObject`), unique equality indexes, and a version history per database.
- **Values**: one value model with a total order across types (`1 == 1.0 == Decimal(1)`, `True != 1`),
  used identically by collections, indexes and queries.
- **Collections**: `List`, `Dictionary` (key-ordered), `HashDictionary`, `RepeatedKeysDictionary`,
  `Set`, `CountedSet` and `DBObject` records, all immutable and persistent (every change returns a new
  version; unchanged parts are shared).
- **Indexes on every collection**: `EqualityIndex`, `RangeIndex`, `VectorIndex` (exact, IVF-Flat, or
  HNSW when `hnswlib` is installed), and an `Index` base class for custom indexes. Indexes are
  persisted with the collection and maintained by every mutator.
- **Queries**: a LINQ front end (`from_collection`, `F`) and expression terms over physical plans; the
  planner picks indexes by cost, never drops a predicate, and `explain()` shows the plan that runs.
  Python lambdas are allowed and appear as an explicit `PythonFilter` node.
- **Storage**: `MemoryStorage`; `FileStorage` (append-only segments, CRC-checked records, `fsync` per
  commit by default); `CloudStorage` over S3 or Google Cloud Storage; `TieredStorage` and a cluster
  coordinator for several writer processes. The cluster is new, with no production history, and its
  guarantees assume that the storage devices honour `fsync`:
  - *Power loss with the disks intact* (D50, Task 8.3f): a node accepts a root with ONE synced
    acceptor-log entry that also holds the root's records, in every durability mode, so an acknowledged
    commit survives power loss of any set of nodes.
  - *Lease and ballots* (D46, Task 8.3e): every node syncs its acceptor log before replying, in every
    durability mode, so across process crashes and power losses no term is granted twice, provided at
    most one acceptor log is lost at a time (R48).
  - *Loss of disks* (D54, Task 8.3g): replication is bounded. Three voters (configurable) order roots, the
    other members are learners, and a commit's records are synced on the committer plus `f` data replicas
    (one, with three voters), not on every member; the others keep them only in a bounded cache and fetch on
    demand what they miss. Losing `f + 1` voters stops writes while learners keep serving reads; losing every
    durable copy of a commit's records (`f + 1` disks) before the persister uploads them to the object store
    loses that data, as no re-replication runs.
  - *Fairness*: two to four contending writer processes take the lease in turn; at eight, and for hot-key
    read-modify-write transactions, fairness is not yet guaranteed (open defect D-LIVE-2).
  - *Reads*: a local read is a committed snapshot that may lag; a linearizable read is a barrier commit,
    which adds a permanent version per read (see the cluster tutorial).
- **Semantic model** (an MVP): a registry module declares entities, fields, relations navigable from both
  sides, relationships with roles, computed attributes, maintained aggregates, invariants and intents;
  installing it writes a catalog into the database that any program or agent can read (see the semantic model
  tutorial). Field types and enumerations are catalogued, not yet enforced.
- **Extensible models**: a `DBObject` may have parents (other records, or registry names); `o.x` and
  `o["x"]` resolve own attributes, then the parents in C3 order, then the extension chains that the
  database's installed modules define. Methods and data defaults extend by the same rule, `_super()`
  reaches the next definition, and the database stores only data and module names, never code.
- **Free-threading**: tested on CPython 3.14t (GIL disabled) and with the GIL; `parallel_scan` and
  `parallel_filter` scale on free-threaded builds.
- **Interop**: export to Apache Arrow and Parquet (`to_arrow`, `to_parquet`).

## Installation

Requirements: Python 3.12+.

```bash
pip install --pre proto_db
```

Until 2.0.0 is final, the published versions are release candidates, so `pip` needs `--pre` (or an
explicit version such as `proto_db==2.0.0rc4`). Start with a single-node space: under heavy load the
cluster reports outcomes as unknown, or times out, more often than it should (see the CHANGELOG's known
limitations). On Python 3.14, end the process after a Ctrl-C that interrupted `proto_db` code: the
interrupt can leave one of its locks held (see the concurrency model).

Optional extras:

| Extra | Installs | Needed for |
|---|---|---|
| `vectors` | numpy | `VectorIndex` |
| `hnsw` | numpy, hnswlib | the HNSW vector backend (note: hnswlib re-enables the GIL on 3.14t) |
| `parquet` | pyarrow | `to_arrow`, `to_parquet`, `scan_parquet` |
| `s3` / `gcs` | boto3 / google-cloud-storage | `CloudStorage` with `S3ObjectStore` / `GCSObjectStore` |
| `dev` | pytest, hypothesis, sphinx, build, twine, ... | development |

`import proto_db` imports none of them and starts no thread; each optional package is imported when
the feature that needs it is first used.

## Quick start

```python
from proto_db import MemoryStorage, ObjectSpace

space = ObjectSpace(MemoryStorage())
db = space.new_database("example")

# A transaction commits when the block ends without an exception.
with db.new_transaction() as tr:
    numbers = tr.new_list().append_last(1).append_last(2).append_last(3)
    tr.set_root_object("numbers", numbers)
    tr.set_root_object("ada", tr.new_object(name="Ada Lovelace", born=1815))

tr = db.new_transaction()
print(list(tr.get_root_object("numbers")))   # [1, 2, 3]
print(tr.get_root_object("ada").name)        # Ada Lovelace
tr.abort()
space.close()
```

Use `FileStorage("/path/to/space")` instead of `MemoryStorage()` to persist the space;
`durability="fsync"` (the default), `"flush"` or `"none"` selects what a returned commit survives.

## Indexes and queries

```python
from proto_db import EqualityIndex, F, MemoryStorage, ObjectSpace, RangeIndex, from_collection

space = ObjectSpace(MemoryStorage())
db = space.new_database("people")
with db.new_transaction() as tr:
    people = tr.new_dictionary() \
        .add_index(EqualityIndex("by_city", "city")) \
        .add_index(RangeIndex("by_age", "age"))
    for key, city, age in [("ann", "Paris", 25), ("bob", "Rome", 40), ("cid", "Paris", 33)]:
        people = people.set_at(key, tr.new_object(name=key, city=city, age=age))
    tr.set_root_object("people", people)

tr = db.new_transaction()
people = tr.get_root_object("people")
query = from_collection(people).where((F.city == "Paris") & (F.age > 30))
print(query.explain()["plan"])            # IndexLookup or IndexRange, chosen by cost
print([p.name for p in query.to_list()])  # ['cid']
tr.abort()
space.close()
```

The LINQ front end works over plain iterables too:

```python
from proto_db import Agg, F, from_collection

orders = [{"customer": 1, "total": 10.0}, {"customer": 2, "total": 7.5}, {"customer": 1, "total": 2.5}]
totals = (from_collection(orders)
          .group_by(F.customer, {"customer": Agg.key(), "spent": Agg.sum(F.total)})
          .order_by(F.spent, descending=True))
print(totals.to_list())  # [{'customer': 1, 'spent': 12.5}, {'customer': 2, 'spent': 7.5}]
```

## Concurrency

```python
import threading
from proto_db import MemoryStorage, ObjectSpace

space = ObjectSpace(MemoryStorage())
db = space.new_database("counters")
with db.new_transaction() as tr:
    tr.set_root_object("hits", 0)

def increment(tr):
    tr.set_root_object("hits", tr.get_root_object("hits") + 1)

# A read-modify-write that loses the race raises ProtoConflictError; run() retries it.
threads = [threading.Thread(target=lambda: [db.run(increment, retries=100) for _ in range(50)])
           for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()

tr = db.new_transaction()
print(tr.get_root_object("hits"))  # 200
tr.abort()
space.close()
```

A collection root is not one unit: writers of different keys commit side by side, and a conflict names
the unit that failed.

```python
from proto_db import MemoryStorage, ObjectSpace, ProtoConflictError

space = ObjectSpace(MemoryStorage())
db = space.new_database("shop")
with db.new_transaction() as tr:
    tr.set_root_object("stock", tr.new_dictionary().set_at("pen", 10).set_at("ink", 5))

a, b, c = db.new_transaction(), db.new_transaction(), db.new_transaction()
for tr, item in ((a, "pen"), (b, "ink"), (c, "pen")):
    stock = tr.get_root_object("stock")
    tr.set_root_object("stock", stock.set_at(item, stock.get_at(item) - 1))
a.commit()
b.commit()                                  # another key: no conflict
try:
    c.commit()                              # the same key as a
except ProtoConflictError as error:
    print(error.unit.kind.name, error.unit.key)   # KEY pen
tr = db.new_transaction()
print(dict(tr.get_root_object("stock").items()))  # {'ink': 4, 'pen': 9}
tr.abort()
space.close()
```

[Chapter 5 of the tutorial](docs/tutorial/05-concurrency.md) starts from whole-root optimistic locking, shows
where it fails, and builds the precondition model on the ERP, unit by unit; the
[queries under concurrency](docs/tutorial/queries/06-queries-under-concurrency.md) chapter measures what each
query plan declares. The [concurrency topic tutorial](docs/source/tutorials/concurrency.rst) covers the units of every collection,
"check and add", identifiers without a counter, lock cells and the one deliberate escape (a `Snapshot`); the
[concurrency model](docs/source/concurrency-model.rst) states every rule and the known limits.

## Extensible models

A module extends a registry name; a record lists the name (through its catalog object) among its parents.
The database stores the record and the module names; the code lives in the imported module.

```python
import os, sys
from proto_db import MemoryStorage, ObjectSpace

with open("readme_contacts.py", "w") as f:   # a module, normally a file of the application
    f.write(
        "from proto_db.registry import module\n"
        "contacts = module('readme_contacts')\n"
        "@contacts.extends('readme.Partner')\n"
        "class Partner:\n"
        "    country = 'AR'                                  # a data default\n"
        "    def greeting(self):                             # a method\n"
        "        return f'Dear {self.name} ({self.country})'\n")
sys.path.insert(0, os.getcwd())

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
with db.new_transaction() as tr:
    tr.install_module("readme_contacts")             # added to the database's module list at commit

with db.new_transaction() as tr:
    partner_class = tr.catalog_object("readme.Partner")
    ana = tr.new_object(parents=(partner_class,), name="Ana")
    print(ana.greeting(), ana["country"], ana.phone)  # Dear Ana (AR) AR None: a miss is None
    print("country" in ana, ana.has_defined_attr("country"))  # True False: inherited, not own
space.close()
```

The [extensible-models tutorial](docs/source/tutorials/extensible-models.rst) covers `_super`, C3, catalog
objects, as-of reads with the modules of their time, and every error the registry raises.

## Examples

Runnable, self-checking scripts live in `examples/` (see `examples/README.md`): getting started,
file storage, indexed queries with `explain()`, LINQ queries, transactions under concurrency, and
vector search. `examples/tutorial/` holds one self-checking script per tutorial chapter, generated from the
chapter. Each one runs in the test suite, as do the Python snippets of this README and every code block of the
tutorials.

## Documentation

The Sphinx site is under `docs/source` (build it with `python -m sphinx -W -b html docs/source docs/build/html`):

- **The tutorials** under `docs/tutorial/` (Markdown, linked from the site's tutorials page): the complete
  tutorial, the queries tutorial and the semantic model tutorial (see [Tutorial](#tutorial) above).
- **Topic tutorials**, each executed by the test suite: getting started, modelling data, indexing and querying,
  concurrency and retries, vector search, writing a custom index, operations (durability, inspection,
  backup), an ERP example ("as of" queries, an audit with no audit table, stable paginated reports), a CAD
  example (reusing decoded objects across transactions), a cluster tutorial (local against linearizable
  reads) and extensible models (parents, modules and the runtime registry).
- **Design**: concepts, architecture, the concurrency and isolation model, the index framework, the query
  planner, durability and recovery, objects across transactions, the storage format.
- **Reference**: the API (generated from the docstrings of the `proto_db` package, whose module docstring
  lists the public names) and the performance results by workload profile.
- Design specification: `docs/superpowers/specs/2026-11-01-protodb-v2-design.md`; decision log:
  `docs/decisions.md`; principles: `docs/design-principles.md`.

The 0.1 pages are kept for reference in `docs/legacy/`.

## Testing

```bash
pip install -e ".[dev]"
python -m pytest            # runs tests/ (pytest's testpaths)
```

The suite has unit, contract, property-based (Hypothesis), integration, crash (fault injection
and SIGKILL), concurrency and static-check layers. The primary target is the free-threaded build:
`python3.14t -m pytest`, and once more with `-X gil=1`.

## License

MIT License. See LICENSE for details.
