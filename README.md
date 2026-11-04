# ProtoDB

[![PyPI version](https://img.shields.io/pypi/v/proto_db.svg)](https://pypi.org/project/proto_db/)
[![Python Version](https://img.shields.io/pypi/pyversions/proto_db.svg)](https://pypi.org/project/proto_db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Project Status](https://img.shields.io/badge/status-2.0.0.dev0-orange.svg)](#status)

ProtoDB is an embedded, transactional, object-oriented database for Python. It stores immutable,
persistent collections with secondary indexes, runs queries through an index-aware planner, and
commits serializable transactions per root, all in-process, without a separate server.

### Status

`2.0.0.dev0`: the 2.0 rebuild (design: `docs/superpowers/specs/2026-11-01-protodb-v2-design.md`,
decisions: `docs/decisions.md`). The 0.1 implementation has been removed; the API below is the 2.0
API. The performance suite and the rewritten documentation site are still to come, so APIs may
change before 2.0.0 is released.

### A note on names

One project, two spellings, and they are not interchangeable:

| | Name |
|---|---|
| The project, the repository, and the documentation | **ProtoDB** — `github.com/gamarino/ProtoDB` |
| The Python distribution and the import | **`proto_db`** — `pip install proto_db`, `import proto_db` |

The package keeps its underscore spelling because renaming it would break every
importer; everything else says ProtoDB. Earlier documentation called the project
*ProtoBase*, and some checkouts still live in a directory by that name — it is the
same project. The old spelling survives in `ProtoBaseException`, a deprecated alias of
`ProtoDBError` (the base of the exception hierarchy) that is removed in 2.1.

## Design principles

ProtoDB is built from a small set of principles. Every design decision cites them, and
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

- **Transactions**: snapshot isolation, read sets checked at commit (a conflict raises
  `ProtoConflictError`; `Database.run` retries), per-key merges of collections written without being
  read, named mutable cells (`MutableObject`), a version history per database.
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
  coordinator for several writer processes. Every node syncs its acceptor log (lease promises,
  accepted roots) before replying in every durability mode, so the lease never grants a term twice,
  even across power loss. On a TieredStorage a node accepts a root with ONE synced acceptor-log
  entry that also holds the root's records (D50, Task 8.3f), so acknowledged commits survive power
  loss of any set of nodes in every durability mode; the mode governs only writes outside the
  commit path. Replication is bounded (D54, Task 8.3g): three voters (configurable) order roots,
  the other members are learners, and a commit's records are synced on the committer plus `f`
  data replicas (one, with three voters), not on every member; others fetch records on demand.
  Losing `f + 1` voters stops writes while learners keep serving reads; losing every copy of a
  commit's records (`f + 1` disks) before the persister uploads them loses that data, as no
  re-replication runs. Two to four contending writer processes take
  the lease in turn; at eight, and for hot-key read-modify-write transactions, fairness is not
  yet guaranteed (open defect D-LIVE-2).
- **Free-threading**: tested on CPython 3.14t (GIL disabled) and with the GIL; `parallel_scan` and
  `parallel_filter` scale on free-threaded builds.
- **Interop**: export to Apache Arrow and Parquet (`to_arrow`, `to_parquet`).

## Installation

Requirements: Python 3.11+.

```bash
pip install proto_db
```

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

## Examples

Runnable, self-checking scripts live in `examples/` (see `examples/README.md`): getting started,
file storage, indexed queries with `explain()`, LINQ queries, transactions under concurrency, and
vector search. Each one runs in the test suite.

## Documentation

- Design specification: `docs/superpowers/specs/2026-11-01-protodb-v2-design.md`
- Decision log: `docs/decisions.md`
- API: the docstrings of the `proto_db` package; its module docstring lists the public names, each of which is documented.

The Sphinx site under `docs/source` is being rewritten for 2.0; the 0.1 pages are kept for
reference in `docs/legacy/`.

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
