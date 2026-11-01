# ProtoDB 2.0 — Design

Date: 2026-11-01. Status: approved for unattended implementation (maintainer granted full design freedom).
Branch: `fix/dict-merge-and-wal-durability` (F0 already committed as 087ec14).

## 1. Intent

ProtoDB is an embedded, transactional, object-oriented database for Python built on immutable,
structurally shared collections. A review on 2026-11-01 (claude.ai session + three review agents,
all findings reproduced) showed that the 0.1 code base reached a green test suite by tuning code
to the tests: integer values merged as `+1`, special cases for keys named `counter` and `k`,
~280 broad `except` handlers (126 of them `pass`), commits that were not durable, indexes lost on
the first write, and queries that silently return empty or partial results.

ProtoDB 2.0 keeps the ideas that are sound — persistent AVL collections, roots with history,
atomic root publication, read-set validation with retry — and rebuilds the rest so that every
claim is backed by a test that would fail without it.

### Maintainer requirements (verbatim intent)

1. Complete repair; full design freedom; no users and no existing databases to preserve.
2. **Every collection supports associated indexes** (none by default). There are no
   non-indexable collections, so a query can always consult the indexes a collection has.
3. Index kinds: **equality, range, vector**. New kinds are added **by extending base classes**;
   all hooks needed for that must exist and be verified.
4. Exhaustive, professional tests, run on **Python 3.14t** (free-threaded) to include real
   multithreading.
5. A complete performance suite.
6. Complete design documentation and tutorials.
7. Dictionary concurrent merge = per-key net-effect replay (decided before the freedom grant).

### Goals

- Correctness first: ACID at root granularity, crash safety at every write point, no silent
  failure anywhere on a correctness path.
- One index model shared by all collections, one planner shared by all query front ends.
- Free-threading safe: correct and scalable under CPython 3.14t without the GIL.
- Measurable: a self-verifying benchmark suite with baselines.

### Non-goals

- SQL. A network server. Backward compatibility with 0.1 data or API.
- Distributed consensus. Multi-node operation is supported only through compare-and-swap of the
  root on a shared medium (see §9); there is no custom voting protocol.

### Anti-goals (a failure even if tests pass)

- Any code path that recognises test data (key names, collection names, `pytest` in `sys.modules`).
- Any merge that infers intent from value types.
- Any `except` that hides an error on a commit, storage, index or query path.
- Benchmarks that do not verify the work they report.
- An index whose results differ from a full scan.

## 2. Decision log

Every design call is recorded in `docs/decisions.md` (ADR-style, one entry per decision, with the
alternatives considered). The decisions taken in this document are D1–D16 below.

| ID | Decision |
|----|----------|
| D1 | Full repair; the internals are rebuilt module by module under the existing concepts. |
| D2 | One value model with a total order across types (§4). Index results and scan results use the same comparator. |
| D3 | Canonical types: `None, bool, int, float, Decimal, str, bytes, date, time, datetime (naive or aware), timedelta, UUID, tuple, list, dict`, plus atoms. |
| D4 | On-disk format v2: framed binary records, msgpack payloads, CRC32, per-segment header (§6). A `protodb inspect` CLI replaces "readable files". Parquet is an export format, not the store. |
| D5 | Cloud/cluster: CAS on the root via conditional writes; data strictly before root; UDP voting removed. |
| D6 | Durability: fsync per commit by default; `durability="flush"` and `"none"` are explicit opt-outs for tests/benchmarks. Group commit is a later optimisation. |
| D7 | Strings are stored inline. The global `Literal` catalog is removed; reading a string returns `str`. |
| D8 | `Dictionary` becomes a key-ordered persistent AVL tree (O(log n)), not a binary search over a positional list (O(log² n)). |
| D9 | Conflict ⇒ retry. Read sets include absent roots. `ConcurrentOptimized` merge (per-key/per-element op-log replay) applies only to roots this transaction did not read. |
| D10 | Atom identity: every atom gets a stable `oid` (UUID4), persisted (assigned lazily on first use — amended by D21). Equality and hashing of atoms use `oid`, never `id()` or the storage pointer. |
| D11 | `MutableObject` is kept with fixed semantics: a named mutable cell with read-set conflict detection. |
| D12 | Single exception hierarchy rooted at `ProtoDBError`; `ProtoBaseException` remains as an alias for one release. |
| D13 | `hnswlib` is optional and imported lazily (it re-enables the GIL on 3.14t). The default vector backend is exact search with numpy; IVF-Flat is persistent and built in. |
| D14 | Python floor stays 3.11; CI matrix 3.11–3.14 plus 3.14t. Unused dependencies (`six`, `wrapt`) are dropped. |
| D15 | Package split into subpackages (§3). Top-level `proto_db` re-exports the public API. |
| D16 | A static checker test forbids swallowed exceptions and test-aware code in `proto_db/`. |

## 3. Package layout

```
proto_db/
  __init__.py            public API re-exports
  errors.py              exception hierarchy (D12)
  values.py              value model: canonical types, total order key, equality (D2, D3)
  core/
    atom.py              Atom, AtomPointer, oid, registry, lazy load
    codec.py             atom <-> msgpack-ready dict, type tags (D3)
    dbobject.py          DBObject (immutable record), MutableObject (D11)
  storage/
    base.py              SharedStorage / BlockProvider contracts
    format.py            record framing, segment header, root record (D4)
    memory.py            MemoryStorage
    file.py              FileBlockProvider + FileStorage (WAL segments, recovery)
    cloud.py             CloudStorage + clients (S3, GCS) with conditional root writes (D5)
    cache.py             bounded, thread-safe atom caches
  collections/
    base.py              IndexedCollection base: index plumbing shared by every collection
    avl.py               persistent AVL node machinery (positional and keyed)
    lists.py             List
    dictionaries.py      Dictionary, RepeatedKeysDictionary
    hash_dictionaries.py HashDictionary
    sets.py              Set, CountedSet
  indexes/
    base.py              Index base class + KeySpec + hooks contract
    equality.py          EqualityIndex
    range.py             RangeIndex
    vector.py            VectorIndex + backends (exact, ivf_flat, hnsw-optional)
  query/
    expressions.py       Expression, Term, operators
    plans.py             physical plans
    planner.py           index selection and cost model
    linq.py              LINQ front end (builds the same plans)
  transactions.py        ObjectSpace, Database, Transaction, retry helper
  parallel.py            parallel scan (threads; real speedup on 3.14t)
  fsm.py                 FSM (no test code)
  tools/inspect.py       `python -m proto_db.tools.inspect <space>` dump to JSON
benchmarks/              self-verifying performance suite (§12)
docs/                    Sphinx: concepts, design, tutorials, API, decisions (§13)
tests/                   moved out of the package (§11)
```

Module size rule: no module above ~800 lines; split by responsibility.

## 4. Value model (D2, D3)

`values.py` defines what a storable value is and how values compare. It is the single source of
truth for the collections (key order), the indexes and the query evaluator.

- `canonical(value)` validates and normalises a value; `list`/`dict` values become frozen containers (`FrozenList`/`FrozenDict`, subclasses whose mutators raise) so stored values can never be mutated through reads (ruling R13) (raises `ProtoValidationError` for an
  unsupported type). `float('nan')` is rejected as a key (it breaks total order) but allowed as a
  stored value.
- `order_key(value) -> tuple` returns a key that totally orders all canonical values:
  `(rank, payload)` with ranks `None < bool < number < str < bytes < date < time < datetime
  < timedelta < UUID < tuple < atom`. Numbers (`int`, `float`, `Decimal`) share one rank and
  compare **exactly** (Python's native mixed comparison is exact; never convert to `float`).
  `bool` has its own rank: `True` is not the number 1 inside ProtoDB.
  Naive and aware datetimes get separate ranks (they are not comparable in Python).
  Atoms order by `oid`.
- `values_equal(a, b)` ⇔ `order_key(a) == order_key(b)`. So `1 == 1.0 == Decimal(1)` is true,
  `True == 1` is false, `10**16 + 1 != 10**16`.
- `hash_key(value)` is consistent with `values_equal` (used by EqualityIndex, Set, HashDictionary
  buckets).

## 5. Atoms and serialization

- `Atom` has `oid: UUID` (assigned lazily on first observation or save, persisted; D21), `atom_pointer` (set when saved),
  `transaction`. Persisted state = public instance attributes; underscore attributes are runtime
  only. Classes register by a stable type name (`__type_name__`, default `module.ClassName`), so
  third-party subclasses (custom indexes) cannot collide.
- `codec.py` converts an atom's state into a msgpack-ready dict using explicit type tags for every
  canonical type (D3) and `{"@ref": [segment_hex, offset, type_name]}` for nested atoms (D18). Round-trip is exact for
  every canonical type (property-tested).
- Lazy loading: an atom loaded by pointer is a stub until first attribute access; loading is
  thread-safe (striped lock pool, D21, or idempotent publish — never a torn half-loaded object visible to
  another thread).
- Saving is explicit and happens inside commit, children first, exactly once per atom.

## 6. Storage format v2 (D4, D6)

- A space directory holds `space.root` and WAL segments named `<uuid4-hex>.seg`.
- Segment header (once): magic `b"PDB2SEG\0"`, format version (u16 LE), segment UUID.
- Record: `length:u32 LE | kind:u8 | crc32:u32 LE | payload[length]` where payload is msgpack.
  kinds: `ATOM=1`, `BLOB=2`. CRC32 (zlib) covers kind+payload.
- An `AtomPointer` is `(segment_uuid, offset)`.
- Each process writes only to segments it created; segments are never appended to after the
  process that created them closes them (no shared append; fixes the 0.1 two-writer issue).
- Root record (`space.root`): msgpack `{version, segment, offset, epoch, crc}` written as
  tmp + fsync + rename + fsync(dir). `epoch` increments on every publish (CAS token, §9).
- Commit pipeline: (1) serialize and append all new records; (2) flush + fsync the segment;
  (3) publish the root. Reads of own unflushed data are served from memory until step 2 completes.
- Recovery on open: read and CRC-check the root; validate that the root record exists and passes
  CRC. Torn tails after the last published root are ignored (never reused). A corrupt root raises
  `ProtoCorruptionError` — it never looks like an empty database.
- `durability`: `"fsync"` (default), `"flush"`, `"none"` (D6).
- `proto_db.tools.inspect` dumps segments and roots to JSON for manual inspection.

## 7. Transactions and concurrency (D9, D11)

- `ObjectSpace` → `Database` → `Transaction`. A transaction sees the snapshot taken at start.
- Read set: every `get_root_object(name)` records `(name, pointer-or-ABSENT)`. At commit, under the
  root lock, any change to a recorded root raises `ProtoConflictError` (subclass of
  `ProtoLockingError`). Nothing is published in that case.
- Roots written but not read are merged when the root is `ConcurrentOptimized`
  (Dictionary/RKD/Set/CountedSet/HashDictionary): the transaction's op log is replayed by net
  effect onto the current version. Other types: last writer wins (documented).
- A raised exception after the root is published is forbidden: post-publish steps cannot fail the
  commit.
- `Database.run(fn, retries=…)` runs `fn(tr)` in a fresh transaction and retries on conflict with
  bounded exponential backoff with jitter.
- Root lock = one `threading.Lock` per space (thread exclusion) + an OS file lock held by the
  process for the duration (process exclusion). A timeout raises; it never proceeds unlocked.
- Free-threading: immutable structures need no locks. Shared mutable state (WAL buffers, segment
  writer, caches, lazy-load publication, transaction bookkeeping, space root reference) is guarded
  by locks. Every component documents its thread-safety contract.
- `MutableObject`: a named mutable cell registered in the database's mutable table; reads enter the
  read set, writes are applied at commit, conflicts raise.

## 8. Collections and the index model

### 8.1 IndexedCollection

Every collection derives from `IndexedCollection(Atom)`:

- `indexes -> IndexSet` (immutable mapping name → Index; empty by default)
- `add_index(index: Index) -> Self` builds the index over the current elements and returns a new
  collection version.
- `remove_index(name: str) -> Self`, `get_index(name) -> Index | None`, `index_names() -> list[str]`
- `elements() -> Iterator` — what the collection indexes and what queries return:
  List → items, Dictionary → values, HashDictionary → values, Set/CountedSet → members,
  RepeatedKeysDictionary → records.
- `as_query_plan() -> CollectionScanPlan` exposes `collection.indexes` to the planner — always,
  for every collection.
- Every mutator computes the new structure and the new `IndexSet` in the same step:
  `self._with(content=…, indexes=self.indexes.on_add(e) / on_remove(e) / on_replace(old, new))`.
  There is no code path that returns a collection with stale indexes; a contract test enforces it
  for every mutator of every collection.

### 8.2 Collections

- `List` — persistent positional AVL (existing algorithm kept, rebalance bug fixed): `get_at,
  set_at, insert_at, remove_at, append_first/last, extend, slice, head, tail`.
- `Dictionary` — persistent keyed AVL ordered by `order_key(key)` (D8): `get_at, set_at,
  remove_at, has, items, keys, values`. ConcurrentOptimized (per-key replay).
- `RepeatedKeysDictionary` — multimap key → CountedSet of records: `add(key, record),
  remove_record(key, record), remove_key(key), get(key)`.
- `HashDictionary` — persistent AVL keyed by `hash_key` with collision buckets; arbitrary
  canonical keys (not only ints).
- `Set` — members keyed by `hash_key`; `add, remove, has, union, intersection, difference`.
  Never mutates the receiver.
- `CountedSet` — multiset; `add`, `remove` (decrements), `count(e)`, `total`.

All collections: `count`, iteration, `content_equals(other)` (content comparison; `==` stays oid identity per D10, ruling R12), `__repr__`, and a factory on the
transaction (`tr.new_list()`, … including `new_set`, `new_counted_set`, `new_repeated_keys_dictionary`).

### 8.3 Index framework

`Index(Atom)` is the extension point. Concrete indexes implement these hooks:

| Hook | Purpose |
|------|---------|
| `key_spec: KeySpec` | persistable description of the indexed key: attribute path(s) (`"city"`, `"address.zip"`), or `ELEMENT` (the element itself), or a registered named extractor. Never a lambda. |
| `empty() -> Self` | an empty index with the same definition |
| `with_added(element) -> Self` | |
| `with_removed(element) -> Self` | |
| `with_replaced(old, new) -> Self` | default = removed + added |
| `build(elements) -> Self` | bulk build; default = fold of `with_added` |
| `supports(term) -> bool` | which operators it can answer |
| `estimate(term) -> Estimate(rows, cost)` | for the planner |
| `plan(term) -> IndexPlan` | an executable plan yielding matching elements |
| `explain() -> dict` | description for `explain()` |
| `validate(elements) -> None` | consistency check vs a full scan (used by tests and `inspect`) |

Elements whose key is missing (attribute absent) are recorded under a distinct MISSING marker so
`IsNone`/`NotNone` and scans agree. Indexes are persistent atoms and serialize like any atom.

- `EqualityIndex` — HashDictionary `hash_key(key) → CountedSet(elements)`; supports `==`, `in`,
  `!=` (via complement estimate: scan preferred), `IsNone/NotNone`.
- `RangeIndex` — keyed AVL `order_key(key) → CountedSet(elements)`; supports `==, <, <=, >, >=,
  between*`, ordered iteration (used by `order_by`).
- `VectorIndex` — persistent vector store (id → vector) + backend: `exact` (numpy), `ivf_flat`
  (persisted centroids + lists), `hnsw` (optional; graph rebuilt lazily from the store, cached
  per process); supports `near[]` with `k` and/or threshold; metrics `cosine`, `l2`, `ip`.

`tests/contracts/index_contract.py` is a reusable suite parametrised by index class: any new index
kind that passes it implements every hook correctly (requirement 3).

## 9. Cloud and multi-process (D5)

- `CloudStorage` uploads segment objects first, then writes the root with a conditional write
  (S3 `If-Match`/`If-None-Match`, GCS `ifGenerationMatch`). A failed precondition raises
  `ProtoConflictError` and the transaction retries.
- Shared filesystem (several processes/nodes): OS lock + epoch CAS on `space.root`.
- `ClusterFileStorage`'s UDP voting is removed. Mock clients live in `tests/`, never in the package;
  a missing SDK raises a clear error instead of falling back to a mock.

## 10. Query layer

- Expressions: `Term(path, op, value)`, `And`, `Or`, `Not`, operators as today plus `near[]`.
  Evaluation uses `values.py` semantics.
- Physical plans: `CollectionScan`, `IndexLookup`, `IndexRange`, `VectorSearch`, `Filter`,
  `Intersect`, `Union` (dedup by element identity), `Project`, `Sort`, `Limit`, `GroupBy`, `Join`,
  `Unnest`, `Count`.
- Planner: given a collection plan and a predicate, asks each index `supports`/`estimate`, picks
  the cheapest access path, keeps every unindexed conjunct as a residual filter, and never drops a
  predicate. `explain()` shows exactly the plan that `execute()` runs.
- LINQ is a front end that builds these plans; untranslatable predicates (Python lambdas) run as an
  explicit `PythonFilter` node visible in `explain()`. No exception in the query path is caught to
  return a fallback result.
- Invariant (property-tested): for every collection, every index configuration and every
  predicate, `execute()` returns the same multiset as a full scan.

## 11. Testing strategy

- Tests move to `tests/` (top-level) and run with `pytest`.
- Layers: unit (per module), contract suites (collections, indexes, storages), property-based
  (Hypothesis, including stateful model tests against Python list/dict/set/Counter models),
  integration (end-to-end, file storage), crash (fault-injecting block provider failing at every
  write/fsync/rename point + subprocess SIGKILL), concurrency (stress on 3.14t: no lost updates,
  serializability of counters, index consistency, cache safety), static checks (D16).
- Every test file states what failure it catches. Benchmarks are not tests.
- Runs: `python3.14t -m pytest` (primary), CPython 3.14 with GIL, CI matrix 3.11–3.14 + 3.14t.
- Coverage gate: ≥ 90 % lines on `proto_db/` excluding cloud SDK glue.

## 12. Performance suite

- `benchmarks/` package, run with `python -m benchmarks [--quick] [--json out.json]`.
- Every benchmark prints the work done and verifies its result (row counts, checksums) before
  reporting timing; a mismatch fails the run.
- Workloads: point lookup and range query (indexed vs scan vs Python dict/list vs sqlite3 with an
  equivalent index), bulk insert, commit throughput per durability mode, concurrent commit
  throughput and conflict rate (1..N threads), concurrent read scaling on 3.14t vs 3.14,
  persistent update cost (versions), vector kNN latency/recall by backend, cold open/recovery time,
  cache hit rates.
- Methodology: warm-up, repeated runs, median and p95, environment capture (CPU, Python build,
  GIL state), results as JSON + a Markdown report; `--compare base.json` flags regressions.

## 13. Documentation

Sphinx site under `docs/` rewritten:

- Concepts (values, atoms, collections, transactions, indexes, queries).
- Design: architecture, storage format specification, concurrency and isolation model, index
  framework (with "writing a custom index"), query planner, durability and recovery.
- Tutorials: getting started; modelling data; indexing and querying; concurrency and retries;
  vector search; writing a custom index; operations (durability modes, inspect, backup).
- API reference (autodoc), `docs/decisions.md`, benchmark methodology and latest results.
- README rewritten to state only what is implemented and tested.
- Every tutorial is executable: its code blocks run in the test suite (doctest-style).

## 14. Phases

P1 foundations (errors, values, codec, atoms) → P2 storage v2 → P3 collections → P4 index
framework → P5 transactions/concurrency → P6 query layer + LINQ → P7 vectors → P8 peripheral
(parallel, FSM, cloud) → P9 performance suite → P10 documentation and tutorials → P11 final
verification (full suite on 3.14t and 3.14, review, report). Each phase ends green and committed.
