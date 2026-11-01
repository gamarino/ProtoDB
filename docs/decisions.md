# ProtoDB 2.0 decision log

ADR-style entries. Each records context, decision, alternatives and consequences.
Source: `docs/superpowers/specs/2026-11-01-protodb-v2-design.md` section 2.

## D1 - Full repair of the internals
- Context: the 0.1 internals have structural defects (identity, durability, ordering, conflicts).
- Decision: rebuild the internals module by module under the existing concepts.
- Alternatives: patch incrementally; rewrite with a new API.
- Consequences: public concepts survive; 0.1 modules are removed at once in Task 8.5.

## D2 - One value model with a total order
- Context: index scans and collection scans compared values differently.
- Decision: a single total order across types (spec section 4), shared by indexes and scans.
- Alternatives: per-index comparators; Python's native ordering.
- Consequences: heterogeneous values are comparable; the comparator is a core module.

## D3 - Canonical types
- Context: serialization needs a closed set of values.
- Decision: `None, bool, int, float, Decimal, str, bytes, date, time, datetime (naive or aware), timedelta, UUID, tuple, list, dict`, plus atoms.
- Alternatives: pickle-like arbitrary objects.
- Consequences: safe, portable encoding; other types must be converted by the caller.

## D4 - On-disk format v2
- Context: the 0.1 format has no integrity checks.
- Decision: framed binary records, msgpack payloads, CRC32, per-segment header. A `protodb inspect` CLI replaces human-readable files. Parquet is an export format only.
- Alternatives: keep JSON-like files; Parquet as the store.
- Consequences: corruption is detectable; inspection needs tooling.

## D5 - Cloud and cluster coordination
- Context: UDP voting is unreliable.
- Decision: CAS on the root via conditional writes; data written strictly before the root; UDP voting removed.
- Alternatives: keep voting; external lock service.
- Consequences: relies on providers offering conditional writes.

## D6 - Durability
- Context: commits were not durable.
- Decision: fsync per commit by default; `durability="flush"` and `"none"` are explicit opt-outs for tests and benchmarks.
- Alternatives: no fsync; group commit now.
- Consequences: safe by default; group commit is a later optimisation.

## D7 - Strings stored inline
- Context: the global `Literal` catalog added indirection and contention.
- Decision: strings are stored inline; the catalog is removed; reading a string returns `str`.
- Alternatives: keep the catalog with caching.
- Consequences: simpler reads; repeated strings are not deduplicated.

## D8 - Dictionary as key-ordered persistent AVL tree
- Context: binary search over a positional list gave O(log^2 n).
- Decision: `Dictionary` is a key-ordered persistent AVL tree, O(log n).
- Alternatives: HAMT; keep the positional list.
- Consequences: ordered iteration and range queries; structural sharing on update.

## D9 - Conflict implies retry
- Context: lost updates under concurrency.
- Decision: conflict raises and the caller retries. Read sets include absent roots. `ConcurrentOptimized` merge (per-key/per-element op-log replay) applies only to roots the transaction did not read.
- Alternatives: last-writer-wins; always merge.
- Consequences: serializable behaviour; merge is a controlled optimisation.

## D10 - Atom identity
- Context: equality used `id()` or storage pointers, which change.
- Decision: every atom gets a stable `oid` (UUID4) at creation, persisted; equality and hashing use `oid`.
- Alternatives: pointer identity; content hashing.
- Consequences: stable identity across saves; 16 extra bytes per atom.

## D11 - MutableObject semantics
- Context: `MutableObject` had undefined concurrency behaviour.
- Decision: keep it as a named mutable cell with read-set conflict detection.
- Alternatives: remove it.
- Consequences: a supported escape hatch for mutable state with defined conflicts.

## D12 - Single exception hierarchy
- Context: several overlapping exception families.
- Decision: one hierarchy rooted at `ProtoDBError`; `ProtoBaseException` remains an alias for one release.
- Alternatives: keep the families.
- Consequences: callers catch one root; old names keep working via aliases in `proto_db.errors`.

## D13 - Optional hnswlib
- Context: `hnswlib` re-enables the GIL on 3.14t.
- Decision: import it lazily and optionally. Default vector backend is exact numpy search; IVF-Flat is persistent and built in.
- Alternatives: hard dependency.
- Consequences: free-threading preserved by default; ANN is opt-in.

## D14 - Python floor and dependencies
- Context: wide compatibility wanted; some dependencies unused.
- Decision: floor stays 3.11; CI matrix 3.11-3.14 plus 3.14t; drop `six` and `wrapt`.
- Alternatives: raise the floor.
- Consequences: no 3.12+ only syntax in the code base.

## D15 - Subpackage layout
- Context: a flat package hid structure.
- Decision: split into subpackages (spec section 3); top-level `proto_db` re-exports the public API.
- Alternatives: stay flat.
- Consequences: clearer ownership; import paths change for internals.

## D16 - Static checker for hygiene
- Context: swallowed exceptions hid failures; test-aware code skewed behaviour.
- Decision: a static checker test forbids swallowed exceptions and test-aware code in `proto_db/`.
- Alternatives: code review only; linters.
- Consequences: `tests/static/` enforces it; scope grows via `CHECKED_ROOTS` until Phase 11 covers the whole package.

## D17 - Value model details
- Context: spec section 4 left list/dict and naive-vs-aware `time` open.
- Decision: rank order is MISSING < None < bool < number < str < bytes < date < time-naive < time-aware < datetime-naive < datetime-aware < timedelta < UUID < tuple < atom. `list` and `dict` are storable (`canonical` accepts them recursively) but are not orderable keys (`order_key` raises `ProtoValidationError`). Aware `time` orders by its UTC-normalised time of day.
- Alternatives: order lists/dicts; compare naive and aware times as one rank.
- Consequences: keys and index terms are limited to scalars, tuples and atoms; equal instants in different time zones are equal keys.
