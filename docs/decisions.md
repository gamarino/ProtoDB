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
- Amended by D21: identity semantics unchanged; the `oid` is assigned lazily, on first need, instead of at construction.

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
- Decision: rank order is MISSING < None < bool < number < str < bytes < date < time-naive < time-aware < datetime-naive < datetime-aware < timedelta < UUID < tuple < atom. `list` and `dict` are storable (`canonical` accepts them recursively) but are not orderable keys (`order_key` raises `ProtoValidationError`). Aware `time` orders by its UTC-normalised time of day; aware `datetime` orders by UTC microseconds computed from the fold-resolved offset (never by raw aware comparison, which is fold-sensitive).
- Alternatives: order lists/dicts; compare naive and aware times as one rank.
- Consequences: keys and index terms are limited to scalars, tuples and atoms; equal instants in different time zones are equal keys.

## D18 - Atom persistence details and DBObject versions
- Context: spec section 5 left open how a new version of an immutable record is identified, how an absent attribute differs from `None`, and how atoms reach storage before transactions exist (ruling R3).
- Decision: `DBObject.with_(**changes)` returns a new atom with a new `oid`: versions are distinct atoms and compare unequal (D10). An absent attribute reads as `None`; an explicit `None` is stored and `has_defined_attr` tells them apart. Atoms hold a `context` (`PersistenceContext`: `storage`, `save(atom)`, `load(ptr, type_name)`); the saved payload is `{"t": type_name, "oid": 16 bytes, "s": encoded_state}`. References encode as `{"@ref": [segment_hex, offset, type_name]}`. Dicts encode as ordered `[key, value]` pairs (keys may be any canonical value); `tzinfo` is limited to `datetime.timezone` and keyed `zoneinfo.ZoneInfo`. A stub learns its `oid` only when loaded, so `oid`, equality and hashing load a stub.
- Alternatives: keep the `oid` across versions (identity of an entity rather than of a value); encode `None` as absence; persist arbitrary `tzinfo` objects by pickling.
- Consequences: references always point at one exact version; entity identity across versions is an application-level key or a `MutableObject` (D11). Persisted attribute names must not shadow class attributes (methods, properties, class-level defaults), which would bypass lazy loading: saving such state raises `ProtoValidationError`, loading it raises `ProtoCorruptionError`, and DBObject rejects such field names at construction. Integers outside the msgpack range are encoded as signed hexadecimal text, which the int/str digit limit does not cover.

## D19 - FileStorage durability, failure and locking details
- Context: spec sections 6-7 fix the format, the commit pipeline and the root lock, but leave open what each durability mode syncs, what happens after an I/O failure, how `publish_root` relates to `root_lock`, and what to do with a leftover `space.root.tmp`.
- Decision: `"fsync"` syncs segment data, the directory after creating a segment, the temporary root file and the directory after the rename; `"flush"` hands segment data to the OS without fsync but keeps the root fsyncs (process-crash safe, not power-loss safe); `"none"` syncs nothing. Any `OSError` on the write path raises `ProtoUnexpectedError` and marks the instance failed: later pushes, flushes and publishes raise `ProtoUnexpectedError` (reads still work), so no later root can reference records whose write was lost; recovery is opening a new instance. After a failure in the root rename or the final directory fsync the outcome of that publish is unknown until reopen. `publish_root` flushes, verifies that the pointer names a readable record (a root that would make the space unopenable is refused with `ProtoCorruptionError`), then does the epoch CAS and the root replace under the root lock, taking it itself unless the calling thread already holds it. The OS lock is `flock`/`msvcrt.locking` on `space.lock`, opened per acquisition. A leftover `space.root.tmp` is ignored, never deleted at open, because another process may be writing it under the lock. `read_root` reads `space.root` from disk on every call so it sees other processes' publishes.
- Alternatives: retry fsync after a failure (unsafe: the kernel may have dropped the dirty pages); keep writing to a new segment after a failure; a separate lock file for the CAS; removing the tmp file at open.
- Consequences: a failed disk write costs reopening the storage instance. The crash matrix (`tests/crash/test_crash_matrix.py`) checks every write/fsync/rename/directory-fsync point under process-crash, ordered power-loss and metadata-reordering power-loss models.

## D20 - Persistent AVL machinery details
- Context: spec section 8.2 and D8 fix the structure (positional and key-ordered persistent AVL trees) but not the node layout, the error behaviour of positional operations, range-bound conventions or how slices are computed.
- Decision: `proto_db/collections/avl.py` has two atom node kinds, `PosNode` (`value, left, right, height, count`; type name `proto_db.avl.PosNode`) and `KeyNode` (`key, value, left, right, height, count`; `proto_db.avl.KeyNode`); the empty tree is `None`. Fields are set only by constructors (`__setattr__` raises `ProtoUserError`); a `KeyNode` caches `order_key(key)` in a runtime-only attribute. All operations are pure module functions with path copying. Positional indexes must be `int` (not `bool`) within `0..count-1` (`0..count` for insert, `0 <= start <= stop <= count` for slices); anything else raises `ProtoValidationError` - negative-index translation belongs to `List`. `pos_slice` and `pos_concat` use split/join (Blelloch et al., "Just Join") and run in O(log n). `key_put` returns `(root, replaced, old_value)` and keeps the existing key object on replace, as `dict` does; `key_remove` returns `(root, removed, old_value)` and the same root when the key is absent. In `key_iter_range(root, lo=UNBOUNDED, hi=UNBOUNDED, lo_inc=True, hi_inc=True)` only the exported `UNBOUNDED` sentinel leaves a side open; `None` and `MISSING` are ordinary bounds compared by `order_key` (so `lo=None, lo_inc=False` excludes `MISSING` and `None` keys, which `RangeIndex` needs). `key_from_sorted` requires strictly increasing order keys. Traversals and `check_invariants` use explicit stacks; recursive helpers recurse only along one root-to-leaf path. `check_invariants` reports a root height outside 1..128, or any path longer than the root height (cycles), as `ProtoCorruptionError`; a subtree shared by two parents is checked once per path.
- Alternatives: a single node class with a mode flag; an O(k) slice by iteration; `None` as the unbounded marker (rejected: it made ranges bounded at `None` inexpressible and silently wrong); negative indexes in the machinery.
- Consequences: every key value, `None` and `MISSING` included, is usable as a range bound; values are stored unvalidated, so collections call `values.canonical` before inserting; equal keys of different types (`1`, `1.0`) are one key.

## D21 - Lazy atom identity and striped atom locks (amends D10)
- Context: building an atom cost about 4.3 us on 3.14t, of which `uuid.uuid4()` was about 2.1 us and the per-atom `RLock` about 0.2 us. Every persistent collection and index path-copies O(log n) atoms per mutation, and most of those nodes are never saved nor compared, so the cost multiplied everywhere.
- Decision: identity semantics of D10 are unchanged (a UUID4 `oid`, persisted, used by `==` and `hash`; versions are distinct atoms); only the assignment time changes. A new atom has no `oid` until first need - the first `oid` read, `==`/`hash`, or `_save` - when the UUID4 is generated under the atom's lock; once observed it never changes. A stub takes its `oid` from the payload on load (reading `oid` on a stub still loads it). The per-atom `RLock` is replaced by a module-level pool of 64 `threading.RLock`s; `_lock_for(atom)` picks one from `id(atom)` with the low four bits dropped and two bit ranges folded (plain `id % 64` hit only two stripes because object addresses are aligned). Lock order rule: a thread holding an atom stripe never acquires another atom stripe; re-acquiring its own stripe is allowed. To make that hold, `_save` encodes its state (which saves the children) with no stripe held and takes its stripe only to publish its own payload (check pointer, assign `oid`, push, set pointer); `_load` fetches the payload once under the stripe and keeps it on the stub, decodes it with no stripe held (decoding may hash, and therefore load, child stubs), and publishes the first decoded state under the stripe with the loaded flag set last. Storage locks are leaves: they are taken under a stripe but never call back into atoms. `_state` iterates a copy of the instance dictionary because another thread may add the lazily assigned `oid` concurrently.
- Alternatives: keep eager UUIDs (rejected: half the construction cost for identities most nodes never need); a cheaper identifier such as a process counter (rejected: changes the persisted format and global uniqueness); striped locks held across children saves and payload decoding, relying on re-entrancy (rejected: two unrelated atoms sharing a stripe give an A-B/B-A deadlock between a parent and a child on crossed stripes); per-atom locks created lazily for stubs and on first save (unnecessary once no stripe is nested).
- Consequences: a `PosNode` costs about 1.5 us to build (was 4.2 us) and a 10^4-element `pos_insert` sequence runs in about 0.29 s (was 0.69 s). The UUID cost moves to the first save or comparison: saving a freshly built 10^4-node tree is about 35 % slower (135 ms vs 101 ms), about break-even with the construction saving when every node built is saved, and loading costs about 10 % more (two stripe acquisitions per stub). Concurrent first saves of one atom may encode its state more than once; only one payload is pushed. A stub fetch holds its stripe during the storage read, so a slow read can briefly delay unrelated atoms on the same stripe. `values.is_atom` reads `oid` and therefore assigns it.

## D22 - IndexedCollection, IndexSet and net-effect replay details
- Context: spec section 8.1 and rulings R5/R6 fix the API (`IndexedCollection`, `IndexSet`, `as_query_plan`, `rebase_onto`) but not how new versions are built, what an op log contains, how index-definition changes merge, or what a no-op mutator must return.
- Decision: `IndexSet` is an immutable atom over a keyed AVL tree (`name -> index`); its element hooks map every index through `with_added`/`with_removed`/`with_replaced` and reject a hook result whose `name` differs; an empty set returns itself. Collections and index sets refuse public attribute assignment after construction. A collection declares its persisted fields in `_fields` (must include `count` and `indexes`) and runtime fields to carry over in `_runtime_fields` (gathered across the MRO); `_with(**changes)` builds a new instance from them and rejects unknown names. `add_index` checks the name before building and calls `index.build(elements())`. `ConcurrentOptimized` keeps a runtime-only `_op_log` (never persisted; a loaded version starts empty); no-op mutations do not log, so the replay is the net effect relative to the version's snapshot (discarding an absent member does not undo a concurrent add). `rebase_onto(current)` replays the last op per key (`order_key(op[1])` by default) in order of last occurrence through the subclass's `_replay_op`, then index-definition changes per name: created here -> added (`ProtoConflictError` if `current` already has that name), removed here -> removed if present, removed-then-added -> replaces `current`'s, created-then-removed -> nothing; added indexes are rebuilt over the merged elements. `ConcurrentOptimized` must precede `IndexedCollection` in the bases (checked at class creation). Query plans are runtime objects, not atoms; `Estimate(rows, cost)` is a frozen dataclass of finite non-negative floats in `query/plans.py`. In the collection contract a mutator whose expected events are empty may return the receiver or an equal new version; every other mutator must return a new object.
- Alternatives: replay index ops in log order (rejected: an index created and dropped within a transaction would conflict with, or delete, a concurrently created one); log no-op mutations as intents (rejected: makes replay depend on operations that had no effect on the transaction's own version); a shared empty `IndexSet` singleton (rejected: atoms bind to the first context that saves them).
- Consequences: every collection version saves its own (possibly empty) IndexSet atom once; mutators that do not change content cost no index work. Rebasing re-runs `build` for indexes added by the transaction.
