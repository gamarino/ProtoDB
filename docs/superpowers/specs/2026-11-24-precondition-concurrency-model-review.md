# Review of the precondition concurrency model specification (pre-implementation)

Reviewed on 2026-11-24 against origin/master 56ed529 (proto_db) and c6798c8
(protoBaseST), before any implementation. It applies to
`2026-11-24-precondition-concurrency-model-design.md`. Each item gives the
finding, the evidence, and the proposed resolution. Items marked **author**
need the maintainer's decision; items marked **proposed** are resolutions the
implementer will apply unless the maintainer objects.

## Blocking

| # | Rule | Finding | Proposed resolution |
| --- | --- | --- | --- |
| B1 | R50, R52, R59 | Format versions are linear and sticky (`next_root_version` takes the max; `READABLE_FORMAT_VERSIONS={2..6}`; decoders ignore unknown root keys). No mark exists that stops old code from writing while letting it read: a new version 7 also stops old code from reading, and the upgrade is one-way. protoBaseST reads only versions 2-4 and writes 4 hard-coded (`framing.st:85,261,298`); requiring version 7 there means implementing formats 5-6 (parents, registry, catalog types 22-34) and log format 3. | **author** |
| B2 | Section 8, R31, R32 | The candidate cannot be a finished root: in cluster mode the SpaceRoot carries `cluster_term/epoch/previous` from the permission's stamp, and `_build_root` has side effects that must stay under the lock (registry `committing()`, `_committed_version`). R31's "no iteration outside the lock" also meets the existing `_IMPORT_ROUNDS` loop. | **proposed:** the candidate is the reapplied objects and mutables maps only; DatabaseRoot and SpaceRoot are always built under the lock; the module-import loop stays as is. |
| B3 | P-B, R18, section 6 | `count`, `total_count`, `root` and `indexes` are plain attributes, and the engine reads them itself (mutators, `List._position`, `Set.add` via `_find`, the planner's scan estimate at `query/plans.py:241`). Recording every read would make every query take the whole collection. | **proposed:** only reads made by user code through a linked instance's public API are recorded; engine-internal reads are not. On linked instances the attributes become recording properties. |
| B4 | R28, section 6 | Whether a lookup uses an equality index is the planner's choice; RangeIndex also answers `==` with the same group structure; `in`, `!=`, MISSING and None are unspecified; `IndexPlan` holds only the index. | **proposed:** a plan answered by a KeyGroupIndex group (EqualityIndex or RangeIndex) for `==` or `in` records one group precondition per key, registered when the plan is built (so a cached result records the same); every other plan records the whole collection. |
| B5 | P-B, R22 | `get_index` returns a raw index, and `ordered_index_scan(index)` and `VectorSearch(index, ...)` are public, so reads can bypass the linked collection. | **proposed:** `get_index` on a linked collection records the whole collection. |
| B6 | R29 | No uniqueness exists today. Undefined cases: elements lacking the field (MISSING) or holding None; `1` and `1.0` share a group; the `side` group holds all unorderable keys; in a Dictionary the indexed element is the value, so two keys holding the same value are one member with count 2; a CountedSet indexes distinct elements only. | **author** (proposal: MISSING and None never violate uniqueness; `1` and `1.0` are duplicates, consistent with `order_key`; unorderable keys are not allowed in a unique index (ProtoValidationError); in a Dictionary "the element being replaced" is the entry at the same key) |
| B7 | R30, R34, R39 | ivf_flat: the inverted list depends on the current centroids (it cannot travel as data), and an add can trigger `_retrained()`, a full k-means under the lock, with numpy and pure-Python paths that may produce different centroids (a P-C risk). exact and hnsw can be updated from a precomputed vector. | **proposed:** apply R30's fallback to ivf_flat: writes to a collection with an ivf_flat index take the whole collection (no reapplication). |
| B8 | R24 | A stale format-3 index must be rebuilt (with extractors) before its first change; under the lock that is impossible. | **proposed:** a write to a collection whose base has a stale index rebuilds it in preparation and takes the whole collection. |

## Needs a decision (smaller)

| # | Rule | Finding | Proposed resolution |
| --- | --- | --- | --- |
| N1 | Assumed decisions | A2-A5 are still "to be confirmed by the author". | **author** |
| N2 | R21, R37 | Reads outside the read set today and not listed: `Transaction.installed_modules`, `catalog_object`, the session's catalog read from `source._objects`, `Transaction.load(ptr)`. R37 omits `__catalog`, which `_merged_registry` also merges; `_merged_semantic_catalog` rebuilds entities from the registry under the lock. | **proposed:** reserved roots (`__*`) keep their own merge under R37 (`__catalog` included) and their reads are outside the model; `Transaction.load(ptr)` joins R21's list; the semantic-catalog merge is checked for user code and reported if it runs any. |
| N3 | R23 | A transaction that only installs a module or updates the catalog publishes today. | **proposed:** it counts as having intentions and publishes. |
| N4 | R27 | Extractors also run at mutation time and in queries (`Term.matches`, sorting by `@name`). | **proposed:** the rejection applies wherever an extractor runs, detected by a context variable set around `extract` and checked at every Transaction read entry point and in `MutableObject.get`. |
| N5 | R1 | "Instance of" includes application subclasses that override mutators or `elements`. | **proposed:** a subclass keeps fine-grained treatment only if it overrides none of the listed operations; otherwise it is an object root (P-A). |

## Corrections and implementation notes (no decision needed)

- `Dictionary.get(k)` and `HashDictionary.get(k)` do not exist (only `RepeatedKeysDictionary.get`); the row keeps `get_at` and `has`.
- `count` is an attribute, `len` is `__len__`.
- R49: the hooks are `IndexSet.on_add/on_remove/on_replace` (`collections/base.py:228-238`); IndexedCollection uses `_after_add/_after_remove/_after_replace`. No `with_*` accepts a precomputed key; variants are added.
- Two more places run extractors under the lock today: replaying `add_index` and `rebuilt_stale`; both go to preparation.
- R41: `_with` sets `atom_pointer=None`; a pointer-preserving copy of a loaded cached atom, kept out of `_AtomCache._ident`, is added.
- Versions derived by `slice`, `head`, `tail` or `intersection` and then assigned take the whole collection (P-B).
- List: a `set_at` on a position the transaction itself appended folds into its append intention (R9); a negative index is normalized against the transaction's own view and records the whole list.
- Dictionary value boxes (D51): per-key preconditions compare the stored form (the box pointer), so nothing is loaded under the lock.
- R28: a group is the tuple `(members tree pointer, total)`; comparing it by token gives spurious conflicts after path copying with equal content (acceptable under P-A, not ABA by value).
- R25: index definitions compare by their persisted definition fields; an extractor is persisted by name only (documented limit).
- Pre-pass validation against R0, and in a cluster against `coordinator.current_root()` (which may lag), can fail where R1 would pass: acceptable under P-A, documented.
- `Database.run` also has an `intents` parameter (R53).
- R36: errors carry the failed unit as attributes that survive `copy.copy`.
- R55: dropping a leftover `__intentLog` is a reserved-root write (R37).
- IntentRecord (type id 34) is written only by the replay; `Intent` (type id 31) does not depend on it. Conformance vectors with intent-log cases are kept (their digests are pinned).
- R60: protoBaseST vendors Python's vectors (`fetch_vectors.sh`); new scenarios are written in Python and the digest in `format-spec.rst` is re-pinned.
- R61: `tests/interop/` exists only in protoBaseST; mixed clusters are a non-goal, so cross-language cases are standalone.
- Section 12: group commit is not a mode (it happens when commits queue); a fixture running one test across standalone, forced group commit and cluster has to be built. The CI matrix already has 3.12-3.14 and 3.14t.
- protoBaseST: `keygroups.st` and `vectors.st` are also affected; nested atoms are saved under the lock today; the build runs on the round-runner actor, so the pre-pass runs in the caller.
