# Read-path audit of the upper layers (R21, R22, R37, N2)

Audit of every point where the query module, the query cache, the paginators, the LINQ front end,
the semantic layer and the transaction's own auxiliary readers read a collection, an index or the
database root. It applies to `2026-11-24-precondition-concurrency-model-design.md` (R21, R22, R37,
section 14) and to the review `2026-11-24-precondition-concurrency-model-review.md` (B3, B4, B5,
N2). Line numbers are those of the commit that adds this document.

## How a read is recorded

A transaction hands out a **linked** copy of a fine-grained collection root
(`Transaction.get_root_object`, `proto_db/transactions.py:2919`). On a linked copy:

- a listed operation records its own unit (`record_key`, `record_range`,
  `proto_db/collections/linking.py`);
- any other read by non-engine code of a persisted field (`root`, `count`, `indexes`, ...) goes
  through `IndexedCollection.__getattr__` (`proto_db/collections/base.py:456`) and records
  `Unit(WHOLE, root)`. That is the fallback that makes a direct tree access by an upper layer safe
  (P-A: coarser is safe) instead of silent;
- reads inside `engine_reads()` record nothing; the engine uses it only where it records the read
  itself, before or after.

So an upper-layer site is one of:

| Resolution | Meaning |
| --- | --- |
| **records X** | The site records unit or range X through the model. |
| **WHOLE** | The site records `Unit(WHOLE, root)` (explicitly, or by the `__getattr__` fallback). |
| **engine** | An engine read that reveals nothing to the caller (cost estimate, plan shape); the result's reads are recorded elsewhere, named in the row. |
| **R20** | A read of a value reached from a value already recorded (an attribute, a collection held inside a record): covered by the unit that produced the holder. |
| **R21 outside** | Outside the transaction's read set and its guarantee, documented as such. |
| **R37 reserved** | A reserved root (`__*`) read outside the model; the root keeps its own merge. |

## Sites

### Query planner (`proto_db/query/planner.py`)

| Site | Reads | Resolution | Covering test |
| --- | --- | --- | --- |
| `plan_query` `:404`, planning block `:424` | indexes, counts, estimates of the source | engine; then `_record_reads` `:378` records the plan's reads | `test_read_paths.py::test_query_read_then_concurrent_change_conflicts` |
| `_record_reads` `:378` | the finished plan | records `INDEX_DEFS`; an `==`/`in` key-group plan `INDEX_GROUP` per key (B4); a RangeIndex range scan a `KeyRange` (R68); a positional window over a Dictionary scan the range from its first key (R68); any other plan WHOLE | `test_query_read_then_concurrent_change_conflicts`; `test_precondition_indexes.py::test_equality_lookup_conflicts_only_same_group`, `::test_unindexed_lookup_records_whole`; `test_precondition_ranges.py::test_range_query_conflicts_with_change_inside_range`, `::test_planner_window_records_range_from_the_start` |
| `_source` `:299` (`get_index`, `index_names`) | index definitions and index objects | engine (inside `:424`); recorded as `INDEX_DEFS` by `_record_reads` | `test_precondition_indexes.py::test_concurrent_add_index_conflicts_with_write` |
| `_Candidate.__post_init__` `:178` (cost of every candidate) | `CollectionScanPlan.estimate`, index estimates | engine (ruling 4: estimation never records) | `test_read_paths.py::test_plan_estimate_records_nothing` |
| `_fine_reads` `:348`, `collection.indexes.get` `:357` | the plan's indexes | engine (inside `_record_reads`) | as `_record_reads` |
| `plan_query` source given as a `CollectionScanPlan` (`:433`) | the scanned collection | WHOLE | `test_precondition_ranges.py::test_unbounded_scan_still_whole` |

### Plan nodes (`proto_db/query/plans.py`)

| Site | Reads | Resolution | Covering test |
| --- | --- | --- | --- |
| `CollectionScanPlan.execute` `:229` (`collection.elements()`) | every element | WHOLE (`root` through `__getattr__`) | `test_read_paths.py::test_cached_query_result_registers_same_reads[hand_built]` |
| `CollectionScanPlan.explain` `:233` (`c.count`, `c.index_names()`) | the row count, the index names | WHOLE and `INDEX_DEFS`: `explain()` shows the count, so it is a read | `test_read_paths.py::test_plan_estimate_records_nothing` (its last assertion) |
| `CollectionScanPlan.estimate` `:239` (`count`) | the row count | **engine** (changed by this task: was WHOLE outside `plan_query`, ruling 4) | `test_read_paths.py::test_plan_estimate_records_nothing` |
| `CollectionScanPlan.window` `:247` (`_scan_window`) | a positional window | Dictionary: the range from its first key to the window's last one, WHOLE at the end (`dictionaries.py:130,144`); List: WHOLE (`lists.py:97` reads `count`); other classes: no rank access | `test_precondition_ranges.py::test_planner_window_records_range_from_the_start`; `test_read_paths.py::test_cached_query_result_registers_same_reads[window]` |
| `CollectionScanPlan.exact_rows` `:252` (`count`) | the row count | WHOLE | `test_read_paths.py::test_pager_read_then_concurrent_change_conflicts`, `::test_cached_query_result_registers_same_reads[counted_window]` |
| `IndexPlan` `:287`, `ordered_index_scan` `:375`, `VectorSearch` `:392` | an index's structure (not a linked object) | recorded when the index was obtained: by the planner (`_record_reads`), by `get_index` (WHOLE and `INDEX_DEFS`, B5, `base.py:556`), or by reading `collection.indexes` (WHOLE, `__getattr__`) | `test_precondition_indexes.py::test_get_index_records_whole` |
| `Unnest.execute` (`value.elements()`), `terms.py:324` (`contains` over a collection value) | a collection held in an element | R20 (a linked collection given as a value records WHOLE through its own `__getattr__`) | (R20, no new path) |

### Query cache (`proto_db/query/cache.py`)

| Site | Reads | Resolution | Covering test |
| --- | --- | --- | --- |
| `as_plan` `:97` (a collection source: `as_query_plan`, `base.py:679`) | the collection | WHOLE | `test_linked_collections.py::test_query_on_a_linked_collection_records_whole` |
| `QueryCache.result` `:564`, hit returned at `:582` | nothing (a stored result) | **records the plan's reads** (changed by this task, `record_served_reads` `:305`, called at `:577` on hit and miss alike); reads that cover the miss's execution | `test_read_paths.py::test_cached_query_result_registers_same_reads` (4 shapes), `::test_cached_composite_plans_register_reads` (Join, Union, nested Limit), `::test_cached_query_hit_outside_the_read_commits` |
| `record_served_reads` `:305`, an unknown node type | the scans reachable through the node's attributes | fail-closed: WHOLE for each reachable scan, `ProtoUnexpectedError` when none is reachable; `SERVED_TYPES` lists the types handled by name | `::test_unknown_plan_node_records_whole_or_raises`, `::test_record_served_reads_handles_every_cacheable_node` (parity with `_plan_key`) |
| `_plan_key` / `_ptr` (collection and index pointers) | `atom_pointer` (runtime, names the version, not content) | not a content read; the content is recorded by `record_served_reads` | as above |
| `QueryCache.explain` `:606` | `plan.explain()` | as `CollectionScanPlan.explain` | as above |

Before this task a hit returned the stored result without recording anything. A plan built by
`plan_query` had recorded its reads when it was planned, so a hit through a `Queryable` or a
planner plan was already covered; a plan built by hand (`Filter(CollectionScanPlan(d), ...)`) and a
`Count` over a window recorded only on a miss, through execution. The fix derives the reads from
the plan being answered, not from the stored result: a `QueryResult` is shared by every
transaction of the space, and a precondition names one transaction's root and expects its base,
so the result carries no units (a deliberate deviation from the brief's wording "the plan's
recorded units are stored in QueryResult", with the same observable contract: a hit registers the
preconditions the miss registered through `record_served_reads`, and those cover what the miss's
execution recorded; for a `Limit` over a planned window the execution reads a narrower range,
inside the one recorded).

### Paginators (`proto_db/query/pager.py`, `Transaction.pager`, `Queryable.pager`)

| Site | Reads | Resolution | Covering test |
| --- | --- | --- | --- |
| `Pager.__init__` `:152` (`exact_rows`), `:153` (`window(0, 0)` probe) | the row count | WHOLE for a scan; for an index plan, the units its planning recorded (the row count of a group or range is inside it) | `test_read_paths.py::test_pager_read_then_concurrent_change_conflicts` |
| `Pager.page` `:194` (`plan.window`) | the page | as `CollectionScanPlan.window` / index plans | same |
| `Pager._materialized` `:175` (through the cache, or `plan.execute()`) | the whole result | as `QueryCache.result` / plan execution | `test_pager_read_then_concurrent_change_conflicts` (second half: a hit through the cache) |
| `Queryable.pager` `linq.py:1062` | `Queryable.plan()` → `plan_query` | records what `plan_query` records (a group lookup: `INDEX_GROUP`) | `test_read_paths.py::test_linq_pager_records_reads` |
| `Transaction.pager` `transactions.py:2956` | the root's collection, through a snapshot pager | WHOLE, explicitly (`:2970`): the pager pages the committed version and reveals its row count; it takes no window, so no range applies | `test_read_paths.py::test_transaction_pager_records_reads` |
| `portable_pager` `pager.py:230`, `Snapshot.pager` `transactions.py:643` | a snapshot's unlinked collection | R21 outside (a snapshot) | (R21) |
| `Database.resume_pager` `transactions.py:2648`, `check_resumed` `pager.py:253` | a snapshot | R21 outside | (R21) |

### LINQ (`proto_db/query/linq.py`)

| Site | Reads | Resolution | Covering test |
| --- | --- | --- | --- |
| `Queryable.plan` `:991` (`plan_query` at `:1011`, `:1015`) | the source collection | records what `plan_query` records; Python filters, sorts and windows above it run over its rows | `test_read_paths.py::test_linq_pager_records_reads`, `::test_cached_query_result_registers_same_reads[planned, window]`; `test_precondition_ranges.py::test_planner_window_records_range_from_the_start` |
| `__iter__` `:1045`, `to_list`, `first`, `first_or_none` `:1078-1085` | plan execution | as the plan nodes | as above |
| `count` `:1069` (`Count` → `exact_rows`) | the row count | WHOLE for a scan source; the planned units for an index source | `test_cached_query_result_registers_same_reads[counted_window]` |
| `cached` `:1052` | `QueryCache.result` | as the cache | as above |
| `_source` `:900` (`ValuesScan` of Python values) | values already in memory | nothing to record (their reads happened where they were obtained) | (no path) |

### Semantic layer (`proto_db/semantic/`)

| Site | Reads | Resolution | Covering test |
| --- | --- | --- | --- |
| `_SessionReader.root` `entities.py:649`, `StoredReader.root` `:713`, `Reader.members` `expressions.py:118`, `_members_of` `expressions.py:509`, `_traverse` `entities.py:1420` | a root (through `get_root_object`) and its elements | `ROOT_PRESENCE` and WHOLE (a linked List/Set iterated) | `test_read_paths.py::test_semantic_root_query_records_the_whole_root` |
| `StoredReader.cell`, `_SessionReader` cells (`get_mutable`), `Reader.follow` | a cell | records `CELL` (R3) | `test_read_paths.py::test_semantic_query_read_then_concurrent_change_conflicts` |
| `Members.__len__/__iter__/__contains__` `entities.py:609,615,636`; `_find` `:932,940`; `update` re-keying `:1114`; `_indexed` `:1516,1542,1553`; `_holds_records_only` `:1711`; `constraints.py:125` | a collection held inside a record | R20 (covered by the `CELL` of the record that holds it) | `test_semantic_query_read_then_concurrent_change_conflicts` (the members of the customer's `orders`) |
| `_bound` `entities.py:798` (`snapshot_mutable`) | the committed record of a cell | R21 outside, but the same cell was read with `get_mutable` first (its docstring), so the commit validates it | `test_transactions_snapshot_mutable.py::test_snapshot_mutable_is_not_recorded_in_the_read_set` |
| `SemanticSession._read` `session.py:109` (`source._objects`, the catalog) | `__semanticCatalog`, the module list | R37 reserved (read from the version, never through the read set) | `test_read_paths.py::test_semantic_describe_from_snapshot_records_nothing` (its `semantic(a).describe` part) |
| `SemanticSession._read` `session.py:105` (a Snapshot's catalog), `query_stored` `entities.py:1699` | a snapshot | R21 outside | `test_semantic_describe_from_snapshot_records_nothing` |

### Transaction auxiliary readers (`proto_db/transactions.py`) and others

| Site | Reads | Resolution | Covering test |
| --- | --- | --- | --- |
| `installed_modules` `:3474` | `__currentModules` | R37 reserved (N2) | `test_registry_roots.py::test_reading_modules_does_not_conflict_with_ordinary_writes` |
| `catalog_object` `:3571` | `__catalog` | R37 reserved (N2; R37 includes `__catalog`) | `test_registry_roots.py::test_a_handed_out_catalog_object_never_silently_loses` |
| `get_root_object` of a `__*` root `:2919` | a registry root | records `Unit(ROOT, name)` (R241): an ordinary read of a reserved root | `test_precondition_roots.py::test_reserved_roots_keep_their_merge` |
| `_merged_registry` / `_merged_semantic_catalog` `:3678` | the current registry and catalog under the root lock | R37 reserved (their own merge); composes with engine code only (N2, closed below) | `test_precondition_roots.py::test_reserved_roots_keep_their_merge`, `test_read_paths.py::test_semantic_catalog_merge_runs_no_user_code` |
| `Transaction.load(ptr)` `:2868` | an atom by pointer | R21 outside (N2; now stated in its docstring) | (R21) |
| `snapshot_mutable` `:3144` | a cell of the base | R21 outside (documented) | `test_transactions_snapshot_mutable.py::test_snapshot_mutable_is_not_recorded_in_the_read_set` |
| `snapshot_ref` `:2952` → `Database.snapshot_at` `:2605`, `Snapshot` `:513` | a version | R21 outside (now stated in the `Snapshot` docstring) | `test_read_paths.py::test_semantic_describe_from_snapshot_records_nothing` |
| `Database.history` `:2568` | committed versions | R21 outside (now stated in its docstring) | (R21) |
| objects brought from another transaction (a linked copy whose transaction ended, `link_of` → `None`) | anything | R21 outside | `test_linked_collections.py::test_link_invalid_after_commit_and_abort` |
| `proto_db/agent/operations.py:355,368` | a snapshot (`snapshot_at`) | R21 outside | (R21) |
| `proto_db/tools/notation.py:187,191`, `tools/verify.py:248` | roots through `get_root_object` | recorded as any root read (tools, not upper layers) | (no new path) |

## N2: user code in the semantic-catalog merge (closed)

`_merged_semantic_catalog` calls `proto_db.semantic.store.merged_semantic_catalog`, which runs
`merged_model` → `module_contributions` → `_extension_item` (`proto_db/semantic/compose.py:246`)
and `compose_modules`, under the space root. It imports nothing and calls no application function,
but before this fix it read the declarations by dispatch: `value.element(name)` on descriptors,
`spec.element(name)` on intents, `constraint.reads()`/`constraint.element(scope)` on invariants,
`value.maintained` in `_maintained`, and `copy.deepcopy` of the dunder values. A module that
subclassed a descriptor, `IntentSpec` or `Invariant` and overrode those methods ran its code under
the root (measured: the overrides were called with `Transaction._holding_root_lock()` true).

**Closed.** The composition never asks a declaration through dispatch:

- the engine class is found by identity in the type's MRO (read through `type.__dict__["__mro__"]`,
  so no metaclass can intercept it); `_maintained` uses the same test for `Aggregate`;
- the state is read with C-level accessors of the engine class (the `__dict__` getset of
  `_Descriptor`, the slot descriptors of `IntentSpec`/`Invariant`; an intent's spec from the
  function's own `__dict__`), checked to be plain data (exact builtin types and the engine's
  `FrozenDict`/`FrozenList`, recursively; anything else is `ProtoValidationError`), copied into an
  instance of the engine class itself, and the engine class's method is called on that copy
  (`Field.element(shadow, name)`, `IntentSpec.element`, `Invariant.reads`/`element`);
- dunder values are copied by the engine's `_plain_copy` (lists and dicts rebuilt, immutables
  shared), never by `copy.deepcopy`, so no `__deepcopy__` runs. A copy is kept rather than dropped:
  the language maps are plain dicts the catalog model may later fold into. The remaining
  `copy.deepcopy` calls in `compose_modules` copy the items `_extension_item` produced (now checked
  plain data) or a stored catalog decoded by the engine: no class of a module reaches them.

A subclass therefore composes exactly as its engine class (its overrides are ignored by the
catalog, consistently in the code's composition and in the merge). Tests:
`test_read_paths.py::test_semantic_catalog_merge_runs_no_user_code` (module
`tests/unit/semantic_modules/acme_overrides.py`: no override is called, under the root or
elsewhere, while the merge composes the module under the root; the stored items equal the engine
classes' items) and `::test_extension_item_never_deep_copies_a_module_value`.

## Summary of changes made by this audit

1. `CollectionScanPlan.estimate` reads the row count as an engine read (ruling 4).
2. `QueryCache.result` records the reads of every cacheable plan it answers, hit or miss
   (`record_served_reads`), so a served result registers what its miss registered (R22).
3. Docstrings of `Transaction.load`, `Snapshot` and `Database.history` state that their reads are
   outside the read set (R21, N2).
4. Fix round 1: `record_served_reads` handles an unknown node fail-closed and lists its types
   (`SERVED_TYPES`, with a parity test against `_plan_key`); the semantic composition reads
   declarations with engine code only (N2 closed).

Every other site already read through the model (Tasks 2-10) or is listed above as R21/R37.
