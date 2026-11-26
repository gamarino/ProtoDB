# Precondition Concurrency Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace proto_db's per-root conflict detection with the precondition/intention model: fine-grained units, serializability in commit order, no user code under the space root, then document it, merge to master and release.

**Architecture:** A new module `proto_db/preconditions.py` holds the canonical data (units, preconditions, ranges, the per-transaction read log with escalation) and the validator. Collection roots obtained from a transaction become *linked* instances (runtime fields only) that record reads in the transaction and carry their intentions per version. `Transaction._commit` gains a preparation phase (index keys, persistence) and a pre-pass (validate and reapply against R0 outside the lock); under the lock it validates by diff R0→R1 (R65) and reapplies on R1 only when the root moved.

**Tech Stack:** Python 3.12-3.14 and 3.14t (free-threaded), pytest, Sphinx docs. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-11-24-precondition-concurrency-model-design.md` (rules P-A..P-G, R1-R70; section 14 holds the approved amendments and prevails), with the review `docs/superpowers/specs/2026-11-24-precondition-concurrency-model-review.md` (B1-B8, N1-N5 and its correction list, all accepted).

## Global Constraints

- Branch `feat/precondition-concurrency` in worktree `/home/gamarino/Documentos/proyectos/worktrees/protobase-preconditions`, based on origin/master 56ed529 (it includes the runtime registry and the semantic MVP). The checkout `/home/gamarino/Documentos/proyectos/ProtoBase` is read-only.
- Product/import name is `proto_db`; never write "ProtoDB" or "ProtoBase" as a product name in docs or code.
- Every rule R<n> is normative. If a rule is impossible or ambiguous in the code, stop and ask the maintainer; never resolve by analogy with the old behavior.
- P-C: nothing under the space root runs an extractor, key function, computed attribute or user callback.
- P-G: no persisted format change except the version-7 root mark (R63) and the `unique` index attribute (R29).
- No timing-dependent tests: no sleeps for ordering, no wall-clock assertions. Interleavings are forced with barriers/events or with explicit two-transaction sequences (open A, open B, commit B, commit A).
- Tests run sequentially (`python -m pytest -q -p no:xdist`); each task runs its own tests plus `tests/unit/test_transactions.py`; the full suite runs at the end of each task that touches `proto_db/transactions.py`, and fully in Task 17.
- Every modified or deleted test is listed in the commit message with the rule that motivates it; no test is deleted without a replacement covering the new rule.
- Constants: `PRECONDITION_ESCALATION_LIMIT = 1024`, `INDEX_CATCH_UP_ROUNDS = 3`, root format version 7 with feature `"preconditions": 7`, `PROTOCOL_VERSION = 16`.
- Release version: `2.0.0rc5` (behavior-incompatible changes go before 2.0.0 final).
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- No `rm -rf`, `git clean`, `pkill`; never delete branches or worktrees without asking.

## Review Focus

1. A transaction reads a collection root, never assigns it, and writes something else: its reads must still be validated (R44/R45). Test in Task 4.
2. Two branches derived from one linked base, only the second assigned, under a concurrent commit to a key only the first branch touched: no conflict, and only the second branch's intentions are published (R43). Test in Task 4.
3. The same collection value assigned to two roots in one transaction: the second assignment is not linked (R46) and replaces with an object-root precondition. Test in Task 4.
4. A retry inside `Database.run` after a conflict must not see a link, a read log or an escalation left from the failed attempt (R47). Test in Task 2.
5. An empty-diff commit under the lock (R1 differs from R0 only by a re-committed unchanged base, `_commit_base` in a cluster) must not raise a conflict: the diff is by content units, not by root pointer. Test in Task 14.

---

### Task 1: Remove the intents mode (R53-R55)

**Files:**
- Delete: `proto_db/semantic/intents.py` (after the R54 inventory), and the mode branches in `proto_db/semantic/session.py`, `proto_db/semantic/entities.py`
- Modify: `proto_db/transactions.py` (`Database.new_transaction`, `Database.run`, `Transaction.__init__`, the `intents` property, `_check_not_logged`, `_check_not_written_by_log`, the replay in `_build_root` ~3867-3872, `mutable_names` ~3312)
- Modify tests: `tests/unit/test_semantic_intents.py` (rewritten to the cell path), `tests/unit/test_semantic_constraints.py`, `tests/unit/test_semantic_history_verifier.py`, `tests/concurrency/test_semantic_intents_concurrency.py`, `tests/unit/test_catalog_atoms.py`
- Modify: `benchmarks/semantic/credit_experiment.py`
- Create: `docs/superpowers/specs/2026-11-24-intents-removal-inventory.md`

**Interfaces:**
- Produces: `Database.new_transaction()` and `Database.run(fn, retries=10, backoff=(0.001, 0.2))` with no `intents` parameter; passing `intents=` raises `TypeError` (plain Python).
- Keeps: `proto_db.catalog.Intent` (type id 31), `IntentRecord` (type id 34), `intent_records`, `intent_log_root`, the conformance vectors with intent-log cases.

- [ ] **Step 1: Write the R54 inventory.** List every use of the mode and of the catalog types `Intent`/`IntentRecord` with file:line, marking which depend on the replay (the review's list is the starting point). Commit it before deleting anything.
- [ ] **Step 2: Write the failing tests** in `tests/unit/test_intents_removed.py`:

```python
def test_new_transaction_has_no_intents_parameter(db):
    with pytest.raises(TypeError):
        db.new_transaction(intents=True)

def test_run_has_no_intents_parameter(db):
    with pytest.raises(TypeError):
        db.run(lambda tr: None, intents=True)

def test_space_with_intent_log_root_opens_and_reads(intent_log_space):
    # fixture: a space written by the conformance vector that holds __intentLog
    db = intent_log_space.open_database("db")
    tr = db.new_transaction()
    assert tr.get_root_object("accounts") is not None
    tr.abort()

def test_leftover_intent_log_dropped_on_next_commit(intent_log_space):
    db = intent_log_space.open_database("db")
    db.run(lambda tr: tr.set_root_object("x", 1))
    assert "__intentLog" not in db.current_root_names(include_reserved=True)
```
(`current_root_names(include_reserved=True)` is a test helper in `tests/support/` that reads the DatabaseRoot's objects map directly; add it if absent.)

- [ ] **Step 3: Run** `python -m pytest -q tests/unit/test_intents_removed.py`. Expected: the first two FAIL (parameter accepted).
- [ ] **Step 4: Remove the mode** per R53: parameter, log creation, replay, fine reads per (cell, attribute), raw read/write refusals. The semantic session keeps one path: its operations use `get_mutable`/`set_mutable`/`new_mutable`/`remove_mutable` (unit = the cell, R3). Keep the drop of a leftover `__intentLog` (R55, a reserved-root write under R37).
- [ ] **Step 5: Rewrite the semantic tests** that used the mode so each covered behavior is asserted through the cell path; tests that only checked the mode's own mechanics are replaced by `test_intents_removed.py`.
- [ ] **Step 6: Run** `python -m pytest -q tests/unit tests/concurrency/test_semantic_intents_concurrency.py tests/conformance`. Expected: PASS; conformance digests unchanged.
- [ ] **Step 7: Commit** `semantic: remove the intents mode (R53-R55)`, listing each modified/deleted test with its rule.

### Task 2: Precondition core, conflict errors, mutables (R3, R6-R10, R36, R47, R66, section 5 mutables)

**Files:**
- Create: `proto_db/preconditions.py`
- Modify: `proto_db/errors.py` (`ProtoConflictError`), `proto_db/transactions.py` (`get_mutable`, `new_mutable`, `set_mutable`, `remove_mutable`, `mutable_names`, `_check_mutables`, retire `_mexpected`)
- Test: `tests/unit/test_preconditions_core.py`, `tests/unit/test_precondition_mutables.py`; rewrite `tests/unit/test_mutable_object.py` (380, 401, 414, 432)

**Interfaces:**
- Produces in `proto_db/preconditions.py`:
  - `ABSENT` — a module-level sentinel token for an absent unit.
  - `class UnitKind(enum.Enum)`: `ROOT, ROOT_PRESENCE, ROOT_NAMES, CELL, CELL_NAMES, KEY, ELEMENT, COUNT, RECORDS, POSITION, WHOLE, INDEX_DEFS, INDEX_GROUP, RANGE`.
  - `@dataclass(frozen=True) class Unit: kind: UnitKind; root: str | None = None; key: Any = None; index: str | None = None` (for CELL, `key` is the cell name; for INDEX_GROUP, `index` names the index and `key` the index key).
  - `@dataclass(frozen=True) class KeyRange: root: str; index: str | None; low: Any; high: Any; low_inclusive: bool; high_inclusive: bool` (`index=None` means the Dictionary's own keys; `low`/`high` may be `None` for unbounded).
  - `PRECONDITION_ESCALATION_LIMIT = 1024`
  - `class ReadLog` with `record(unit: Unit, expected) -> None` (first observation wins, R8/R9), `record_range(r: KeyRange) -> None`, `whole(root: str) -> bool`, `preconditions() -> dict[Unit, Any]`, `ranges() -> tuple[KeyRange, ...]`; recording the 1025th fine unit of one root replaces that root's fine units by `Unit(WHOLE, root)` (R66). Thread-safe under a lock owned by the log (R44).
- Produces in `proto_db/errors.py`: `ProtoConflictError(message, *, unit: Unit | None = None, database: str | None = None)` with attributes `unit` and `database` that survive `copy.copy`.
- Mutables: `Transaction` records `Unit(CELL, key=name)` with the base token on `get_mutable`, `set_mutable`, `remove_mutable`; `new_mutable` records `(CELL name, ABSENT)`; `mutable_names()` records `Unit(CELL_NAMES)` with the base's sorted name tuple token.

- [ ] **Step 1: Write the failing tests.**

```python
# test_preconditions_core.py
def test_first_observation_wins():
    log = ReadLog(); u = Unit(UnitKind.KEY, "d", key="a")
    log.record(u, 1); log.record(u, 2)
    assert log.preconditions()[u] == 1

def test_escalation_after_limit():
    log = ReadLog()
    for i in range(PRECONDITION_ESCALATION_LIMIT + 1):
        log.record(Unit(UnitKind.KEY, "d", key=i), i)
    assert log.whole("d")
    assert all(u.kind is UnitKind.WHOLE for u in log.preconditions() if u.root == "d")

def test_conflict_error_unit_survives_copy():
    e = ProtoConflictError("x", unit=Unit(UnitKind.CELL, key="c"), database="db")
    assert copy.copy(e).unit == e.unit and copy.copy(e).database == "db"

# test_precondition_mutables.py  (two-transaction sequences: open A, open B, commit B, commit A)
def test_blind_set_mutable_conflicts_with_concurrent_set(db): ...   # A.set_mutable("c",1); B.set_mutable("c",2); B commit; A commit -> ProtoConflictError, e.unit == Unit(CELL, key="c")
def test_set_mutable_other_cell_no_conflict(db): ...                # A sets "c1", B sets "c2": both commit, both values present
def test_remove_mutable_of_cell_removed_by_other_conflicts(db): ... # was a no-op (section 5 table)
def test_new_mutable_conflicts_when_other_created_it(db): ...
def test_mutable_names_conflicts_on_concurrent_create(db): ...
def test_mutable_names_no_conflict_on_value_change(db): ...         # B changes a cell's value only
def test_writing_base_value_keeps_precondition(db): ...             # R10: A sets c to its base value; B changed c; A conflicts
def test_retry_starts_with_fresh_log(db): ...                       # Review Focus 4: second attempt of db.run records nothing from the first
```
Each `...` body: build the base with `db.run`, open A and B with `db.new_transaction()`, apply the named operations, `B.commit()`, then `with pytest.raises(ProtoConflictError) as e: A.commit()` (or `A.commit()` and assert both values), asserting `e.value.unit` as written.

- [ ] **Step 2: Run** `python -m pytest -q tests/unit/test_preconditions_core.py tests/unit/test_precondition_mutables.py`. Expected: FAIL (module missing).
- [ ] **Step 3: Implement** `proto_db/preconditions.py` and the error attributes; switch mutable writes to compare against the base token (`exact_token`, atoms by pointer, R6); delete `_mexpected` and the blind/read distinction; `_check_mutables` validates every `CELL`/`CELL_NAMES` precondition against the current mutables map, and R35 applies intentions on the current map.
- [ ] **Step 4: Rewrite** `tests/unit/test_mutable_object.py` 380/401/414/432 (blind writes now conflict).
- [ ] **Step 5: Run** the two new files, `tests/unit/test_mutable_object.py`, `tests/unit/test_transactions.py`, then the full suite. Expected: PASS.
- [ ] **Step 6: Commit** `transactions: preconditions core; mutables compare against the base (section 5, R36, R66)`.

### Task 3: Object roots and root names (R2, R11, R12, section 5 roots, section 6 transaction reads)

**Files:**
- Modify: `proto_db/transactions.py` (`get_root_object`, `set_root_object`, `remove_root_object`, `root_names`, `_build_root`; delete `_merged`, `_replaced`, `replace=`), `proto_db/collections/base.py` (delete `log_is_complete`, `_log_complete`; keep `net_effect`, `counter_net_effect`)
- Modify docs that use `replace=True`: `docs/source/tutorials/operations.rst:82`, `docs/source/tutorials/vector-search.rst:133`
- Test: `tests/unit/test_precondition_roots.py`; rewrite the merge/blind/replace tests of `tests/unit/test_transactions.py` (475, 488, 1033, 1089, 1098 and the rest of that family)

**Interfaces:**
- Produces: `Transaction.set_root_object(name: str, value) -> None` (no `replace`); reads record `Unit(ROOT, name)` (object root, expected = base token or `ABSENT`), `Unit(ROOT_PRESENCE, name)` (collection root: expected = the base value's class, R12), `Unit(ROOT_NAMES)` (`root_names()`).
- Until Task 4 lands, a collection root assigned by `set_root_object` is treated as unlinked (R11): object-root precondition, value replaces.
- R37: reserved roots (`__*`, including `__catalog`, `__currentModules`, `__semanticCatalog`) keep `_merged_registry`/`_merged_semantic_catalog` and are outside the model.

- [ ] **Step 1: Write the failing tests** in `test_precondition_roots.py` (same two-transaction pattern):
  `test_object_root_write_conflicts_with_concurrent_write` (unit `Unit(ROOT,"o")`), `test_object_roots_different_names_no_conflict`, `test_object_root_attribute_writes_conflict` (R2: A and B change different attributes of the same object root → conflict), `test_absent_root_create_conflicts_with_concurrent_create`, `test_remove_root_conflicts_with_concurrent_write`, `test_root_names_conflicts_on_concurrent_create`, `test_unlinked_collection_replaces_and_compares_base` (A assigns `Dictionary()` built separately over a root B changed → conflict; no merge), `test_set_root_object_rejects_replace_keyword` (`TypeError`), `test_class_change_conflicts` (R12: A reads collection root "c" (ROOT_PRESENCE), B replaces it with a Set; A commits a mutable → conflict), `test_reserved_roots_keep_their_merge` (two concurrent module installs still both land).
- [ ] **Step 2: Run** them. Expected: FAIL.
- [ ] **Step 3: Implement** the root rules; delete `_merged`, `_replaced`, `replace=`, `log_is_complete`, `_log_complete`, the consumed-log step (tx.py 3796-3800).
- [ ] **Step 4: Fix** the two tutorials (`replace=True` removed; assign the new value, which now replaces comparing against the base) and rewrite the listed `test_transactions.py` tests.
- [ ] **Step 5: Run** the new file, `tests/unit/test_transactions.py`, `tests/docs`, full suite. Expected: PASS.
- [ ] **Step 6: Commit** `transactions: object roots compare against the base; root_names recorded; retire replace and op-log merge (section 5, R2, R12)`.

### Task 4: Linked collections; Dictionary and HashDictionary (R5, R11, R13, R17, R18, R40-R47, N5, D51 boxes)

**Files:**
- Create: `proto_db/collections/linking.py`
- Modify: `proto_db/collections/base.py` (runtime fields, recording hooks), `proto_db/collections/dictionaries.py`, `proto_db/collections/hash_dictionaries.py`, `proto_db/collections/avl.py` (positional helpers), `proto_db/core/atom.py` (pointer-preserving copy), `proto_db/transactions.py` (`get_root_object` returns a linked copy; `set_root_object` checks the link; `_build_root` validates and reapplies)
- Test: `tests/unit/test_linked_collections.py`, `tests/unit/test_precondition_dictionary.py`; rewrite `tests/contracts/collection_contract.py` (`rebase_model` at 21/52/63) into an intention contract

**Interfaces:**
- Produces in `linking.py`:
  - `@dataclass(frozen=True) class Link: transaction: "Transaction"; root: str; base_pointer: AtomPointer | None`
  - `def linked_copy(atom, link: Link)` — a new instance with the same persisted fields and the same `atom_pointer` (loaded first, outside `_AtomCache._ident`), carrying `_link` (R41).
  - `def link_of(value) -> Link | None` — valid only if the transaction is active and is the caller's (R46/R47).
  - `def record_read(collection, unit: Unit, expected) -> None` and `def record_whole(collection) -> None` — no-ops on unlinked collections and while the engine reads (B3).
  - `engine_reads()` — a context manager that suppresses recording for engine-internal reads (mutators, planner estimates).
- Collections: every version carries `_link` (inherited through `_with`, R42) and `_changes: tuple` (the per-version intention record, R43; the existing `_op_log` is renamed `_changes`, completeness logic deleted). On linked instances `count`, `root` and `indexes` read by user code record `Unit(WHOLE, root)` (B3) — implemented as properties that call `record_whole` unless inside `engine_reads()`.
- Dictionary/HashDictionary: `get_at(k)`, `has(k)`, `__contains__` record `Unit(KEY, root, key=k)` with the stored-form token (a `ValueBox` by pointer, D51); `set_at`, `remove_at` record the same precondition when the key was not already written by this transaction (R9) and append to `_changes`. Iteration, `items`, `keys`, `values`, `elements`, `content_equals`, `len` record WHOLE (until Task 10 refines `keys(after=)`).
- `Dictionary.key_at_position(i: int)` and `Dictionary.position_of(k) -> int` (R17), built on `avl.key_count_below`/`avl.key_iter_from`; both record WHOLE.
- Classification (R1, N5): fine-grained treatment only for the six listed classes and for subclasses that override none of the listed operations (`linking.fine_grained(cls) -> bool`, by method identity); otherwise object root.
- Versions produced by an unlisted operation (`slice`, `head`, `tail`, `intersection`, `rebuilt`, `from_items`) carry a `_whole` mark; assigning one publishes with a whole-collection precondition (P-B).
- `_build_root`: for each assigned linked root, validate its preconditions (R33, all before any intention), then reapply `_changes` on the current collection with the tree operations (R34); a WHOLE-precondition root publishes the transaction's value.

- [ ] **Step 1: Write the failing tests.**
  `test_linked_collections.py`: `test_get_root_object_returns_linked_copy_not_cached_atom` (the cached atom has no `_link`; `link_of(c).root == "d"`), `test_link_inherited_by_mutators`, `test_link_invalid_after_commit_and_abort` (R47), `test_collection_from_other_root_is_unlinked` (Review Focus 3: same value assigned to "d" and "e"; "e" gets an object-root precondition), `test_collection_from_other_transaction_is_unlinked`, `test_unassigned_version_reads_still_validated` (Review Focus 1), `test_two_branches_only_assigned_published` (Review Focus 2), `test_count_read_on_modified_version_records_whole` (R18), `test_read_of_own_written_key_records_nothing` (R9), `test_subclass_overriding_set_at_is_object_root` (N5), `test_slice_result_assigned_takes_whole_collection`.
  `test_precondition_dictionary.py`, one pair per row of the section 5 and 6 Dictionary/HashDictionary tables, parametrized over both classes: `test_set_at_other_key_no_conflict_both_present`, `test_set_at_same_key_conflicts` (unit `Unit(KEY,"d",key="k")`), `test_insert_same_new_key_conflicts_even_same_value`, `test_remove_at_other_key_no_conflict`, `test_remove_at_same_key_conflicts`, `test_get_at_then_other_key_change_no_conflict`, `test_get_at_absent_then_concurrent_insert_conflicts`, `test_has_same_pair`, `test_iteration_conflicts_with_any_change`, `test_key_at_position_conflicts_with_any_change`, `test_boxed_value_compared_by_box_pointer` (a large value; validation does not load it — assert via a counting storage from `tests/support/`).
- [ ] **Step 2: Run** them. Expected: FAIL.
- [ ] **Step 3: Implement** `linking.py`, the pointer-preserving copy, recording, `_changes`, the positional reads, the classification and the validation/reapplication in `_build_root`.
- [ ] **Step 4: Rewrite** the collection contract: for each collection, applying a version's `_changes` to a concurrently modified current must equal the serial result (replaces `rebase_model`).
- [ ] **Step 5: Run** both new files, `tests/contracts`, `tests/unit/test_dictionary.py`, `tests/unit/test_hash_dictionary.py`, `tests/unit/test_transactions.py`, full suite. Expected: PASS.
- [ ] **Step 6: Commit** `collections: linked roots, per-version intentions, Dictionary/HashDictionary units (section 9, R17)`.

### Task 5: Serializability checker (section 12 item 2)

**Files:**
- Create: `tests/concurrency/test_serializability.py`, `tests/support/serial_model.py`
- Modify: `tests/concurrency/harness.py` (reuse `run_threads`, `Attempts`)

**Interfaces:**
- Produces: `serial_model.Model` — an in-memory replica of roots, cells, Dictionary/HashDictionary (later Set, CountedSet, RKD, List, ranges) with `apply(op) -> read_value`; `serial_model.random_program(rng, state_names, ops) -> list[Op]`; the checker `check_history(records, initial) -> None` asserting that re-running committed programs in commit order reproduces the final state and every value each committed program read.
- Commit order is recorded as the published root's epoch for each committed transaction (read after commit from the transaction).

- [ ] **Step 1: Write** `test_serializability_dictionary_and_cells` with `seed=20261124`, 4 threads × 200 transactions, a state of 3 roots (one object, one Dictionary, one HashDictionary) × 6 keys + 4 cells, operations limited to Tasks 2-4. Each thread barrier-starts; programs read and write random units; record (epoch, program, reads).
- [ ] **Step 2: Run** `python -m pytest -q tests/concurrency/test_serializability.py`. Expected: PASS on the Task 4 code (if it fails, the failure is a Task 2-4 defect: fix it there).
- [ ] **Step 3: Commit** `tests: serializability checker over the closed set of operations`. Tasks 6, 7, 9 and 10 each extend `serial_model` and add their operations to the checker's mix.

### Task 6: Set, CountedSet, RepeatedKeysDictionary (section 5 and 6 rows, D26)

**Files:**
- Modify: `proto_db/collections/sets.py`, `proto_db/collections/dictionaries.py` (RKD), `tests/support/serial_model.py`, `tests/concurrency/test_serializability.py`
- Test: `tests/unit/test_precondition_sets.py`

**Interfaces:**
- Set: `add(e)` no precondition, intention present; `remove(e)` records `Unit(ELEMENT, root, key=e)` with expected PRESENT or ABSENT per the base (the third row: absent in base → precondition "still absent", no intention); `union` = add per element, `difference` = remove per element, `intersection` = WHOLE; `has(e)`/`in` records ELEMENT.
- CountedSet: increments/decrements compose without precondition through `counter_net_effect` (D26); the composed count never goes below 0 (a decrement past 0 equals the serial no-op); `has(e)`, `count_of(e)` record `Unit(COUNT, root, key=e)` with the base count.
- RKD: record operations compose as today (D26; `remove_at`/`remove_key` is a precondition-free clear, serializable); `get_at(k)`, `get(k)`, `has(k)` record `Unit(RECORDS, root, key=k)` with the token of k's nested CountedSet (R5: the nested set has no units).

- [ ] **Step 1: Write the failing tests**, a pair per row: `test_set_add_concurrent_remove_leaves_present`, `test_set_remove_present_conflicts_with_concurrent_remove`, `test_set_remove_absent_conflicts_with_concurrent_add`, `test_set_add_different_elements_no_conflict`, `test_set_intersection_conflicts_with_any_change`, `test_set_has_pair`, `test_counted_set_concurrent_increments_compose` (two +1 → +2), `test_counted_set_double_decrement_floor_zero` (base count 1, two concurrent decrements → 0, no error), `test_counted_set_count_of_conflicts_with_concurrent_increment`, `test_rkd_records_compose`, `test_rkd_get_at_conflicts_with_record_change_same_key`, `test_rkd_get_at_other_key_no_conflict`.
- [ ] **Step 2: Run** `python -m pytest -q tests/unit/test_precondition_sets.py`. Expected: FAIL.
- [ ] **Step 3: Implement**; extend the serial model and the checker mix with these operations.
- [ ] **Step 4: Run** the new file, `tests/unit/test_counted_set.py`, `tests/concurrency/test_counters.py`, `tests/concurrency/test_serializability.py`, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `collections: Set, CountedSet and RepeatedKeysDictionary units (section 5, D26)`.

### Task 7: List (R14, R15, R16, R48)

**Files:**
- Modify: `proto_db/collections/lists.py`, `tests/support/serial_model.py`, `tests/concurrency/test_serializability.py`
- Test: `tests/unit/test_precondition_list.py`

**Interfaces:**
- List gains `_changes` like the others (R48): `append_last(v)`/`extend(vs)` append intentions applied at the end in commit order, no precondition; `set_at(i, v)` with `0 <= i < base length` records `Unit(POSITION, root, key=i)` (exists in current and equal to base); `set_at` on a position this transaction appended folds into that append intention (R9); `get_at(i)` records POSITION; a negative index normalizes against the transaction's own view and records WHOLE (R15, review correction); `insert_at`, `remove_at`, `append_first` and every other structural operation mark the list WHOLE for the transaction (R14), and later reads add nothing (R19).

- [ ] **Step 1: Write the failing tests**: `test_append_last_concurrent_both_kept_commit_order`, `test_extend_concurrent_both_kept`, `test_set_at_other_position_no_conflict`, `test_set_at_same_position_conflicts` (unit `Unit(POSITION,"l",key=2)`), `test_set_at_on_own_append_follows_it` (A appends x then `set_at(len_base, y)`; B appends z first; result ends `[..., z, y]`), `test_negative_index_records_whole`, `test_insert_at_conflicts_with_concurrent_append`, `test_get_at_pair`.
- [ ] **Step 2: Run**. Expected: FAIL.
- [ ] **Step 3: Implement**; extend the serial model.
- [ ] **Step 4: Run** the new file, `tests/unit/test_list.py`, the checker, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `collections: List units and append intentions (R14-R16, R48)`.

### Task 8: Extractor purity (R27, N4)

**Files:**
- Modify: `proto_db/indexing/keyspec.py` (around `KeySpec.extract`), `proto_db/transactions.py` (every public read entry point), `proto_db/core/mutable.py` or the module defining `MutableObject.get`
- Create: `proto_db/indexing/purity.py`
- Test: `tests/unit/test_extractor_purity.py`

**Interfaces:**
- Produces: `purity.extracting()` — a context manager setting a `contextvars.ContextVar` while an extractor runs; `purity.check_not_extracting(what: str) -> None` raising `ProtoUserError` (message names `what`). `KeySpec.extract` wraps every extractor call in `extracting()`; `Transaction.get_root_object`, `get_mutable`, `root_names`, `mutable_names`, `load`, `snapshot_mutable` and `MutableObject.get` call `check_not_extracting`.

- [ ] **Step 1: Write the failing tests**: `test_extractor_reading_transaction_fails_at_mutation`, `test_extractor_resolving_mutable_fails`, `test_extractor_reading_transaction_fails_in_query` (a `Term.matches` path), `test_pure_extractor_unaffected`.
- [ ] **Step 2: Run**. Expected: FAIL.
- [ ] **Step 3: Implement**.
- [ ] **Step 4: Run** the new file, `tests/unit/test_equality_index.py`, `tests/contracts/index_contract.py`, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `indexing: extractors may not read through a transaction (R27)`.

### Task 9: Indexes under the model (R24-R26, R28-R30, R49, B4-B8)

**Files:**
- Modify: `proto_db/indexing/base.py`, `keyed.py`, `equality.py`, `range.py`, `vector.py`, `vector_backends.py`, `proto_db/collections/base.py` (`IndexSet`, `add_index`, `remove_index`, `get_index`, `rebuilt_stale`), `proto_db/query/planner.py`, `proto_db/query/plans.py`, `docs/source/format-spec.rst` (index definition: `unique`)
- Test: `tests/unit/test_precondition_indexes.py`, `tests/unit/test_unique_index.py`, `tests/unit/test_no_user_code_under_lock.py`

**Interfaces:**
- `Index.with_added_key(element, key)`, `Index.with_removed_key(element, key)`, `Index.with_replaced_keys(old, old_key, new, new_key)` — the precomputed-key variants (R49); `VectorIndex` variants take the vector; the existing `with_added`/`with_removed`/`with_replaced` compute the key and call them.
- Preparation phase (called from `Transaction._commit` before the pre-pass): `IndexedCollection.prepare_changes(base) -> tuple` computes every key for every element in `_changes` against every index of the base collection (R24), builds `add_index` indexes (R26) and rebuilds stale format-3 indexes, marking that collection WHOLE (B8). The keys travel in `_changes`.
- Every transaction with intentions on an indexed collection records `Unit(INDEX_DEFS, root)` with the token of the tuple of each index's persisted `_definition` fields (R25); `index_names`/`get_index` record INDEX_DEFS; `get_index` on a linked collection also records WHOLE (B5).
- Plans: a plan answered by a KeyGroupIndex (EqualityIndex or RangeIndex) for `==` or `in` records `Unit(INDEX_GROUP, root, index=name, key=k)` per key with the group tuple `(members pointer, total)` token, registered when the plan is built (B4; a cached result re-registers the same); every other plan records WHOLE (until Task 10 adds ranges).
- `EqualityIndex(..., unique: bool = False)`; `unique` joins `_definition` (default False when absent on read). With a unique index, every intention adding or replacing an element with key x carries `Unit(INDEX_GROUP, ..., key=x)` expecting "empty, or only the replaced entry"; violation against the own view → `ProtoValidationError` at the write; against a concurrent commit → `ProtoConflictError` (R29). Rules (B6): MISSING and None never violate; `1` and `1.0` are duplicates; a key without `order_key` raises `ProtoValidationError`; in a Dictionary, the replaced element is the entry at the same key.
- ivf_flat (B7): a collection with an ivf_flat index marks every write WHOLE (no reapplication); exact and hnsw use the precomputed vector.
- Under the lock no extractor runs: `rebase`-time replay of `add_index` and `rebuilt_stale` are gone from the lock path.

- [ ] **Step 1: Write the failing tests**: `test_index_keys_precomputed_no_extractor_under_lock` (a `tests/support/recording_index.py` extractor records whether the space root is held — via a test hook `Transaction._holding_root_lock()` — never true, also on reapplication over a changed R1), `test_concurrent_add_index_conflicts_with_write` (R25/R26), `test_equality_lookup_conflicts_only_same_group`, `test_range_index_eq_lookup_records_group`, `test_get_index_records_whole`, `test_stale_format3_index_rebuilt_in_preparation`, `test_ivf_flat_write_takes_whole_collection`, `test_hnsw_and_exact_reapply_from_vector`; `test_unique_index.py`: `test_unique_concurrent_same_value_one_commits` (section 12 item 7), `test_unique_violation_own_view_is_validation_error`, `test_unique_retry_sees_duplicate_validation_error` (via `db.run`), `test_unique_missing_and_none_allowed`, `test_unique_int_float_duplicate`, `test_unique_unorderable_key_refused`, `test_unique_replace_same_dictionary_key_allowed`, `test_unique_attribute_persisted_and_old_index_reads_false`.
- [ ] **Step 2: Run**. Expected: FAIL.
- [ ] **Step 3: Implement**; update the index section of `format-spec.rst` for `unique`.
- [ ] **Step 4: Run** the new files, `tests/contracts`, `tests/unit/test_equality_index.py`, the vector tests (`tests/unit/hnsw_checks.py` users), `tests/concurrency/test_indexed_roots.py`, `tests/concurrency/test_index_weights_concurrency.py`, the checker (extended with equality lookups), full suite. Expected: PASS.
- [ ] **Step 5: Commit** `indexing: precomputed keys, group invariants, unique index (section 7)`.

### Task 10: Range invariants (R67, R68)

**Files:**
- Modify: `proto_db/collections/dictionaries.py` (`keys(after=)`, `_scan_window`), `proto_db/query/plans.py` (RangeIndex range scans), `proto_db/preconditions.py` (range check), `tests/support/serial_model.py`
- Test: `tests/unit/test_precondition_ranges.py`

**Interfaces:**
- Range reads record `KeyRange` through `ReadLog.record_range` instead of WHOLE: a RangeIndex range plan records `KeyRange(root, index=name, low, high, incl...)` of the scanned index keys; a Dictionary window from `start` records `KeyRange(root, None, start, last_returned_key_or_None, True, True)` (`None` high = to the end when exhausted).
- `preconditions.range_violated(r: KeyRange, changed_keys: Iterable) -> bool` using `order_key` comparisons (data only, P-C).
- Full validation (no pre-pass): a range fails if the base's and the current's sub-trees over `[low, high]` differ in any key or value (index: group tuple).

- [ ] **Step 1: Write the failing tests**: `test_range_query_then_write_no_conflict_outside_range` (the January/March example), `test_range_query_conflicts_with_change_inside_range`, `test_range_bounds_inclusive_exclusive` (changes exactly at `low`/`high`), `test_dictionary_window_conflicts_only_inside_window`, `test_exhausted_window_covers_to_end` (a later key inserted beyond the last → conflict), `test_unbounded_scan_still_whole`.
- [ ] **Step 2: Run**. Expected: FAIL.
- [ ] **Step 3: Implement**; add range reads to the checker.
- [ ] **Step 4: Run** the new file, `tests/unit/test_query_plans.py`, `tests/unit/test_planner.py`, `tests/unit/test_pager.py`, `tests/concurrency/test_pager_stability.py`, the checker, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `preconditions: range invariants for RangeIndex scans and Dictionary windows (R67, R68)`.

### Task 11: Read-path audit of the upper layers (R21, R22, N2)

**Files:**
- Modify: `proto_db/query/planner.py`, `plans.py`, `cache.py` (`QueryCache`, `QueryResult`), `pager.py` (`Pager`, `portable_pager`), `linq.py` (`Queryable.pager`), `proto_db/transactions.py` (`Transaction.pager` ~2944-2963, `installed_modules`, `catalog_object`), `proto_db/semantic/expressions.py`, `entities.py`, `session.py`
- Create: `docs/superpowers/specs/2026-11-24-read-path-audit.md` (every site from the review's R22 list with its resolution)
- Test: `tests/unit/test_read_paths.py`

**Interfaces:**
- Every listed site reads through the recording operations; `Transaction.pager` records WHOLE explicitly (or a range, when keyed by a Dictionary window); a `QueryCache` hit registers the same preconditions as the plan that produced it (the plan's recorded units are stored in `QueryResult`); planner estimates run inside `engine_reads()`.
- R21 list (unrecorded, documented outside the guarantee): `snapshot_mutable`, `Snapshot`/`Database.snapshot_at`, `Database.history()`, objects from another transaction, `Transaction.load(ptr)`; reserved-root reads (`installed_modules`, `catalog_object`, the session's catalog read) stay outside the model under R37.

- [ ] **Step 1: Write the audit document**, then one failing test per path (section 12 item 5): `test_query_read_then_concurrent_change_conflicts`, `test_cached_query_result_registers_same_reads`, `test_pager_read_then_concurrent_change_conflicts`, `test_transaction_pager_records_reads`, `test_linq_pager_records_reads`, `test_semantic_query_read_then_concurrent_change_conflicts`, `test_semantic_describe_from_snapshot_records_nothing`.
- [ ] **Step 2: Run**. Expected: FAIL where today's code reads tree nodes directly.
- [ ] **Step 3: Fix** each site.
- [ ] **Step 4: Run** the new file, `tests/unit/test_semantic*`, `tests/unit/test_query_cache.py`, `tests/unit/test_query_plans.py`, `tests/unit/test_linq.py`, `tests/unit/test_pager.py`, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `query, semantic: every read path records through the model (R22)`.

### Task 12: Preparation, pre-pass and delta validation (section 8, R31-R35, R39, R64, R65, N3)

**Files:**
- Create: `proto_db/collections/diff.py`
- Modify: `proto_db/transactions.py` (`_commit`, `_commit_root`, `_standalone_round`, `_build_root`, `_scoped_build_root`), each collection module (`changed_units`)
- Test: `tests/unit/test_tree_diff.py`, `tests/unit/test_prepass.py`

**Interfaces:**
- `diff.changed_keys(old_root, new_root) -> Iterator[Any]` over AVL trees (keys whose presence or value token differs), skipping equal-pointer subtrees; each collection class implements `changed_units(self, other, root: str) -> Iterator[Unit]` (Dictionary/HashDictionary KEY, Set ELEMENT, CountedSet COUNT, RKD RECORDS, List POSITION — or WHOLE when lengths/structure diverge beyond appends), plus `changed_index_groups(self, other, index: str) -> Iterator[key]`.
- `_commit` phases: close → preparation (R9 collapse, Task 9 `prepare_changes`, persist reachable atoms) → pre-pass (read R0 = last published root; full validation against R0; reapply on R0; persist new nodes; keep the candidate maps, R64) → under the lock: R1 == R0 (same pointer) → build roots from the candidate; else delta validation (R65: changed units R0→R1 ∩ recorded preconditions/ranges → `ProtoConflictError` naming the first such unit), reapply on R1, persist, publish → after: end links (R47).
- R23/N3: a transaction with no intentions and no module install/catalog update publishes nothing; module installs and catalog updates count as intentions.
- Group commit (R32): the candidate serves only the round's first entry and only when R1 is R0; later entries diff R0 against the round's current root.

- [ ] **Step 1: Write the failing tests**: `test_diff_equal_trees_yields_nothing_without_loading` (counting storage: zero reads), `test_diff_yields_exactly_changed_keys` (property-style over seeded random edits, `seed=7`), `test_prepass_unchanged_root_no_storage_reads_under_lock` (section 12 item 8: a storage wrapper counts reads while `_holding_root_lock()`; expect 0 and no reapplication), `test_prepass_changed_root_validates_and_reapplies_once`, `test_prepass_conflict_raised_without_taking_lock`, `test_delta_validation_cost_independent_of_read_set` (A reads 1000 keys; B changes 1 unrelated key; count unit comparisons under the lock: ≤ the diff size), `test_group_commit_second_entry_builds_on_first` (force a round with `tests/concurrency/test_standalone_group_commit.py`'s mechanism), `test_read_only_transaction_never_conflicts` (R23).
- [ ] **Step 2: Run**. Expected: FAIL.
- [ ] **Step 3: Implement** the diff and the phases.
- [ ] **Step 4: Run** the new files, `tests/concurrency/test_standalone_group_commit.py`, `tests/unit/test_transactions.py`, the checker, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `transactions: preparation, pre-pass and delta validation under the lock (section 8, R64, R65)`.

### Task 13: Index creation catch-up (R69)

**Files:**
- Modify: `proto_db/transactions.py` (pre-pass), `proto_db/collections/base.py` (`add_index`/`remove_index` intentions)
- Test: `tests/unit/test_index_catch_up.py`

**Interfaces:**
- `INDEX_CATCH_UP_ROUNDS = 3` in `proto_db/preconditions.py`. A collection whose only operations in the transaction are `add_index`/`remove_index` and with no recorded read is marked `catch_up`; the pre-pass diffs base→R0, applies the changed elements to the index under construction (extractors allowed: outside the lock), sets the expected value to R0's collection token, and repeats with a newer published root up to the limit.

- [ ] **Step 1: Write the failing tests**: `test_add_index_survives_concurrent_writes` (B commits 3 writes between A's `add_index` and A's commit; A commits; the index covers all elements, checked by a lookup per element), `test_add_index_with_prior_read_does_not_catch_up` (A also reads the collection → conflict), `test_catch_up_round_limit_then_conflict` (writes land during every pre-pass round, forced with a storage hook that commits B on each R0 read; after 3 rounds → `ProtoConflictError`).
- [ ] **Step 2: Run**. Expected: FAIL.
- [ ] **Step 3: Implement**.
- [ ] **Step 4: Run** the new file and the index tests, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `indexing: add_index catches up with concurrent writes (R69)`.

### Task 14: Cluster, version mark and three-mode fixture (R38, R50-R52, R63, R60 Python side)

**Files:**
- Modify: `proto_db/storage/format.py` (`FORMAT_VERSION = 7`, `FEATURE_VERSIONS["preconditions"] = 7`, `READABLE_FORMAT_VERSIONS` adds 7, root version 7 for every root this code publishes), `proto_db/cluster/protocol.py` (`PROTOCOL_VERSION = 16`), `proto_db/transactions.py` (`_commit_under_lease`, `_permit_round`, `_lease_base`), `docs/source/format-spec.rst` (format 7, the one-way upgrade, "Format stability" amended), `docs/source/coordinator-protocol.rst`
- Create: `tests/support/modes.py`, `tests/conformance/concurrency_scenarios.json`, `tests/conformance/test_concurrency_scenarios.py`
- Test: `tests/unit/test_format_v7.py`, `tests/concurrency/test_cluster_preconditions.py`

**Interfaces:**
- `modes.space_in_mode(mode: str, tmp_path)` with `mode in ("standalone", "group", "cluster")` — "group" forces queued commits into one round; "cluster" builds a 3-node cluster from `tests/support/cluster.py`. Tasks' precondition tests that live in `test_precondition_*.py` are parametrized over the three modes in this task (fixture `db_any_mode`).
- Cluster: validation and reapplication run on the building node under the permission (R38); the pre-pass uses `coordinator.current_root()` as R0; pre-pass conflicts take the normal backoff.
- Conformance scenarios (R60, data only): each `{"name", "base", "a": [ops], "b": [ops], "order": ["b","a"], "expect": {"state": ...} | {"conflict": {"kind", "root", "key"}}}`; at least one scenario per row of the section 5 and 6 tables; the runner executes them through proto_db; the format-spec digest of the vector set is re-pinned.

- [ ] **Step 1: Write the failing tests**: `test_new_root_is_version_7`, `test_space_written_by_rc4_opens_without_migration` (a pinned version-6 fixture from `tests/conformance`), `test_writer_without_version_7_rejected` (section 12 item 9: decoding a v7 root with `READABLE_FORMAT_VERSIONS` patched to {2..6} raises `ProtoNotSupportedError`), `test_protocol_version_16`, `test_cluster_dictionary_keys_no_conflict`, `test_cluster_same_key_conflicts`, `test_cluster_recommitted_base_is_not_a_conflict` (Review Focus 5), `test_concurrency_scenarios` (all scenarios, all three modes).
- [ ] **Step 2: Run**. Expected: FAIL.
- [ ] **Step 3: Implement**; parametrize the `test_precondition_*.py` files over `db_any_mode`.
- [ ] **Step 4: Run** the new files, `tests/unit/test_cluster_*`, `tests/concurrency/test_cluster_*`, `tests/conformance`, full suite. Expected: PASS.
- [ ] **Step 5: Commit** `format 7 and protocol 16: the precondition model's mark; cluster validation (R38, R50-R52, R63)`.

### Task 15: Documentation, tutorials, examples, CHANGELOG (section 10, section 12 documentation, R70)

**Files:**
- Rewrite: `docs/source/concurrency-model.rst`, `docs/source/tutorials/concurrency.rst`
- Modify: `docs/decisions.md` (new decision **D73 — The precondition concurrency model**, recording P-A..P-G and the unit tables, and amendment notes on D9, D26 (kept), D33, D34, D35), `docs/source/tutorials/extensible-models.rst` (~1027 blind `set_mutable`), `operations.rst`, `vector-search.rst`, `erp.rst`, `cluster.rst`, `indexing-querying.rst` (unique index, range queries), `docs/source/performance.rst` (intents mode removed), `docs/source/api.rst`, `docs/source/objects-across-transactions.rst` (links end with the transaction), `docs/kit/semantic/*`, `docs/kit/erp_demo.py`, `examples/transactions_concurrency.py`, `examples/README.md`, `README.md`, `CHANGELOG.md` (`[Unreleased]`: every row of the section 10 API table plus format 7, protocol 16, ranges, delta validation, escalation, catch-up, intents mode removed)
- Test: `tests/docs` (existing doc tests) and running every file in `examples/`

**Interfaces:**
- The concurrency page contains: the units per operation (section 5 and 6 tables), the "verify and add" table (section 7), the "verify reference" pattern, ids without a shared counter (UUID/ULID), Snapshot as the deliberate escape, retries and external effects, lock cells with owner and expiry, the known limits (section 13 list plus escalation and catch-up limits), the one-way format-7 upgrade, and the storage-compaction note (R70).

- [ ] **Step 1: Rewrite and update** the pages and examples listed.
- [ ] **Step 2: Run** `python -m pytest -q tests/docs`, `for f in examples/*.py; do python "$f" || exit 1; done`, `python -m sphinx -W -b html docs/source /tmp/claude-1000/-home-gamarino-Documentos-proyectos/c08c626b-89d4-4a1e-b668-5ae9b9051093/scratchpad/sphinx-out` (warnings are errors). Expected: PASS, no warnings.
- [ ] **Step 3: Commit** `docs: the precondition concurrency model (D73), tutorials and examples`.

### Task 16: protoBaseST proposal (R62)

**Files:**
- Create in protoBaseST, worktree `/home/gamarino/Documentos/proyectos/worktrees/protobasest-preconditions` on branch `docs/precondition-model-proposal` from origin/main: `docs/proposals/2026-11-24-precondition-concurrency-model.md`

**Interfaces:**
- The document presents, for the FAST group: the problem (per-root conflicts, blind-write merges, the extractor inside the commit, already listed in `docs/deviations.md:103-121,116,189,990`), the model (units, preconditions, intentions, the tables), what proto_db shipped and measured, what a protoBaseST port would touch (`transactions.st`, `collections.st`, `indexes.st`, `keygroups.st`, `vectors.st`, `avl.st`, `round.st`, `cluster/`; the build on the round-runner actor, so the pre-pass runs in the caller), the format-7 consequence (protoBaseST reads versions 2-4; reading a version-7 space needs formats 5-7), and the open questions for the group. No code changes.

- [ ] **Step 1: Write** the document (English, as the repo's docs).
- [ ] **Step 2: Commit** `docs: proposal, the precondition concurrency model for protoBaseST` and open a PR to protoBaseST main.

### Task 17: Final verification, merge and release 2.0.0rc5

**Files:**
- Modify: `pyproject.toml:7`, `proto_db/__init__.py:32` (`2.0.0rc5`), `CHANGELOG.md` (`[Unreleased]` → `[2.0.0rc5] - <date>`)

- [ ] **Step 1: Full suite** sequentially on 3.12 and on 3.14t if installed locally: `python -m pytest -q -rfEs`. Expected: PASS. Also `tests/perf` smoke if present in CI.
- [ ] **Step 2: Whole-branch review** by a fresh reviewer on the most capable model against the spec (P-A..P-G, R1-R70) and the review; fix findings; re-run the suite.
- [ ] **Step 3: Version bump** and CHANGELOG date; commit `Release 2.0.0rc5`.
- [ ] **Step 4: Push** the branch, open the PR to master, wait for the CI matrix (3.12, 3.13, 3.14, 3.14t) to pass.
- [ ] **Step 5: Merge** the PR to master (merge commit), and the protoBaseST proposal PR to its main.
- [ ] **Step 6: Release**: tag `v2.0.0rc5` on the merge commit, `gh release create v2.0.0rc5 --latest` (as rc4 was published: not marked prerelease) with the CHANGELOG section as notes; the `publish-to-pypi.yml` workflow publishes on release. Verify: the workflow run succeeds and `pip download proto_db==2.0.0rc5 --no-deps` fetches the wheel.
