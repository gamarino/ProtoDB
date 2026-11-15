# Runtime Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `DBObject`s with ordered parents (other `DBObject`s or registry-name strings), one resolution rule for
fields and methods (`o.name` and `o["name"]`), a per-database module list, and an in-memory registry of extension
chains with `_super`. Optional symbol references go in only if a measurement justifies them. Tests, documentation
examples and an extensive tutorial are included, and a release closes the plan.

**Architecture:** The persisted additions are:
- an optional `"p"` envelope key on atom records (format-5 feature `parents`);
- two registry roots, `__currentModules` and `__catalog`, written only by `proto_db.registry` and merged
  add-if-absent at commit.

Resolution is a C3 linearization over the parents, in which a registry name expands to its extension chain. It is
cached by pointers. Methods live only in imported Python modules; nothing language-specific is persisted.

**Tech Stack:** Python 3.12+ (3.14t free-threaded tested with the GIL on and off), msgpack, pytest, Sphinx.

**Spec:** `docs/superpowers/specs/2026-11-14-runtime-registry-design.md`. Read sections 1-6, 3a, 7-9, 13 and 14
before any task.

## Global Constraints

- **Base scheme unchanged.**
  - A `DBObject` without parents, in a database without modules, has today's API, bytes and behaviour.
- **A miss returns `None`**, through `o.x` and through `o["x"]`, with or without parents (maintainer, 2026-11-14:
  "return None is ok, no error, to make extension easier in time"). `"x" in o` and `has_defined_attr` tell absence
  from an explicit `None`.
  - Its records keep version 4.
- **Never persist a callable,** a Python class name or an import path for registry purposes (spec A1).
- **Parents are `DBObject`s or non-empty `str` only** (spec Q6). Writes never go to a parent (spec section 5).
- **Extensions define only plain functions (methods) and canonical data defaults.** Every other callable is refused
  at load with `ProtoValidationError` (spec Q4).
- **`self is o` inside a method.** `_super()` finds its position from the caller's code object (spec section 6).
- **A module-list mismatch raises; there is no automatic rebuild** (spec Q2).
- **Installing is additive and permanent.** There is no uninstall API.
- **Every *error* is explicit.** A refused definition, `_super` misuse, a mismatch, an unimportable module or an
  inconsistent hierarchy raises; it is never swallowed. A miss is not an error (see above).
- **Format additions are additive** (`format-spec.rst`, "Format stability"):
  - a v4 reader refuses v5 data with `ProtoNotSupportedError`;
  - the conformance vectors and the digest in `format-spec.rst` are regenerated with `python -m tests.conformance.generate`.
- **Machine rules (the maintainer's):**
  - Run heavy runs under `systemd-run --user --scope -p MemoryMax=12G -p MemorySwapMax=0`, one at a time; check
    `pgrep -af "pytest|benchmarks|burn"` first.
  - Temp dirs go under `/home/gamarino/Documentos/proyectos/scratch-8.3n/registry/<unique>/`, never `/tmp`.
  - **Never run `rm` or any cleanup.** List cleanups in the report.
  - The plant tool's `make` and `run` delete directories under their destination. Point them only at a fresh,
    unique directory under the scratch root, or use a runner of your own.
- **Interpreters:**
  - `.venv314t/bin/python` with `-X gil=0` and `-X gil=1`;
  - standard 3.14 at `/home/gamarino/Documentos/proyectos/scratch-8.3n/ci314/venv31410/bin/python`;
  - `.venv` (3.12).
- **Commits:**
  - Author Gustavo Marino <gamarino@gmail.com>, with the `GIT_AUTHOR_*` and `GIT_COMMITTER_*` variables set.
  - Stage explicit paths only, never `.junie/`.
  - End every message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
  - Work on branch `feat/runtime-registry`, in worktree
    `/home/gamarino/Documentos/proyectos/scratch-8.3n/wt-registry`. The base is the head of
    `fix/dict-merge-and-wal-durability`.
- **Language.** Code, comments and docs are in professional English. Name the product `proto_db` (the import name)
  and the repository protoBase. Never quote a number that the task did not measure.

## Rulings made while planning

- **R150, miss behaviour (maintainer, 2026-11-14).** A miss returns `None` in both forms, as `DBObject` does today.
  There are no not-found errors. This supersedes the spec's anti-goal A2 for misses only.
- **R151, segment versions.**
  - A record that uses a feature is appended only to a segment whose header version is at least the feature's
    version.
  - Otherwise the storage first rolls to a new segment written at the needed version.
  - A space that never uses a feature keeps writing v4.
  - Why: headers are written before their records (spec 3, R134 version rule).
- **R152, release version.**
  - The release that closes this plan is **2.0.0rc4**, together with the pending rc4 fixes.
  - The registry is additive, the format freeze allows additive features, and 2.0.0 is not final.
  - Cost if the maintainer prefers 2.1: a version string and a CHANGELOG heading.
- **R153, symbols.**
  - Task 10 measures first.
  - Task 11 implements symbol references only if Task 10 shows that they save a material share of bytes that
    segment compression does not. Otherwise Task 11 documents the deferral in the spec and the format spec, and
    implements nothing.
- **R154, polyglot notice.**
  - The format-5 additions are published in `format-spec.rst` with vectors.
  - The final report tells the maintainer to notify protoBaseST. Nothing is posted externally by the plan.

## Review Focus

These are the inputs most likely to bite a user that no task's happy path covers. Each is pinned by a test in the
task named.

1. **A parent that is an unsaved `DBObject`.** The child must save its parent first, once, and reload with the same
   linearization (Task 3).
2. **`"x" in o` against `o.x is None`.** `in` must tell an absent name from an explicit `None` anywhere along the
   linearization, own or inherited, data or method (Task 4).
3. **A registry method that calls `self._super()` from a nested function or a generator.** It must raise
   `ProtoUserError`, never resolve from the wrong place (Task 6).
4. **Opening a database whose `__currentModules` names a module the process cannot import.** It must raise
   `ProtoNotSupportedError` naming the module, at open or at first resolution, never at load of an unrelated
   object (Task 8).
5. **Two processes, or two threads, that install different modules concurrently.** Both installs must land, and
   neither commit fails (Task 7, run with the GIL off).

---

### Task 1: (removed)

Not-found errors are no longer needed (R150). The task numbers stay as they are, so the references between tasks
remain valid.

### Task 2: Feature-versioned segments (R151)

**Files:**
- Modify: `proto_db/storage/format.py`, each storage's segment writer (`memory.py`, `file.py`, `tiered.py`, `cloud.py`)
- Test: `tests/unit/test_format_features.py` (new)

**Interfaces:**
- Produces:
  - `FEATURE_VERSIONS = {"parents": 5}`;
  - `READABLE_FORMAT_VERSIONS` includes 5;
  - `FORMAT_VERSION = 5`, while `WRITE_FORMAT_VERSION` stays 4;
  - `SharedStorage.push_atom(payload, *, features=frozenset())`. The keyword is optional, and existing callers are
    unchanged.
- Rule: a storage appends a payload with features `F` only to a segment whose header version is at least
  `write_version(F)`. Otherwise it closes the current segment and opens a new one at that version. Root records
  that point into a v5 segment are written as v5 (see `decode_root`).

- [ ] Step 1: Write the failing tests, parametrized over `MemoryStorage`, `FileStorage`, `TieredStorage` and
  `CloudStorage` with `FakeObjectStore`:
  - `test_a_featureless_space_writes_only_v4_segments`;
  - `test_a_parents_record_lands_in_a_v5_segment` (the segment header read back is 5);
  - `test_a_v4_reader_refuses_the_v5_segment` (monkeypatch `READABLE_FORMAT_VERSIONS` to `{2, 3, 4}`, then
    `ProtoNotSupportedError`);
  - `test_reopen_reads_mixed_v4_and_v5_segments`.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement the roll and the version plumbing. Cluster: if records travel in replication messages,
  check that a v5 record's segment header travels with it. Raise `PROTOCOL_VERSION` only if the message bytes
  change, and record why either way in the task report.
- [ ] Step 4: Run the tests, then `tests/unit/test_format.py`, `tests/crash/`, `tests/conformance/`, plus the
  storage unit tests (`grep -l "segment" tests/unit`). Expect PASS.
- [ ] Step 5: Commit.

### Task 3: `DBObject` parents (persisted `"p"`)

**Files:**
- Modify: `proto_db/core/dbobject.py`, `proto_db/core/atom.py` (`_save_own` ~1019, `_load` ~857, `RESERVED_NAMES`),
  `proto_db/core/codec.py` (if a helper is needed)
- Test: `tests/unit/test_dbobject_parents.py` (new)

**Interfaces:**
- Consumes: `push_atom(..., features={"parents"})` from Task 2.
- Produces:
  - `DBObject(*, context=None, parents=(), **attributes)`;
  - property `o.parents -> tuple`;
  - `o.with_(**changes)`, which keeps the parents;
  - `o.with_parents(*parents) -> DBObject`;
  - `parents` is added to the reserved names.
- Envelope: `{"t","oid","s"}` without parents, unchanged. With parents it is `{"t","oid","s","p"}`, where `p` is the
  tuple encoded with `encode_value` (atoms as `@ref`, strings plain). `_load` accepts exactly these two key sets
  and refuses any other with `ProtoCorruptionError`.
- Validation: each parent is a `DBObject` or a non-empty `str` that passes `check_text`. Anything else raises
  `ProtoValidationError`. An object may not be its own parent (by oid).

- [ ] Step 1: Write the failing tests:
  - `test_no_parents_keeps_v4_bytes` (the encoded payload equals today's for the same oid and state);
  - `test_parents_round_trip_through_every_storage`;
  - `test_with_keeps_parents`;
  - `test_with_parents_replaces_them`;
  - `test_invalid_parent_types_are_refused` (int, None, a `MutableObject`, an empty string);
  - `test_unsaved_parent_is_saved_once_with_the_child` (Review Focus 1);
  - `test_parents_is_a_reserved_attribute_name`;
  - `test_a_record_with_unknown_envelope_keys_is_corrupt`.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests, then `tests/unit/test_atom.py`, `tests/unit/test_dbobject*.py`, `test_codec.py` and
  `test_polyglot.py`. Expect PASS.
- [ ] Step 5: Commit.

### Task 4: Resolution over object parents (no registry yet)

**Files:**
- Create: `proto_db/resolution.py`
- Modify: `proto_db/core/dbobject.py` (`__getattr__` path, `__getitem__`, `__contains__`, `_own`, `_resolve`,
  `_linearization`)
- Test: `tests/unit/test_resolution.py` (new)

**Interfaces:**
- Produces:
  - `linearize(obj, registry=None) -> tuple[Node, ...]` (C3; a node is a `DBObject` or a registry-name node);
  - `resolve(obj, name, registry=None) -> (value, position)`, which returns `(None, None)` on a miss;
  - `o["name"]` accepts a `str`; `o.name` and `o["name"]` return `None` on a miss (R150);
  - `"name" in o`;
  - `o._own -> FrozenDict`;
  - `o._linearization()`.
- C3 failure (an inconsistent hierarchy) raises `ProtoValidationError` naming the nodes.
- Cache: the linearization is keyed by `(type(o), tuple of parent pointers or names)` when every parent is saved;
  otherwise it is cached on the transaction for that tuple object.
- A string parent with no registry available raises `ProtoNotSupportedError` ("registry name X needs a registry").

- [ ] Step 1: Write the failing tests:
  - `test_own_then_parents_in_c3_order`;
  - `test_diamond_visits_each_node_once`;
  - `test_inconsistent_hierarchy_raises`;
  - `test_item_and_attribute_agree` (own, parent, computed name);
  - `test_miss_returns_none_in_both_forms` (with and without parents);
  - `test_in_tells_absent_from_explicit_none` (own, inherited, method; Review Focus 2);
  - `test_writes_never_reach_a_parent`;
  - `test_python_subclass_members_come_first`;
  - `test_linearization_is_cached_by_pointer` (count `linearize` calls);
  - `test_cost_does_not_depend_on_history_depth` (100 versus 1,000 superseded versions: equal frame counts, a
    structural count, not a timing).
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests, then every `tests/unit/test_dbobject*.py`, `tests/docs/test_tutorials.py` (the existing
  tutorials must be unchanged) and `tests/integration/test_examples.py`. Expect PASS.
- [ ] Step 5: Commit.

### Task 5: The registry and modules (in memory)

**Files:**
- Create: `proto_db/registry.py`
- Test: `tests/unit/test_registry_modules.py` (new), plus test modules under `tests/unit/registry_modules/`
  (`base`-like fixtures: `reg_base`, `reg_contacts`, `reg_crm`, `reg_cycle_a`/`reg_cycle_b`, `reg_bad_property`)

**Interfaces:**
- Produces:
  - `module(name: str, depends: tuple[str, ...] = ()) -> ModuleHandle`;
  - `ModuleHandle.extends(registry_name: str, *, parents: tuple[str, ...] = ())`, a class decorator;
  - `Registry.build(module_names: tuple[str, ...], *, mapping: Mapping[str, str] | None = None, allowed: Container[str] | None = None) -> Registry`;
  - `Registry.chain(registry_name) -> tuple[Extension, ...]` (newest first);
  - `Registry.parents_of(registry_name) -> tuple[str, ...]` (the union in load order);
  - `Registry.extension_of_code(code) -> Extension | None`;
  - `Registry.key -> tuple[str, ...]`.
- Load order: topological, with ties broken by the position in `module_names` (installation order). A cycle raises
  `ProtoValidationError` naming the cycle.
- Definitions:
  - plain functions are methods; everything else passes through `canonical()`;
  - `property`, `staticmethod`, `classmethod`, callable instances and lambdas are refused;
  - names starting with `_`, `RESERVED_NAMES` and the `DBObject` API names are refused;
  - one function object in two extensions is refused.
- The import uses `importlib.import_module(mapping.get(name, name))`. On failure it raises `ProtoNotSupportedError`
  naming the module. A module outside `allowed` raises `ProtoValidationError`.

- [ ] Step 1: Write the failing tests:
  - `test_topological_order_with_install_order_ties`;
  - `test_cycle_is_refused`;
  - `test_chain_is_newest_first`;
  - `test_registry_name_parents_union`;
  - `test_refused_definitions` (each kind);
  - `test_reused_function_is_refused`;
  - `test_unimportable_module_names_it`;
  - `test_module_outside_allowed_is_refused`;
  - `test_data_defaults_are_canonical`.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests. Expect PASS.
- [ ] Step 5: Commit.

### Task 6: Registry resolution, binding and `_super`

**Files:**
- Modify: `proto_db/resolution.py`, `proto_db/core/dbobject.py`
- Test: `tests/unit/test_registry_resolution.py` (new)

**Interfaces:**
- Consumes: `Registry` from Task 5.
- Produces:
  - registry-name nodes in `linearize`; a method resolves to `types.MethodType(fn, o)`;
  - `o._super() -> SuperProxy`;
  - a module may not define, in a registry name, a member that the `DBObject` subclass of a resolved object
    defines in Python: this raises `ProtoValidationError` at first resolution.
- The registry for an object comes from its transaction (Task 8). Until then, tests pass it with
  `proto_db.registry.using(registry)`, a context manager used only in tests.

- [ ] Step 1: Write the failing tests:
  - `test_method_and_field_resolve_alike` (`o.win(o.stage)`);
  - `test_self_is_o`;
  - `test_super_walks_three_modules_once_in_load_order`;
  - `test_late_binding_inside_methods`;
  - `test_catalog_object_before_its_registry_name` (spec Q1: a plain node);
  - `test_data_default_overridden_by_later_module_and_by_instance`;
  - `test_super_outside_a_method_raises`, `test_super_from_closure_raises` and `test_super_from_generator_raises`
    (Review Focus 3);
  - `test_super_on_foreign_object_raises`;
  - `test_registry_cannot_shadow_a_python_member`.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests and Task 4's tests. Expect PASS.
- [ ] Step 5: Commit.

### Task 7: Registry roots, install and add-if-absent commit

**Files:**
- Modify: `proto_db/transactions.py` (`_build_root` ~2279, internal `__` write path), `proto_db/registry.py`
- Create: built-in atom types `proto_db.registry.ModuleList` and `proto_db.registry.Catalog`, registered in
  `BUILTIN_ATOM_MODULES`
- Test: `tests/unit/test_registry_roots.py` (new), `tests/concurrency/test_registry_install_concurrency.py` (new,
  `freethreaded` marker)

**Interfaces:**
- Produces:
  - `Transaction.install_module(name: str) -> None`. It imports the module to learn its `depends`, adds the
    missing dependencies first, and checks the runtime's `allowed` (`ObjectSpace(..., modules_allowed=...)`).
  - `Transaction.installed_modules() -> tuple[str, ...]`. It reads without entering the read set.
  - `Transaction.catalog_object(registry_name) -> DBObject | None`.
- Commit: in `_build_root`, `__currentModules` and `__catalog` merge add-if-absent against the current root (never
  the read set, never last-writer-wins). Order is commit order.
- A `__` root whose value is not of the registry type raises `ProtoCorruptionError` on read.
- `set_root_object("__...")` stays refused for applications.

- [ ] Step 1: Write the failing tests:
  - `test_install_adds_module_and_deps`;
  - `test_install_is_idempotent`;
  - `test_concurrent_installs_both_land` (two transactions with the same base: neither conflicts, and the union is
    present);
  - `test_reading_modules_does_not_conflict_with_ordinary_writes`;
  - `test_wrong_typed_registry_root_is_corrupt`;
  - `test_applications_still_cannot_write_dunder_roots`;
  - `test_catalog_object_created_for_new_registry_names`;
  - the concurrency test: 8 threads, 4 modules, both GIL modes; every install lands and there are 0 conflicts
    (Review Focus 5).
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests, then `tests/unit/test_transaction*.py`, the D33 merge tests (`grep -l rebase_onto tests`)
  and `tests/concurrency/test_cloud_concurrency.py`. Expect PASS.
- [ ] Step 5: Commit.

### Task 8: Registry per database: open, consistency and mismatch

**Files:**
- Modify: `proto_db/transactions.py` (ObjectSpace / Database / Transaction), `proto_db/registry.py`
- Test: `tests/unit/test_registry_database.py` (new)

**Interfaces:**
- Produces:
  - `ObjectSpace(storage, *, modules_mapping=None, modules_allowed=None, ...)`;
  - `Transaction.registry -> Registry`, built and cached per process by `Registry.key`, the content of the module
    list;
  - a transaction that installs a module resolves through a registry built for its own list.
- Mismatch: a transaction whose `__currentModules` differs from the registry already bound to that database in
  this process raises `ProtoValidationError` ("registry built for modules ... but the database lists ..."). Spec Q2:
  there is no automatic rebuild.
  - `ObjectSpace.reload_registry(database_name)` rebuilds explicitly.
  - An as-of read of an older version resolves with the registry of the list current in that version.

- [ ] Step 1: Write the failing tests:
  - `test_registry_built_from_the_database_list`;
  - `test_mismatch_raises_after_a_foreign_install` (two spaces over one storage: one installs, the other's next
    transaction raises);
  - `test_reload_registry_clears_the_mismatch`;
  - `test_unimportable_listed_module_raises_at_first_resolution` (Review Focus 4: loading an unrelated plain object
    still works);
  - `test_as_of_read_uses_that_version_list`;
  - `test_installing_transaction_sees_its_module`.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests and Tasks 5-7. Expect PASS.
- [ ] Step 5: Commit.

### Task 9: The `base` module, data files, and plants

**Files:**
- Create: `proto_db/modules/base.py` (registry module `base`: default mechanisms and `read_data_file(self, path)`,
  which reads the file when not overridden)
- Modify: `tests/support/plant_defects.py`
- Test: `tests/unit/test_base_module.py` (new)

- [ ] Step 1: Write the failing tests:
  - `test_base_is_installed_first_and_has_no_deps`;
  - `test_read_data_file_default_reads_the_file`;
  - `test_a_module_overrides_read_data_file_and_supers`.
- [ ] Step 2: Implement until they pass.
- [ ] Step 3: Add the plants. Each targets the tests that must catch it:
  - `in_ignores_parents` (`"x" in o` checks only own attributes);
  - `c3_drops_duplicate_check`;
  - `super_uses_start_position`;
  - `refusal_of_property_removed`;
  - `install_last_writer_wins`;
  - `modules_read_enter_read_set`;
  - `mismatch_not_checked`;
  - `parents_bytes_for_parentless` (a parentless object writes `"p"`);
  - `v4_segment_accepts_parents`.

  Run every plant in both GIL modes with a runner that never deletes, or under a fresh unique scratch dir. Each
  must fail as planted.
- [ ] Step 4: Commit.

### Task 10: Measure symbol references (R153)

**Files:**
- Create: `benchmarks/symbol_bytes.py`, self-reporting per the benchmark rules: it prints the work done and
  verifies it.

- [ ] Step 1: Build the ERP example's data: the Task 12 tutorial's models, scaled so there are at least 10,000
  objects.
- [ ] Step 2: Write it four ways and report the bytes of each:
  - (a) inline strings;
  - (b) a simulated content-id reference for every string at least `L` long, for `L` = 8, 16 and 24;
  - (c) zstd or zlib compression of the segments, if available in the standard library or the existing
    dependencies;
  - (d) (b) plus (c).
- [ ] Step 3: Write `docs/superpowers/2026-11-14-symbol-measurement.md` with the table and the verdict, using R153's
  criterion. The maintainer's rule: use symbols only where they pay.
- [ ] Step 4: Commit.

### Task 11: Symbol references, or their documented deferral (R153)

- If Task 10 justifies them:
  - implement spec 3a with the content-hash id: `__currentSymbols` add-if-absent, writer policy, expansion before
    tokens, keys and hashes, and the D58 amendment;
  - tests: inline and referenced copies are one key and one token, a planted hash collision conflicts and is
    retried inline, an aborted transaction leaves no entry;
  - add plants;
  - add the format-5 feature `symbols`.
- Otherwise: update spec 3a and `format-spec.rst` to record the measurement and the deferral. Leave
  `__currentSymbols` unreserved for now, but keep it reserved by the `__` rule.
- Commit.

### Task 12: Documentation and an extensive tutorial

**Files:**
- Create:
  - `docs/source/tutorials/extensible-models.rst` (executed by `tests/docs/test_tutorials.py`: `python` blocks in
    one namespace, with `Output` captions compared);
  - `docs/source/design-guide/registry.rst`.
- Modify:
  - `docs/source/tutorials/index.rst` and `docs/source/design-guide.rst` (toctrees);
  - `docs/source/api.rst` (registry API);
  - `docs/source/format-spec.rst` (the `"p"` key, feature `parents` at v5, the registry roots by name and type, the
    regenerated vectors and digest);
  - `docs/decisions.md` (D62: the runtime registry, with rulings R150-R154 and the spec's Q1-Q7 decisions);
  - `README.md` (a short tested example);
  - `CHANGELOG.md` (rc4).

The tutorial is extensive. In this order, it covers:
1. plain data objects, which are unchanged;
2. parents between objects, with `o.x` and `o["x"]` and a computed name; a miss returns `None`, and `in` tells
   absence from an explicit `None`;
3. the first registry module: a module file written by the tutorial into its cwd and imported;
4. `install_module` and the catalog class object;
5. a second module extending the first, with `_super`;
6. data defaults overridden by a module and by an instance;
7. a diamond of registry names and C3;
8. the errors (each one shown with its `Output`):
   - `_super` outside a method;
   - a refused definition;
   - a mismatch after another space installs;
   - an unimportable module;
9. as-of reads with the module list of that time;
10. the versioned-entity pattern (`MutableObject` plus a chain of versions), with the point that current indexes
    hold no dead versions, so queries need no is-current filter;
11. what is never persisted (A1), and the multi-language rule.

Each section explains the reasoning, for the course material: the obvious solution, why it fails, and the
alternative.

- [ ] Step 1: Write the tutorial and the docs. Run `python tests/docs/test_tutorials.py --update docs/source/tutorials/extensible-models.rst`
  once to fill in the `Output` blocks, then read every output and check it by hand.
- [ ] Step 2: Run `pytest tests/docs -q` and `tests/integration/test_examples.py`. Run `sphinx-build -W` as CI does
  (see `.github/workflows/tests.yml`). Run the conformance test. Expect PASS.
- [ ] Step 3: Commit.

### Task 13: Verification, final review and release

- [ ] Step 1: Prerequisites on `fix/dict-merge-and-wal-durability` (outside this plan, tracked in the v2 ledger):
  - P1-READ closed and merged;
  - RENEW-BACKOFF (issue #1) merged;
  - issue #1 updated with the commit.
- [ ] Step 2: Merge `feat/runtime-registry` into the branch (`--no-ff`).
- [ ] Step 3: Run the full suites one at a time, under the machine rules: 3.14t with the GIL off, 3.14t with the GIL
  on, standard 3.14 and 3.12. Push. Run CI on all four versions plus the docs, twice green.
- [ ] Step 4: Run a whole-branch Opus review of the registry diff. Do one fix dispatch and one scoped re-review.
- [ ] Step 5: Release 2.0.0rc4, with the maintainer's authorization of 2026-11-14 ("publicar al final"):
  1. Date the CHANGELOG.
  2. Merge to master via the worktree `scratch-8.3n/wt-master` (`--no-ff`).
  3. Tag `v2.0.0rc4` and push it.
  4. Run `gh release create v2.0.0rc4` without `--prerelease`.
  5. Watch `publish-to-pypi.yml`.
  6. Check `pip install --pre proto_db==2.0.0rc4` in a fresh scratch venv: run the README example and one
     tutorial snippet.
- [ ] Step 6: Final report to the maintainer, in Spanish. It covers what shipped, the measurements, the cleanup list
  awaiting approval, and the protoBaseST notice (R154).
