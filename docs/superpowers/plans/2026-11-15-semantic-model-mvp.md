# Semantic Model MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The semantic-model demo script runs end to end on proto_db: a format-6 semantic catalog composed from
registry modules, descriptors and relations with inverses, an MCP agent interface, intents and invariants behind a
flag on one node, the two closing experiments, and the handoff that lets protoBaseST run step 6.

**Architecture:** Entities are declared as the brief writes them: `DBObject` subclasses with `__type_name__` and
descriptors, extended by `Extension` subclasses. At class creation their semantic elements become D62 extensions
of the registry name `__type_name__`, with a new definition kind, *descriptor*. Instances resolve that name as an
implicit last parent (spec section 4). The composed catalog is a pure function of the registry, stored add-if-absent at install in the root
`__semanticCatalog` (feature `catalog`, format 6) and verified against the code on use. A `SemanticSession`
over a transaction or a snapshot gives one API in two modes. In direct mode, operations write cells at once
under today's rules. In intent mode, operations are logged, and `_build_root` validates a fine read set,
replays the log on the current state, maintains aggregates, checks invariants and writes `__intentLog`.

**Tech Stack:** Python 3.12-3.14 and 3.14t (free-threaded, GIL on and off), msgpack, pytest, Sphinx; the MCP
Python SDK as a new optional extra.

**Spec:** `docs/superpowers/specs/2026-11-15-semantic-model-mvp-design.md`. Read it whole before any task. The
brief (`/home/gamarino/Documentos/proyectos/scratch-8.3n/semplan/brief.md`) is the design of record for anything
the spec defers. Read D57, D59 and D62 in `docs/decisions.md`, and the "Format stability" and "Registry roots"
sections of `docs/source/format-spec.rst`.

## Global Constraints

- **Interpreters:** Python 3.12-3.14 and 3.14t. Locally:
  - `.venv314t/bin/python` with `-X gil=0` and with `-X gil=1`;
  - `/home/gamarino/Documentos/proyectos/scratch-8.3n/ci314/venv31410/bin/python` (3.14.10);
  - `.venv/bin/python` (3.14.4).

  CI covers 3.12, 3.13, 3.14 and 3.14t (`.github/workflows/tests.yml`).
- **No semantic layer, no change.** Code that does not use the semantic layer keeps today's API, bytes and
  behaviour:
  - format-4 and format-5 records stay byte for byte;
  - `WRITE_FORMAT_VERSION` stays 4;
  - the full existing suite passes unchanged;
  - `Database.run` and `new_transaction` without `intents=True` behave exactly as today.
- **Safety never depends on timing (P5, R82).** Validation and replay happen at the serialized point and never
  wait for or rely on a duration.
- **No conceptual debt** (design principles). A limit of the MVP is a refusal with an explicit error, never a
  silent approximation.
- **Vectors are the cross-language acceptance.** Every normative item (catalog records, canonical form,
  expression evaluation) has a vector. Regenerate with `python -m tests.conformance.generate` and update the
  pinned digest in `format-spec.rst` in the same commit.
- **Language and naming.**
  - Professional English in code, comments and docs.
  - The product is `proto_db` and the repository is protoBase.
  - Never quote a number that the task did not measure.
- **Tests** go under the existing layout: `tests/unit`, `tests/concurrency` (`freethreaded` marker),
  `tests/conformance`, `tests/integration`, `tests/docs`.
- **Machine rules.**
  - Run any run longer than a few files under `systemd-run --user --scope -p MemoryMax=12G -p MemorySwapMax=0`,
    one at a time. Check `pgrep -af "pytest|benchmarks"` first.
  - Temp dirs go under `/home/gamarino/Documentos/proyectos/scratch-8.3n/semantic/<unique>/` (`$SCR` below),
    never `/tmp`. Pytest always gets `--basetemp=$SCR/pt`.
  - **Never run `rm`** or any cleanup. List cleanups in the task report.
- **Commits.**
  - Author and committer Gustavo Marino <gamarino@gmail.com> (set `GIT_AUTHOR_*` and `GIT_COMMITTER_*`).
  - Stage explicit paths only, never `.junie/`.
  - End every message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
  - Work on branch `feat/semantic-mvp`, in worktree `/home/gamarino/Documentos/proyectos/scratch-8.3n/wt-semantic`.
    The base is the head of `fix/dict-merge-and-wal-durability` (2.0.0rc4).
- **Proposed rulings.** Spec section 17 (R-S1 to R-S22) is pending the maintainer. Implement as written. A ruling
  that changes on review is applied to the task that owns it, and the change is noted in its report. Confirmed
  rulings are recorded as R258 onwards.

## Review Focus

1. **A database written by rc4** (modules installed, no `__semanticCatalog`), opened by this code. Plain
   resolution must work as before. The semantic session must raise `ProtoNotSupportedError` ("no semantic
   catalog"), and the next `install_module` must add the contributions (Task 5).
2. **An as-of read** must describe entities with the catalog of that version, not the current one (Task 5).
3. **A credit violation inside `Database.run(fn, intents=True)`.** It must raise `ProtoConstraintError` once, never
   retried, with nothing published. In the same group-commit round, the other commits must publish (Task 9).
4. **A customer record without `credit_limit`** (written raw, or before the field became required). The
   constraint must report a violation that names the missing operand, never a `TypeError` (Task 9).
5. **Two transactions that install semantic modules concurrently.** Both must land. The stored composition must be
   the composition of the union, never last-writer-wins (Task 5).

---

### Task 1: Decisions D63-D72, principles S1-S7, and the draft catalog chapter

**Files:**
- Modify:
  - `docs/decisions.md`: append D63-D72 in the existing entry format (Context / Decision / rulings), with the
    contents of spec section 13. D69 and D72 are marked "designed; implementation deferred (MVP scope)".
  - `docs/design-principles.md`: a section "Semantic model (S1-S7)" after P7, one paragraph each.
  - `docs/source/format-spec.rst`: a new section "Semantic catalog (format 6)" after "Registry roots", holding
    spec sections 7 and 8 and the `__intentLog` root. Mark it "**Draft: no released writer produces it yet**". It
    is listed in "What is normative".
- Test: `sphinx-build -W` (as CI).

- [ ] Step 1: Write the three documents. Each decision cites the principles it relies on.
- [ ] Step 2: Run `.venv/bin/python -m sphinx -W -b html docs/source $SCR/html`. Expect a clean build. Run
  `pytest tests/conformance/test_conformance.py -q --basetemp=$SCR/pt`. Expect PASS: the pinned digest is
  unchanged, since no vector changed.
- [ ] Step 3: Commit "Docs: semantic model decisions D63-D72, principles S1-S7, draft catalog chapter".

### Task 2: Format 6, the `catalog` feature, catalog atom types and their vectors

**Files:**
- Modify:
  - `proto_db/storage/format.py`: `FORMAT_VERSION = 6`, `READABLE_FORMAT_VERSIONS = {2, 3, 4, 5, 6}`,
    `FEATURE_VERSIONS["catalog"] = 6`, `CATALOG_TYPES` (name -> alias), and `payload_features` detecting catalog
    tags.
  - `proto_db/core/atom.py`: `BUILTIN_TYPE_ALIASES` gains, appended in this order, aliases 24-34:
    `proto_db.catalog.SemanticCatalog`, `Module`, `Extension`, `Entity`, `Relationship`, `Attribute`, `Role`,
    `Intent`, `Constraint`, `Expression`, `IntentRecord`. `BUILTIN_ATOM_MODULES` maps each to
    `proto_db.catalog.atoms`.
  - `proto_db/transactions.py`: an internal write path for `__semanticCatalog` and `__intentLog`, used only by
    `proto_db.semantic`. Application writes of `__` roots stay refused.
- Create:
  - `proto_db/catalog/__init__.py`;
  - `proto_db/catalog/atoms.py`: the eleven types, `_Immutable` atoms with `_format_features = {"catalog"}`;
  - `proto_db/catalog/model.py`: the neutral model and its conversions;
  - `tests/conformance/catalog_model.json`: the neutral model of `conformance.sales.Customer`, `Order` and
    `OrderLine`, after brief section 8, with two modules `conformance.base` and `conformance.sales`.
- Test:
  - `tests/unit/test_catalog_atoms.py` (new);
  - `tests/conformance/generate.py` (`gen_catalog_database`) and `test_conformance.py`;
  - `tests/conformance/README.md`: a new step 13, `catalog_database`.

**Interfaces:**
- Produces:
  - `proto_db.catalog.model.check_model(model: dict) -> None`. It raises `ProtoValidationError` naming the path of
    the first invalid item. The model is `{"module_set": [str], "modules": {name: module}, "entities": {type_name:
    entity}}`, whose items use exactly the field names of spec section 8.
  - `to_atoms(model: dict, context) -> SemanticCatalog`, which writes defaults as absent fields.
  - `from_atoms(catalog: SemanticCatalog) -> dict`.
  - `canonical_form(model: dict) -> bytes` (R-S1): msgpack of `encode_value` of the expanded model. Maps are
    sorted by key in code-point order and contain no atoms.
  - `proto_db.catalog.atoms.SEMANTIC_CATALOG_ROOT = "__semanticCatalog"`, `INTENT_LOG_ROOT = "__intentLog"`.
  - `Transaction._write_semantic_root(name: str, value) -> None` (internal).
  - A `__` root of the wrong type raises `ProtoCorruptionError` on read, as for the registry roots.

- [ ] Step 1: Write the failing tests:
  - `test_catalog_types_are_appended_aliases_24_to_34`;
  - `test_a_catalog_record_lands_in_a_v6_segment` (header read back is 6);
  - `test_a_space_without_catalog_keeps_v4_and_v5_bytes` (the existing parents and registry vectors regenerate
    identically);
  - `test_a_v5_reader_refuses_a_v6_segment` (monkeypatch `READABLE_FORMAT_VERSIONS` to `{2, 3, 4, 5}`, then
    `ProtoNotSupportedError`);
  - `test_model_round_trips_through_atoms` (`from_atoms(to_atoms(m)) == m`);
  - `test_defaults_are_not_written`;
  - `test_ext_is_kept_byte_for_byte`;
  - `test_canonical_form_ignores_oids_and_placement` (one model written in two spaces gives equal bytes);
  - `test_wrong_typed_semantic_root_is_corrupt`;
  - `test_applications_cannot_write_semantic_roots`.
- [ ] Step 2: Run `pytest tests/unit/test_catalog_atoms.py -q --basetemp=$SCR/pt`. Expect FAIL (no module
  `proto_db.catalog`).
- [ ] Step 3: Implement. Check whether any cluster message bytes change. Raise `PROTOCOL_VERSION` only if they do,
  and record why in the report either way.
- [ ] Step 4: Add `gen_catalog_database` to the generator: a space with a format-4 segment, then a format-6
  segment holding `to_atoms(catalog_model.json)` as `__semanticCatalog`, and `vectors/catalog/canonical.msgpack`.
  Manifest key `catalog_database`, with `segments`, `canonical`, and contents in the notation. A catalog record is
  described as `{"catalog": type, "attrs": [...]}`; document it in the README. Add the tests
  `test_catalog_database_segments`, `test_catalog_database_contents_through_the_public_api` and
  `test_catalog_canonical_form_vector`.
- [ ] Step 5: Run `python -m tests.conformance.generate`. Update the digest in `format-spec.rst` and the manifest
  version (`format: 6`). Remove the Task 1 draft mark, and add a "Format history" entry for 6. Run
  `pytest tests/conformance tests/unit/test_format*.py tests/unit/test_type_names.py tests/unit/test_atom_type_modules.py -q --basetemp=$SCR/pt`
  and `PROTODB_REGEN_VECTORS=1 pytest tests/conformance/test_regeneration.py --basetemp=$SCR/pt`. Expect PASS.
- [ ] Step 6: Commit "Format 6: the catalog feature and its atom types, with vectors".

### Task 3: The expression language (MVP subset) and its evaluation vectors

**Files:**
- Create: `proto_db/semantic/__init__.py` (exports filled in by later tasks), `proto_db/semantic/expressions.py`.
- Modify: `tests/conformance/generate.py` (`gen_expressions`: manifest key `expressions`),
  `tests/conformance/test_conformance.py`, `tests/conformance/README.md` (step 14).
- Test: `tests/unit/test_semantic_expressions.py` (new).

**Interfaces:**
- Produces:
  - `check_ast(ast, *, in_query: bool = False) -> None`. It raises `ProtoValidationError` naming the operator.
    `root` and `cell` are allowed only with `in_query=True`, and any operator outside spec section 7 is refused.
  - `evaluate(ast, element, *, reader=None)`. It returns a canonical value, `MISSING`, or a tuple of members (for
    `via`).
  - `reader` exposes `cell(name) -> DBObject | None` and `root(name)`. It dereferences identity handles. Without a
    reader, reaching a handle raises `ProtoUserError`.
  - `to_planner(ast) -> proto_db.query.expressions.Expression | None`, mapping `==, !=, <, <=, >, >=` and
    `and/or/not` of `[cmp, ["attr", name], ["const", v]]` to `Term`/`And`/`Or`/`Not`; `None` otherwise.
  - `dependencies(ast) -> frozenset[str]`: the attribute names and roles an expression reads from its element.
    Task 6 uses it for maintenance, Task 9 for invariants.
  - `parse(text: str) -> list` (R-S20), the Python text form of spec section 7. Anything outside the subset (`in`,
    function calls, `/`) raises `ProtoValidationError` naming the column. It is a hand-written tokenizer and
    recursive-descent parser, not `ast.parse`/`eval`: the grammar is the spec's, not Python's.
  - `sum` and `count` take an optional `where` operand.
- Vector entry: `{"id", "ast", "element": <stored-object description>, "expected": <value description> | {"error":
  "validation"}}`. At least:
  - each operator;
  - int + decimal, and decimal * int;
  - MISSING through arithmetic and through comparison;
  - `sum` of nothing = 0, `sum` with a `where`, and `count`;
  - `via` over two hops;
  - a float operand refused, and division refused.

  The text form is Python-only and has no vectors; its tests compare `parse` results with trees.

- [ ] Step 1: Write the failing tests:
  - `test_each_operator` (parametrized from a local table);
  - `test_int_and_decimal_arithmetic_is_exact`;
  - `test_missing_propagates_and_comparisons_are_false`;
  - `test_float_and_division_are_refused` (R-S7);
  - `test_root_and_cell_only_in_queries` (R-S8);
  - `test_to_planner_translates_simple_predicates` (`["<", ["attr", "date"], ["const", d]]` gives
    `Term("date", "<", d)`);
  - `test_to_planner_returns_none_for_two_attributes`;
  - `test_dependencies_of_sum_over_a_role`;
  - `test_parse_the_briefs_texts` (`parse("committed <= credit_limit") == ["<=", ["attr", "committed"], ["attr",
    "credit_limit"]]`, `parse("state == 'open'")`, `parse("quantity * price")`);
  - `test_parse_refuses_in_and_calls` (`parse("state in ('open','confirmed')")` raises `ProtoValidationError`).
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement. Comparison uses `values_equal`, `order_key` and the rank rule of `proto_db.query.terms`
  (D28); reuse its helpers rather than reimplementing them.
- [ ] Step 4: Add the `expressions` vectors and `test_expression_vector` (parametrized over the manifest).
  Regenerate and update the digest. Run the conformance and unit tests. Expect PASS.
- [ ] Step 5: Commit "Semantic layer: the expression language subset, with evaluation vectors".

### Task 4: The brief's declarations on rc4: descriptors, semantic classes, `Extension`, composition, `describe()`

**Files:**
- Create:
  - `proto_db/semantic/descriptors.py`;
  - `proto_db/semantic/classes.py` (the semantic-class hook, `Extension`, `Relationship`);
  - `proto_db/semantic/compose.py`;
  - the demo modules `docs/kit/semantic/acme_sales.py` and `docs/kit/semantic/acme_delivery.py`. They follow spec
    section 3, the brief's blocks 1, 2, 4, 6, 7 and 8, with English text. Tests import them through a
    `modules_mapping`.
  - `tests/conformance/catalog_fixture.py`: modules `conformance.base` and `conformance.sales`, declaring the model
    of `catalog_model.json` in the brief's spelling (`class Customer(DBObject)`, block 8).
- Modify:
  - `proto_db/core/dbobject.py`: `DBObject.__init_subclass__` calls `proto_db.semantic.classes.semantic_class(cls)`
    (imported lazily) only when the class body holds a semantic element. Other classes are untouched.
  - `proto_db/registry.py`:
    - `DESCRIPTOR = "descriptor"` in `_definition`;
    - the semantic dunders (`__meaning__`, `__label__`, `__help__`, `__synonyms__`, `__key__`,
      `__certification__`, `__constraints__`) collected into a new read-only `Extension.semantic` slot, carried by
      `_Declared`;
    - an internal `ModuleHandle._extend_with(registry_name, definitions, semantic, qualname, parents=())` for
      `Extension` subclasses and semantic classes. It skips the container-class checks the semantic layer has
      already made; the caller-module check is kept.
  - `proto_db/resolution.py`:
    - a descriptor definition resolves with `definition.resolve(receiver)`;
    - an instance of a semantic class has `type(o).__type_name__` as an implicit last registry-name parent, never
      persisted;
    - an `@intent` function binds to an invoker (Task 8 fills in its logging; until then it calls the function
      under a private context that records nothing).
- Test:
  - `tests/unit/test_semantic_descriptors.py` (new);
  - `tests/unit/test_semantic_classes.py` (new);
  - `tests/unit/test_semantic_compose.py` (new);
  - `tests/conformance/test_conformance.py::test_declared_fixture_composes_to_the_catalog_vector` (the central
    conformance test).

**Interfaces:**
- Produces, in `proto_db.semantic`. The brief's spellings are kept exactly where it shows them.
  - `Field(value_type, *, required=False, default=MISSING, unit=None, indexed=False, stored_as=None, **semantic)`.
    `value_type` is a Python type (`int`, `Decimal`, `str`, `bool`, `bytes`, `float`, `date`, `time`, `datetime`,
    `timedelta`, `uuid.UUID`) or `Enum(*values)`, mapped as in R-S11.
  - `Enum(*values: str)`.
  - `Computed(expression, *, unit=None, **semantic)`. `expression` is text (parsed, R-S20), a tree, or a callable
    (Python-only, never stored, R-S19).
  - `Aggregate(*, over: str, sum: str | None = None, count: bool = False, where: str | list | None = None, maintained: bool = False, unit=None, **semantic)`:
    `["sum", over, ["attr", sum], where]` or `["count", over, where]`; maintained gives `computed_stored`.
  - `Relation(target: str, *, cardinality: str, inverse: str | None = None, ordered=False, index_by=(), binding="identity", on_delete="restrict", **semantic)`.
    `cardinality` accepts the relation spellings (`many-to-one`, `one-to-many`, `one-to-one`, `many-to-many`) and
    the end spellings (`one`, `many`, `0..1`, `1..*`) (spec section 5).
  - `RelationTo(target: type | str, *, role: str, ordered=False, **semantic)`, the inverse end of `target`'s role.
  - `Role(target: str, *, inverse: str | None = None, ordered=False, index_by=(), binding="identity", on_delete="restrict", **semantic)`.
  - `Relationship` (a `DBObject` base class without a type name) and `Extension` (a base class whose subclasses
    declare `__extends__`, and optionally `__parents__` and `__registry_module__`).
  - `Invariant(expression, *, meaning: str, enforced: bool = True, message: str = "", name: str | None = None)`,
    whose unnamed form is named by R-S22.
  - `intent(*, meaning: str, requires=None, commutes_with=(), parameters: Mapping[str, str] | None = None)`.
    It returns the same function with `__proto_intent__` set. Parameters are taken from the signature, and their
    types from annotations or `parameters`.
  - `intent_context() -> IntentContext`; outside a running intent it raises `ProtoUserError`.
  - `Certified(by: str, on: str) -> dict`.
  - `**semantic` keys: `meaning`, `label` (str or dict, R-S10), `help`, `group`, `order`, `synonyms`, `examples`,
    `caveats`, `sensitivity` (`public`, `internal`, `personal`, `secret`; default `internal`), `certification`,
    `deprecated`, `ext`. An unknown key raises `ProtoValidationError`.
  - `<SemanticClass>.describe(source) -> dict`, a class method injected into semantic classes only. Without a
    source it raises `ProtoUserError`. Task 5 makes it read the database's composition; this task's tests use
    `compose.describe`.
- Produces, in `proto_db.semantic.compose`:
  - `compose(registry: Registry) -> dict`, the model of Task 2. It folds each registry name's chain oldest first,
    applies spec section 5 (restated structural fields must be equal), creates the inverses and records provenance.
  - `module_contributions(registry: Registry, module_name: str) -> dict`.
  - `describe(model: dict, type_name: str) -> dict`.
  - Refusals are `ProtoValidationError` naming module, type, element and field.
- Class-creation refusals:
  - a semantic class whose Python module declared no registry module, or several without `__registry_module__`;
  - a semantic subclass of a semantic class (`ProtoNotSupportedError`, spec section 4);
  - `__validity__` (`ProtoNotSupportedError`, D72);
  - a stored `Aggregate` whose terms are not neutral.

- [ ] Step 1: Write the failing tests:
  - `test_a_class_without_semantic_elements_is_untouched` (same `__dict__` keys, same records as on the base
    commit);
  - `test_semantic_class_records_keep_their_type_name_and_no_p` (an `Order` record has type `acme.sales.Order`
    and the format-4 envelope);
  - `test_semantic_elements_move_into_the_registry_chain` (`"number" not in Order.__dict__`; the chain of
    `acme.sales.Order` holds it);
  - `test_extension_subclass_equals_the_decorator` (both spellings give equal `module_contributions`);
  - `test_delivery_redefines_the_number_label` (brief block 2: composed `number` has label `{"und": "Order no."}`,
    `required` true, provenance `["acme_sales", "acme_delivery"]`; `delivery_date` has provenance
    `["acme_delivery"]`);
  - `test_restated_structural_field_must_be_equal` (`number = Field(str, ...)` in an extension raises);
  - `test_stored_field_read_does_not_call_the_descriptor` (0 `resolve` calls when the record holds the field);
  - `test_absent_field_resolves_to_its_default_or_none` (`state` gives `"open"`);
  - `test_text_computed_is_neutral_and_lambda_computed_is_python_only` (R-S19, R-S20);
  - `test_cardinality_spellings_give_both_ends` (`Order.customer` "many-to-one": end `one`, `Customer.orders`
    `many`);
  - `test_relation_to_agrees_with_the_role` (`Order.lines` and `OrderLine.order`; a disagreeing `ordered` raises);
  - `test_index_by_through_a_role_is_catalogued_as_a_maintained_key` (`OrderLine.product`
    `index_by=["order.date"]`);
  - `test_invariant_without_a_name_gets_the_hash_name` (stable across processes: the expected name is a literal in
    the test);
  - `test_intent_keeps_the_briefs_signature` (`add_line(self, product, quantity, price)` is catalogued with three
    parameters; `intent_context()` outside an intent raises);
  - `test_describe_class_method_needs_a_source`;
  - `test_semantic_subclass_of_a_semantic_class_is_refused`;
  - `test_describe_returns_only_neutral_data`;
  - `test_composition_is_independent_of_install_order` (permuted lists: equal `canonical_form`);
  - the central conformance test: `canonical_form(compose(registry of the fixture)) ==` the bytes of
    `vectors/catalog/canonical.msgpack`.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement. Existing registry refusals keep their messages; the new kind and hook change no existing
  refusal.
- [ ] Step 4: Run the tests, then `tests/unit/test_registry*.py`, `tests/unit/test_resolution.py`,
  `tests/unit/test_dbobject*.py`, `tests/unit/test_type_names.py`, `tests/docs/test_tutorials.py` and
  `tests/conformance`. Expect PASS.
- [ ] Step 5: Commit "Semantic layer: the brief's declarations on the registry, composition with provenance".

### Task 5: The semantic catalog in the database: install, verification, session, as-of

**Files:**
- Create: `proto_db/semantic/store.py`, `proto_db/semantic/session.py`.
- Modify: `proto_db/transactions.py`:
  - `install_module` collects the semantic additions;
  - `_merged_registry` merges `__semanticCatalog` add-if-absent under the root lock, recomposing the entities for
    the merged module set (no import under the lock, as for the registry).
- Test: `tests/unit/test_semantic_catalog_store.py` (new),
  `tests/concurrency/test_semantic_install_concurrency.py` (new, `freethreaded`).

**Interfaces:**
- Consumes: `compose`, `module_contributions` and `describe` (Task 4); `to_atoms`, `from_atoms` and
  `canonical_form` (Task 2).
- Produces:
  - `merged_semantic_catalog(current: SemanticCatalog | None, registry: Registry, context) -> SemanticCatalog | None`
    in `store.py`. It adds the Module items the catalog lacks and recomposes `entities` for the set. It returns
    `None` when nothing changes.
  - `semantic(source: Transaction | Snapshot) -> SemanticSession`.
  - `SemanticSession.catalog() -> dict`: the stored model, verified on first use against `compose(registry)`. A
    difference raises `ProtoValidationError` naming the module and element (R-S6). No stored catalog with installed
    modules raises `ProtoNotSupportedError` ("no semantic catalog; install a module to add it").
  - `SemanticSession.describe(type_name: str) -> dict`.
  - `SemanticSession.list_entities(filter: str | None = None) -> list[dict]` (`type_name`, `label`, `meaning`).
  - `<SemanticClass>.describe(source)` (Task 4) returns `semantic(source).describe(cls.__type_name__)`.
- Rule: reading `__semanticCatalog` for the session does not enter the read set (as `installed_modules`).

- [ ] Step 1: Write the failing tests:
  - `test_install_writes_the_semantic_catalog`;
  - `test_install_order_does_not_change_the_stored_composition` (two databases; equal `canonical_form`);
  - `test_reinstall_adds_nothing`;
  - `test_a_database_without_semantic_modules_writes_no_v6_segment`;
  - `test_rc4_database_without_semantic_catalog` (Review Focus 1: modules installed through a registry with no
    descriptors, then the semantic modules' code; plain resolution unchanged; `catalog()` raises
    `ProtoNotSupportedError`; after `install_module("acme_delivery")` it works);
  - `test_code_that_differs_from_the_stored_catalog_raises`;
  - `test_class_describe_reads_the_databases_composition` (`Order.describe(tr)` includes `delivery_date` only in a
    database with `acme_delivery`; two databases open in one process differ);
  - `test_as_of_describe_uses_that_versions_catalog` (Review Focus 2: before and after `acme_delivery`);
  - `test_an_extending_module_with_a_forbidden_change_fails_the_install` (nothing pending afterwards);
  - the concurrency test (Review Focus 5): two threads, then 8 threads in both GIL modes, install different
    semantic modules from one base. No conflict; the stored composition equals `compose` of the union.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests, then `tests/unit/test_registry_roots.py`, `tests/unit/test_registry_database.py` and
  `tests/concurrency/test_registry_install_concurrency.py`, in both GIL modes. Expect PASS.
- [ ] Step 5: Commit "Semantic layer: the catalog root, written at install and verified on use".

### Task 6: Entities and relations in direct mode; demo steps 1-3

**Files:**
- Create:
  - `proto_db/semantic/entities.py` (cells, creation, navigation, links, relationship atoms, maintained
    aggregates);
  - `docs/kit/semantic/semantic_demo.py`: steps 1-3, in the style of `docs/kit/erp_demo.py`: `check()`,
    `[check]` lines, exit 1 on a failed check, `--workdir`. Steps 1 and 2 print `describe` through the session
    for now (Task 7 moves them to MCP).
- Modify: `proto_db/semantic/session.py`.
- Test: `tests/unit/test_semantic_relations.py` (new); `tests/docs/test_kit.py::test_the_semantic_demo_runs_and_checks_itself`.

**Interfaces:**
- Produces `SemanticSession` methods. Direct mode applies them to cells at once, under today's read set and
  conflict rules.
  - `create(type_name: str, **fields) -> MutableObject`. Behaviour:
    - the cell is named `f"{type_name}#{uuid4().hex}"` (R-S3);
    - the version is an instance of the semantic class registered for `type_name` (D57 lookup); a type with no
      Python class in the process raises `ProtoNotSupportedError`;
    - a missing `required` field raises `ProtoValidationError`;
    - `one` relations given as fields (handles or versions obtained from the session) are linked.
  - `get(handle: MutableObject) -> DBObject`.
  - `update(handle, **changes) -> None`. Changing a role through `update` is refused; use `link`/`unlink`.
  - `link(source: MutableObject, role: str, target) -> None` and `unlink(...)`. Both ends and every maintained
    aggregate along the inverses are updated.
  - `relate(type_name: str, **roles_and_fields) -> DBObject`. It creates the relationship atom and adds it to the
    collection at each role's inverse end. A version-bound role stores `{"entity": handle, "version": version}`.
  - `navigate(source, role: str)`: a version (`one`), a collection (`many`: `List` if ordered, else `Set`), or
    for a version-bound role its version. Attribute access on a session version (`order.lines`, `line.product`)
    navigates the same way.
- `index_by` (R-S5):
  - a member-attribute path is `RangeIndex(f"by_{path}", path)` on the end collection;
  - a path through an identity role (`order.date`) makes the end collection hold entry `DBObject`s
    `{member, key}` indexed by `RangeIndex("by_order_date", "key")`, and `update` of the keyed attribute re-keys
    the entries of every member in the same transaction;
  - navigation always yields members.
- Maintained `Aggregate`s are updated by deltas (spec section 10), never recomputed.

- [ ] Step 1: Write the failing tests:
  - `test_create_names_the_cell_by_type_and_uuid`;
  - `test_required_field_missing_is_refused`;
  - `test_link_maintains_both_ends`;
  - `test_relationship_atom_is_shared_by_both_collections` (same oid from `order.lines` and
    `product.order_lines`);
  - `test_version_binding_keeps_the_product_as_sold` (update `list_price` after the sale; the line's product
    version keeps the old price, and `.entity` reaches the current one);
  - `test_sales_by_order_date_use_the_index` (the June sales of a product through the session: `explain()` names
    `by_order_date`; the results are lines, not entries);
  - `test_changing_an_order_date_rekeys_its_lines` (after `update(order, date=...)` the product's June query
    no longer returns that order's lines);
  - `test_order_total_and_customer_committed_are_maintained_by_deltas` (patch the evaluator: no `sum` over a whole
    collection during `relate`);
  - `test_direct_mode_concurrent_lines_on_one_order_conflict` (today's rule, the baseline of Task 10).
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement the API and the demo's steps 1-3. Step 3's invoice prints each line's bound product
  version.
- [ ] Step 4: Run the tests and `tests/docs/test_kit.py`. Expect PASS.
- [ ] Step 5: Commit "Semantic layer: entities and relations with inverses, version binding, demo steps 1-3".

### Task 7: Agent operations and the MCP server; demo steps 1, 2 and 5

The MCP Python SDK is **not installed** in any local venv (`import mcp` fails in `.venv314t`, `.venv` and
`ci314/venv31410`, checked 2026-11-15). It becomes a new optional extra.

**Files:**
- Create:
  - `proto_db/agent/__init__.py`;
  - `proto_db/agent/operations.py`, with no third-party imports;
  - `proto_db/agent/mcp_server.py`, which imports `mcp` lazily;
  - `proto_db/agent/__main__.py`, run as
    `python -m proto_db.agent --path P --database D [--mapping module=import.path ...] [--raw]`.
- Modify:
  - `pyproject.toml`: `agent = ["mcp"]`, pinned to the current release after reading its PyPI page in Step 1;
  - `.github/workflows/tests.yml`: install `.[dev,vectors,parquet,agent]`;
  - `docs/kit/semantic/semantic_demo.py`: steps 1, 2 and 5 through a scripted MCP stdio client (R-S9), printing
    each call and response.
- Test: `tests/unit/test_agent_operations.py` (new, all interpreters); `tests/integration/test_agent_mcp.py` (new,
  `pytest.importorskip("mcp")`).

**Interfaces:**
- Produces, in `proto_db.agent.operations`, each with `db: Database` and returning JSON-ready data:
  - `list_entities(db, filter: str | None = None) -> list[dict]`;
  - `describe(db, type_name: str) -> dict`;
  - `check_answerable(db, terms: list[str]) -> dict`: `{"answerable": bool, "matches": {term: [candidate]},
    "unmatched": [term], "suggestions": {term: [candidate]}}`. A candidate is `{"kind", "type_name", "name",
    "via"}`. Matches are case-folded and exact on name, label and synonyms; suggestions are by substring (R-S17).
  - `query(db, tree: dict) -> dict`: `{"rows", "explain", "version", "query", "terms"}`. Behaviour:
    - the tree is the spec section 11 form, checked with `check_ast(..., in_query=True)`;
    - it runs on a `Snapshot` (`Database.snapshot_at`; current, or `as_of`), so `version` is
      `Snapshot.version`;
    - `where` goes through `to_planner`, else a residual;
    - `personal` and `secret` attributes are omitted from rows;
    - `terms` lists the catalog elements used, with their certification.
  - `to_json(value)`: Decimal as a string, dates as ISO 8601, handles as `{"cell": name}`.
  - `raw=True` on `list_entities`, `describe` and `query` (for the experiment's control group) ignores the catalog
    and reports only stored structure.
- Produces `serve(path: str, database: str, *, mapping: Mapping[str, str] | None = None, raw: bool = False) -> None`
  in `mcp_server.py`, exposing the four operations as MCP tools of the same names.

- [ ] Step 1: Check the `mcp` package's current release and its supported Pythons (PyPI). If no wheel set
  installs on 3.14t, CI installs `agent` on the other three versions only. The report says so, and
  `test_agent_mcp.py` skips there with that reason.
- [ ] Step 2: Write the failing tests:
  - `test_list_entities_gives_label_and_meaning`;
  - `test_describe_includes_provenance`;
  - `test_advertising_is_not_answerable` (`check_answerable(db, ["advertising", "spend"])["answerable"] is False`);
  - `test_product_sales_in_june` (rows, an `explain` naming `by_order_date`, `version` equal to the snapshot's);
  - `test_closest_to_limit` (aggregate over customers);
  - `test_query_as_of_reads_that_version`;
  - `test_personal_attributes_are_omitted`;
  - `test_raw_mode_hides_the_catalog`;
  - `test_mcp_server_lists_the_four_tools_and_answers_describe` (integration, stdio).
- [ ] Step 3: Run the tests. Expect FAIL. Implement. Run them again. Expect PASS (the MCP test skips where the extra
  is absent).
- [ ] Step 4: Install the extra in a fresh scratch venv under `$SCR`. Run `tests/integration/test_agent_mcp.py` and
  the demo there. Expect PASS.
- [ ] Step 5: Commit "Agent interface: neutral operations and an MCP server (optional extra agent)".

### Task 8: Intent transactions behind a flag (one node)

**Files:**
- Create: `proto_db/semantic/intents.py`.
- Modify:
  - `proto_db/transactions.py`:
    - `Database.new_transaction(*, intents: bool = False)`;
    - `Database.run(fn, retries=10, backoff=(0.001, 0.2), *, intents: bool = False)`;
    - `Transaction.__init__(database, *, intents: bool = False)`;
    - in `_build_root`, after the read-set check and before the mutables are written: the fine read set check
      and `self._intent_log.replay(...)`;
  - `proto_db/semantic/session.py`: intent mode;
  - `proto_db/semantic/entities.py`: operations as log entries.
- Test: `tests/unit/test_semantic_intents.py` (new), `tests/concurrency/test_semantic_intents_concurrency.py` (new,
  `freethreaded`).

**Interfaces:**
- Consumes: the Task 6 operations, which become log entries in intent mode.
- Produces:
  - Calling an intent on a session version (`order.add_line(product, 2, price)`, the brief's signature) goes
    through the invoker of Task 4. It logs the call and runs the implementation through D62 resolution, so
    `_super()` works, with `intent_context()` bound for the call (R-S21). `SemanticSession.invoke(target, intent,
    **arguments)` is the same by name. Calling an intent on a version not obtained from a session raises
    `ProtoUserError`.
  - `IntentContext` (what `intent_context()` returns): `target` (the handle), `create`, `get`, `update`, `link`,
    `unlink`, `relate`. An intent that returns a new version of its receiver (`self.with_(...)`) updates the target.
  - `IntentLog.replay(tr, db_root) -> tuple[Dictionary, Dictionary]` (the new mutables and objects). It runs only
    under the root lock and raises `ProtoConflictError` when a fine read changed.
  - A UUID for `create` is drawn at record time and logged as an argument (R-S3). Replay draws nothing.
- Rules:
  - fine read set entries are `(cell, attribute) -> exact token` (R-S13), recorded by `get(...)` attribute access
    through a read-tracking view;
  - condition reads (iterating a collection, a `via`, a session query) record the scanned collections' tokens
    (R-S4);
  - `__intentLog` is set to a `List` of `IntentRecord`s of this commit (R-S14), with `transaction` = the new
    version;
  - the flag is refused with `ProtoNotSupportedError` when `space.coordinator is not None`;
  - a raw `set_mutable` of a cell the log touches raises `ProtoUserError`.

- [ ] Step 1: Write the failing tests:
  - `test_without_the_flag_nothing_changes` (same records as before for a non-semantic workload, compared with
    the base commit's output);
  - `test_two_orders_for_one_customer_both_commit` (two transactions from one snapshot; both commit; the customer
    has both orders; `committed` is the sum);
  - `test_reading_price_does_not_conflict_with_a_new_sale` (a fine read of `list_price`, while a concurrent `relate`
    adds to `order_lines`);
  - `test_reading_a_changed_attribute_conflicts`;
  - `test_condition_read_conflicts_when_the_collection_changed`;
  - `test_replay_is_deterministic_and_draws_no_uuid` (monkeypatch `uuid4` to raise during replay);
  - `test_intent_log_records_each_operation` (`__intentLog` of the version, `transaction` = version);
  - `test_add_line_super_chain_runs_once_per_module`;
  - `test_confirm_returning_with_updates_the_target` (brief block 6: `confirm` gives `state == "confirmed"`);
  - `test_intent_context_outside_an_intent_raises`;
  - `test_requires_is_evaluated_at_replay` (`confirm`'s `requires="state == 'open'"`: true at record time and
    false on the current state after a concurrent confirm, it raises `ProtoConstraintError` naming the intent; that class arrives in Task 9, so Task 8 asserts a
    `ProtoValidationError` and Task 9 tightens it);
  - `test_cluster_space_refuses_intents`;
  - `test_raw_write_of_a_logged_cell_is_refused`.
  - The concurrency test: 8 threads create 200 orders for one company and distinct customers, in both GIL modes,
    with direct `commit()` and no retry. Every commit succeeds, and the final collections hold every order once.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement.
- [ ] Step 4: Run the tests, then `tests/unit/test_transaction*.py`, the D33 merge tests (`grep -l rebase_onto tests`)
  and `tests/concurrency/test_standalone_group_commit.py`, in both GIL modes. Expect PASS.
- [ ] Step 5: Commit "Semantic layer: intent transactions behind a flag, replayed at commit (one node)".

### Task 9: Invariants and errors with meaning; demo step 4

**Files:**
- Create: `proto_db/semantic/constraints.py`.
- Modify:
  - `proto_db/errors.py`: `class ProtoConstraintError(ProtoValidationError)` (R-S16);
  - `proto_db/semantic/intents.py`: check after replay;
  - `proto_db/semantic/entities.py`: the direct-mode check at the operation;
  - `docs/kit/semantic/semantic_demo.py`: step 4. Processes create orders concurrently; one exceeds the limit and
    its error is printed. Then the `describe` of the constraint is printed through MCP, as the agent's material for
    explaining and proposing alternatives.
- Test: `tests/unit/test_semantic_constraints.py` (new).

**Interfaces:**
- Produces:
  - `ProtoConstraintError(constraint: str, meaning: str, message: str, entity: str, values: dict)`.
  - `ProtoConstraintError.describe() -> dict` (neutral).
  - `check_constraints(model, touched: Mapping[MutableObject, DBObject], reader) -> None`. It checks cardinality
    for every touched entity, and the enforced constraints whose `dependencies` intersect what changed.
  - The rendered message formats `message` with the placeholders of R-S22: the attribute names, `{entity}` and the
    type's lower-case short name (both the entity's key), and `{left}`, `{right}` and `{excess}` for ordering
    comparisons. A MISSING operand renders as `<missing name>`, and its name is in `values`.

- [ ] Step 1: Write the failing tests:
  - `test_concurrent_orders_within_the_limit_both_commit` (limit 100, committed 20, two of 30: both commit,
    committed 80);
  - `test_the_later_order_over_the_limit_fails_with_the_rule` (second of 60: `ProtoConstraintError` whose
    `constraint` is the R-S22 hash name of `Invariant("committed <= credit_limit")`, with its meaning and the
    message "The order exceeds the credit limit of <customer key> by 10"; nothing of it published);
  - `test_cancelled_orders_leave_the_aggregate` (the aggregate's `where="state != 'cancelled'"`: cancelling gives
    a negative delta);
  - `test_constraint_error_is_not_retried_by_run` (Review Focus 3: `fn` called once);
  - `test_only_the_violating_commit_fails_in_a_group_round` (Review Focus 3);
  - `test_missing_credit_limit_names_the_operand` (Review Focus 4);
  - `test_cardinality_one_is_enforced` (an order whose customer is unlinked);
  - `test_informative_constraint_is_not_enforced` (`enforced=False`);
  - `test_direct_mode_checks_at_the_operation`.
- [ ] Step 2: Run the tests. Expect FAIL.
- [ ] Step 3: Implement. Add step 4 to the demo.
- [ ] Step 4: Run the tests, Task 8's tests and `tests/docs/test_kit.py`. Expect PASS.
- [ ] Step 5: Commit "Semantic layer: invariants checked on the combined result, errors with meaning, demo step 4".

### Task 10: The concurrency experiment and the history verifier

**Files:**
- Create:
  - `benchmarks/semantic/__init__.py`;
  - `benchmarks/semantic/history_verifier.py`;
  - `benchmarks/semantic/credit_experiment.py`. It is self-reporting: it prints the work done and verifies it with
    the verifier.
- Modify: `docs/source/performance.rst`, a section "Semantic model: intents against direct mode", in its existing
  conventions (commit, clean flag, hardware, settings).
- Test: `tests/unit/test_semantic_history_verifier.py` (new).

**Interfaces:**
- Produces `verify_history(db: Database) -> VerifierReport`. The report has `versions`, `violations` (a list of
  `(version, customer cell, committed, limit)`) and `aggregate_mismatches`. The verifier walks
  `Database.history()`, recomputes every customer's sum of order totals from the lines and compares it with the
  limit and with the stored aggregate.
- Produces `python -m benchmarks.semantic.credit_experiment --mode direct|intents --threads N --orders M --customers K --seed S --out FILE`.
  The JSON has `commit`, `clean`, `mode`, `gil`, attempts, commits, conflicts (retries), rejected by conflict,
  rejected by constraint, the verifier's violations, and commit latency p50/p95. Baseline (R-S18): direct mode
  with `Database.run` retries.

- [ ] Step 1: Write the failing tests:
  - `test_verifier_finds_a_planted_violation` (a history written raw with an over-limit state);
  - `test_verifier_finds_an_aggregate_mismatch`;
  - `test_verifier_passes_a_clean_history`.
- [ ] Step 2: Implement until they pass.
- [ ] Step 3: Run the experiment under the machine rules from a clean `python -m benchmarks.export` tree: both
  modes, 3.14t with the GIL on and off, 8 threads, settings stated in the results. Check the done criterion: zero
  independent orders rejected by conflict in intent mode, and verifier violations = 0 in both modes. Report
  whatever is measured, including any direct-mode figure that contradicts the brief's write-skew expectation.
- [ ] Step 4: Write the `performance.rst` section with the results files. Commit "Benchmarks: the
  intents-against-direct credit experiment, with its history verifier".

### Task 11: The protoBaseST handoff (step 6)

This task plans no protoST code. protoBaseST is another repository and another agent.

**Files:**
- Create:
  - `docs/kit/semantic/protoBaseST-handoff.md` (at most two pages). It names brief block 9 (`Customer class >>
    descriptors`) as protoBaseST's declaration spelling, which must reach block 8's canonical form. It says what
    to read
    (`format-spec.rst` "Semantic catalog (format 6)", `tests/conformance/README.md` steps 13-14), what to
    implement (reading the catalog types, the canonical form, `describe`, the step-6 queries through generated
    accessors), and what not to (writes, opcodes, intents);
  - `proto_db/agent/step6.py`, run as `python -m proto_db.agent.step6 --path P --database D --out FILE`. It writes
    the step-6 expected output: `describe` of every entity and the rows of the step-6 queries, in `to_json` form
    with sorted keys;
  - `tests/conformance/test_catalog_cross_language.py`. It compares the protoBaseST output file
    (`PROTODB_ST_STEP6_OUTPUT`) and its canonical form (`PROTODB_ST_CANONICAL`) with Python's for the database at
    `PROTODB_ST_STEP6_DB`. It skips when the variables are unset.
- Modify: `docs/kit/semantic/semantic_demo.py`, whose step 6 runs the protoBaseST command when
  `PROTOBASEST_STEP6_CMD` is set, and otherwise prints the handoff path and marks the step "external".

- [ ] Step 1: Write `test_step6_expected_output_is_stable` (two runs over one database give identical bytes) and the
  cross-language test. Run them: the first PASSes, the second SKIPs.
- [ ] Step 2: Write the handoff note and the acceptance procedure: the protoBaseST agent passes when every catalog
  vector passes in protoBaseST and the cross-language test passes with its outputs.
- [ ] Step 3: Commit "Handoff: protoBaseST step 6, catalog vectors and acceptance test".

### Task 12: The agent experiment, the demo README and the done criteria

**Files:**
- Create:
  - `benchmarks/semantic/questions.json`: 24 questions, at least 4 not answerable (advertising among them), each
    with its ground truth as a query tree or `"not_answerable"`;
  - `benchmarks/semantic/agent_experiment.py`. It runs each question through a pluggable agent command against the
    MCP server in catalog and raw mode, records each transcript, and checks each answer against the ground truth;
  - `docs/kit/semantic/agent-experiment.md`, with the questions, both modes' answers, their correctness, the model
    and the commit;
  - `docs/kit/semantic/README.md`: one page, what each step shows and where to look in the code.
- Modify: `CHANGELOG.md` (an "Unreleased" section), `docs/source/api.rst` (`proto_db.semantic` and
  `proto_db.agent`).
- Test: `tests/docs/test_kit.py::test_the_semantic_demo_runs_and_checks_itself`, extended: all six step headings,
  "every check passed", and a runtime under 300 s.

**Interfaces:**
- Produces `python -m benchmarks.semantic.agent_experiment --agent-cmd "<template with {mcp_config} and {question}>" --out FILE`.
  The agent command is the operator's choice: a headless agent CLI with an MCP configuration. The results name the
  model. The harness never judges by string similarity: a numeric answer must equal the ground truth exactly
  (after Decimal parsing), and a "not answerable" must be declared as such.

- [ ] Step 1: Write the questions and the ground truth. Run the harness on a fake agent command (echoing the ground
  truth) to validate it: all correct.
- [ ] Step 2: Run the experiment with the real agent in both modes. Publish `agent-experiment.md`.
- [ ] Step 3: Write the demo README. Run the demo with one command from a fresh clone in a scratch venv
  (`pip install -e .[agent]`) and time it. Expect under 5 minutes and every check passing.
- [ ] Step 4: Check each done criterion of spec section 15 and record the evidence in the report: the command,
  output or file for each.
- [ ] Step 5: Run the full suites one at a time under the machine rules: 3.14t with the GIL off, 3.14t with the
  GIL on, 3.14.10 and `.venv`. Push the branch. Run CI on four versions plus docs, green.
- [ ] Step 6: Commit "Semantic MVP: agent experiment, demo README, done criteria". Write the final report to the
  maintainer in Spanish: what shipped, the measurements, proposed rulings still open, the cleanup list, and the
  protoBaseST handoff.

## Schedule against FAST (2026-12-03)

FAST shows what already works, the model as the next step, and this script as the plan; it does not need the MVP
finished. There are 18 calendar days from 2026-11-15. The script's stage 1 also competes for that time:
revalidating protoBaseST against rc4, fixing the demo versions, and the talk itself. These are estimates, not
measurements.
- **Realistic before FAST:** Tasks 1-3: the decisions, the format-6 chapter, and the catalog and expression
  vectors. That supports "specification first, then implementation against vectors". Task 4 is likely too: it
  has grown with the brief's real declarations (the semantic-class hook, `Extension`, text expressions in Task
  3). With it, the talk can show the brief's own `class Order(DBObject)` composing to the vector with
  `acme_delivery`'s provenance.
- **Stretch:** Task 5 (`Order.describe(tr)` on a real database); Task 6 (steps 1-3 from the demo script without
  MCP) only if Tasks 1-5 land early.
- **After FAST:** Task 7's server, Tasks 8-10 (intents, invariants, the experiment: the riskiest part, worth
  unhurried reviews), Task 11 (whose consumer, protoBaseST, follows its own schedule) and Task 12.
- If collaborators appear at FAST, Tasks 7 and 11 can run in parallel with Tasks 8-10, as the script foresees for
  its stages 4 and 6.
