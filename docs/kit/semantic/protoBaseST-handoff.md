# protoBaseST handoff: step 6 of the semantic-model demo

This note is for the agent that works on protoBaseST (the Smalltalk implementation of the ProtoBase format).
It names what to read, what to implement, what not to, and how acceptance is decided. It does not restate
the format: the normative text is `docs/source/format-spec.rst`, and where this note and the specification
differ, the specification wins.

## The goal

Step 6 of the demo: a database written by the Python demo (modules `acme_sales` and `acme_delivery`
installed, orders loaded) is opened by protoBaseST, which describes every entity of its catalog and answers
the step-6 queries through generated accessors, giving exactly the data Python gives.

## Declaration spelling

The semantic brief declares an entity in Smalltalk with class-side methods (its block 9):

    Customer class >> typeName    ^ 'acme.sales.Customer'.
    Customer class >> descriptors ^ { PBField named: 'credit_limit' type: #decimal unit: 'currency:company' ...
                                      PBRelation named: 'orders' target: 'acme.sales.Order' ... }.

`Customer class >> descriptors` is protoBaseST's declaration spelling. The catalog it produces must reach the
**canonical form** of the Python declaration (the brief's block 8): the same `Entity`, `Attribute` and `Role`,
byte for byte. The canonical form is specified in the format specification, "Composition and the canonical
form"; the bytes to match are in `tests/conformance/vectors/catalog/canonical.msgpack`.

## What to read

1. `docs/source/format-spec.rst`, "Semantic catalog (format 6)" (the catalog's records, aliases 24 to 34,
   the feature `catalog`, composition, the canonical form), "Entity data" (cells, fields, `one` and `many`
   ends, bound references, entry records, maintained aggregates) and "Constraints".
2. `tests/conformance/README.md`, steps 13 (`catalog_database`) and 14 (`expressions`), and the vectors they
   name: `vectors/catalog/`, `catalog_model.json`, and the `expressions` entries of `manifest.json`.
3. `proto_db/agent/operations.py` (module docstring): the JSON form of `describe`, `list_entities` and `query`
   answers (`to_json`), the row shapes, the sensitivity rules and the error contract. protoBaseST need not
   serve MCP; it must produce the same data.
4. `docs/kit/semantic/queries.py`: the named query trees; `acme_sales.py` and `acme_delivery.py`: the model.

## What to implement

- **Reading the catalog types** (`proto_db.catalog.SemanticCatalog` to `IntentRecord`) from a format-6
  database, and the composition of the modules' contributions in load order.
- **The canonical form** of the catalog, byte for byte.
- **`describe`** of every entity: a JSON object of the composed entity (attributes, roles, constraints,
  provenance; expression trees as nested arrays, language maps as objects, the forms below for values).
- **The step-6 queries** (`widget`, `sold_in_june`, `customer_limits` in `queries.py`) through accessors
  generated from the catalog: starting cells found through the catalog (`list_entities` lists, per type, its
  cells, paged by `limit` and the `next` token), never hard-coded. Each answer has `query`, `rows` and
  `version`; `explain` (plans, index statistics, cost estimates), `terms` and the version's `ref` (a pager
  token) are Python's own output, which the format does not define, so they are neither written nor compared.
  What is compared is:
  - `version`: `{"number": n}`, the version number of the database root that was read (an integer);
  - `query`: the tree that was asked, as `queries.py` gives it (copied through unchanged);
  - `rows`: one object per row, `{"entity", "type_name", "values"}`. `entity` is `{"cell": name}` (the entity's
    mutable cell, `<type>#<32 hex>`) or `null` for a relationship atom, which has no cell; `type_name` is the
    stored type; `values` maps each selected path, or, without `select`, each catalogued attribute and `one`
    role the type has and is allowed to show, to its JSON form. A value the element does not have is left out.
    The JSON forms: a decimal is its decimal text as a string (`"771.00"`: the exponent is kept, so `"2.00"`
    is not `"2"`); an integer or boolean is a JSON number or boolean; text is a string; a date, time or
    datetime is its ISO 8601 text; bytes are `{"bytes": base64}`; a role designating an entity is `{"cell":
    name}`; a version-bound role is `{"entity": {"cell": name}, "version": {"type_name": T, "values": {...}}}`
    with the bound record's own values in these forms; a record held inside another is `{"record": type name}`
    and a collection is `{"collection": "list" | "set", "count": n}`. Customer names are personal and never
    appear in rows (`describe` lists them with their sensitivity);
  - the **order** of `rows` is the order the traversal reaches the members: a `many` end that is a Set is
    walked in the collection's iteration order, a KeyNode tree by `hash_key` (format spec, "Hash keys" and the
    collections chapter), so rows over a Set (`customer_limits`, `widget`) are not in cell-name order; an
    ordered end (a List) is walked in its stored order.
- Reading entities as "Entity data" specifies: cells named `<type>#<32 hex>`, records without parents, bound
  references `{entity, version}`, absent and nil ends read as empty, defaults read when a field is absent,
  maintained values compared by value.

## What not to implement

- **Writes** of entities, relationships, intents or constraints: step 6 only reads.
- **Opcodes** or a bytecode form of the expression language beyond evaluating the `expressions` vectors.
- **Intents** (replaying or recording them), the intent log, and enforcing constraints.
- Anything the specification marks as not specified by this revision.

## The output file and acceptance

The expected output is produced by Python:

    python -m proto_db.agent.step6 --path SPACE_DIR --database acme --out expected.json [--canonical canonical.msgpack]

It holds `{"describe": {type name: description}, "queries": {name: {"query", "rows", "version": {"number"}}}}` as UTF-8
JSON. Python writes it with sorted keys, two-space indentation and one final newline, for humans to read; that
formatting is **not** compared. The comparison is of the parsed data: the same keys, the same values with the
same JSON types (a decimal is a string, `1` is not `1.0`) and the same array order. protoBaseST writes any
JSON with that content for the same database; the demo
writes the database with `python docs/kit/semantic/semantic_demo.py --workdir DIR` (the space is `DIR/space`).

Acceptance, in this order:

1. every catalog vector passes in protoBaseST (`tests/conformance/README.md`, steps 13 and 14, plus the
   earlier steps its reader depends on);
2. the cross-language tests pass with protoBaseST's outputs (what they compare: `describe` of every entity, the
   step-6 queries' `query`, `rows` and `version.number`, and the canonical form's bytes), from the repository root:

       PROTODB_ST_STEP6_DB=DIR/space PROTODB_ST_STEP6_OUTPUT=st-step6.json PROTODB_ST_CANONICAL=st-canonical.msgpack \
         python -m pytest tests/conformance/test_catalog_cross_language.py

   (`PROTODB_ST_STEP6_DATABASE` names the database if it is not `acme`.) Without these variables the two
   cross-language tests skip; the other tests of that file run always and prove the comparator reports a
   one-field difference. On a failure each difference is a line `path: expected ..., actual ...`;
3. the demo's step 6 runs your command: `PROTOBASEST_STEP6_CMD="<command>" python docs/kit/semantic/semantic_demo.py`
   appends `--path`, `--database` and `--out` to it and compares the file it writes. Without the variable the
   step is printed as "external".

This note plans no protoST code and changes no protoBaseST file.
