# protoBaseST handoff: step 6 of the semantic-model demo

For the agent that works on protoBaseST (the Smalltalk implementation of the ProtoBase format): what to read,
what to implement, what not to, and how acceptance is decided. The normative text is
`docs/source/format-spec.rst`; where this note and the specification differ, the specification wins.

## The goal

Step 6 of the demo: protoBaseST opens a database written by the Python demo (modules `acme_sales` and
`acme_delivery`, orders loaded), describes every entity of its catalog and answers the step-6 queries through
generated accessors, giving the data Python gives.

## Declaration spelling

The semantic brief (block 9) declares an entity in Smalltalk with class-side methods:

    Customer class >> typeName    ^ 'acme.sales.Customer'.
    Customer class >> descriptors ^ { PBField named: 'credit_limit' ... . PBRelation named: 'orders' ... }.

`Customer class >> descriptors` is protoBaseST's declaration spelling. The catalog it produces must reach the
**canonical form** of the Python declaration (block 8): the same `Entity`, `Attribute` and `Role`, byte for
byte ("Composition and the canonical form"; bytes in `tests/conformance/vectors/catalog/canonical.msgpack`).

## What to read

1. `docs/source/format-spec.rst`, "Semantic catalog (format 6)" (the catalog's records, aliases 24 to 34,
   the feature `catalog`, composition, the canonical form), "Entity data" (cells, fields, `one` and `many`
   ends, bound references, entry records, maintained aggregates) and "Constraints".
2. `tests/conformance/README.md`, steps 13 (`catalog_database`) and 14 (`expressions`), and their vectors
   (`vectors/catalog/`, `catalog_model.json`, the `expressions` entries of `manifest.json`).
3. `docs/kit/semantic/queries.py` (the named query trees), `acme_sales.py` and `acme_delivery.py` (the model).
   protoBaseST need not serve MCP; it produces the data below.

## What to implement

- **Reading the catalog types** (`proto_db.catalog.SemanticCatalog` to `IntentRecord`) from a format-6
  database, and the composition of the modules' contributions in load order.
- **The canonical form** of the catalog, byte for byte.
- **`describe`** of every entity, a JSON object of the composed entity: its catalogued elements (`attributes`,
  `roles`, intents, `constraints`, `provenance`), and `kind`, `"entity"` or `"relationship"` (the latter when
  it has `roles`). An expression appears as `{"ast": [...]}`, the tree as nested arrays; a language map as an
  object; values take the forms below. A constraint declared enforced that this revision does not enforce (a
  catalog written by another writer may hold one) carries `"not_enforced"`, the reason as text; it is absent
  otherwise.
- **The step-6 queries** (`widget`, `sold_in_june`, `customer_limits` in `queries.py`) through accessors
  generated from the catalog: starting cells found through the catalog (`list_entities` lists, per type, its
  cells, paged by `limit` and the `next` token), never hard-coded. Each answer has `query`, `rows` and
  `version`; `explain` (plans, index statistics, cost estimates), `terms` and the version's `ref` (a pager
  token) are Python's own output, not defined by the format: neither written nor compared. Compared:
  - `version`: `{"number": n}`, the version number of the database root that was read (an integer);
  - `query`: the tree that was asked, as `queries.py` gives it (copied through unchanged);
  - `rows`: one object per row, `{"entity", "type_name", "values"}`. `entity` is `{"cell": name}` (its cell, `<type>#<32 hex>`) or `null` for a relationship atom, which has no cell; `type_name` the stored type; `values` maps each selected path, or, without `select`, each catalogued attribute and `one`
    role the type has and is allowed to show, to its JSON form. A value the element does not have is left out.
    The JSON forms: a decimal is its decimal text as a string (`"771.00"`: the exponent is kept, so `"2.00"`
    is not `"2"`); an integer or boolean is a JSON number or boolean; text is a string; a date, time or
    datetime is its ISO 8601 text; bytes are `{"bytes": base64}`; a role designating an entity is `{"cell":
    name}`; a version-bound role is `{"entity": {"cell": name}, "version": {"type_name": T, "values": {...}}}`
    with the bound record's own values in these forms; a record held inside another is `{"record": type name}`
    and a collection is `{"collection": "list" | "set", "count": n}`. Personal attributes (customer names) never
    appear in rows;
  - the **order** of `rows` is the order the traversal reaches the members: a `many` end that is a Set is
    walked in the collection's iteration order, a KeyNode tree by `hash_key` (format spec, "Hash keys" and the
    collections chapter), so rows over a Set (`customer_limits`, `widget`) are not in cell-name order; a List end is walked in stored order.
- Reading entities as "Entity data" specifies (cells, parentless records, bound references, absent and nil
  ends as empty, defaults for absent fields, maintained values compared by value).
- The expression vectors follow D28 and R265 (a comparison across kinds is false, an absent root has no
  members); only the agent interface's `query` refuses those two cases (design spec section 11, R283), which the
  step-6 queries never reach.

## What not to implement

- **Writes** of any kind: step 6 only reads.
- **Opcodes** or a bytecode form of expressions beyond evaluating the `expressions` vectors.
- **Intents** (replaying or recording them), the intent log, enforcing constraints, and anything the
  specification marks as not specified by this revision.

## The output file and acceptance

The expected output is produced by Python:

    python -m proto_db.agent.step6 --path SPACE_DIR --database acme --out expected.json [--canonical canonical.msgpack]

It holds `{"describe": {type name: description}, "queries": {name: {"query", "rows", "version": {"number"}}}}` as UTF-8
JSON. Python writes sorted keys, two-space indentation and a final newline, for humans; that formatting is
**not** compared. The comparison is of the parsed data: the same keys, values with the same JSON types (a
decimal is a string, `1` is not `1.0`) and the same array order. protoBaseST writes any JSON with that content.
Make the database with `python docs/kit/semantic/semantic_demo.py --workdir DIR` (the space is `DIR/space`).

Acceptance, in this order:

1. every catalog vector passes in protoBaseST (README steps 13 and 14, plus the earlier steps its reader needs);
2. the cross-language tests pass with protoBaseST's outputs (what they compare: `describe` of every entity, the
   step-6 queries' `query`, `rows` and `version.number`, and the canonical form's bytes), from the repository root:

       PROTODB_ST_STEP6_DB=DIR/space PROTODB_ST_STEP6_OUTPUT=st-step6.json PROTODB_ST_CANONICAL=st-canonical.msgpack \
         python -m pytest tests/conformance/test_catalog_cross_language.py

   (`PROTODB_ST_STEP6_DATABASE` names the database if it is not `acme`.) Without these variables the two
   cross-language tests skip; the file's other tests always run and prove the comparator reports a one-field
   difference, as `path: expected ..., actual ...`;
3. the demo's step 6 runs your command: `PROTOBASEST_STEP6_CMD="<command>" python docs/kit/semantic/semantic_demo.py`
   appends `--path`, `--database` and `--out` to it and compares the file it writes. Without the variable the
   step is printed as "external".
