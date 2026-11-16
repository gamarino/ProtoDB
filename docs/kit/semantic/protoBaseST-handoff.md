# protoBaseST handoff: step 6 of the semantic-model demo

This note is for the agent that works on protoBaseST (the Smalltalk implementation of the ProtoBase format).
It names what to read, what to implement, what not to, and how acceptance is decided. It does not restate
the format: the normative text is `docs/source/format-spec.rst`, and where this note and the specification
differ, the specification wins.

## The goal

Step 6 of the demo: a database written by the Python demo (modules `acme_sales` and `acme_delivery`
installed, orders loaded) is opened by protoBaseST, which describes every entity of its catalog and answers
the step-5 queries through generated accessors, giving exactly the data Python gives.

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
- **`describe`** of every entity, in the `to_json` form the agent operations return.
- **The step-6 queries** (`widget`, `sold_in_june`, `customer_limits` in `queries.py`) through accessors
  generated from the catalog: starting cells found through the catalog (`list_entities` lists, per type, its
  cells, paged by `limit` and the `next` token), never hard-coded. Rows have the shape of the `query`
  answer: `rows`, `explain`, `version`, `query`, `terms`.
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

It holds `{"describe": {type name: description}, "queries": {name: answer}}` as UTF-8 JSON, sorted keys,
two-space indentation, one final newline. protoBaseST writes the same file for the same database; the demo
writes the database with `python docs/kit/semantic/semantic_demo.py --workdir DIR` (the space is `DIR/space`).

Acceptance, in this order:

1. every catalog vector passes in protoBaseST (`tests/conformance/README.md`, steps 13 and 14, plus the
   earlier steps its reader depends on);
2. the cross-language tests pass with protoBaseST's outputs, from the repository root:

       PROTODB_ST_STEP6_DB=DIR/space PROTODB_ST_STEP6_OUTPUT=st-step6.json PROTODB_ST_CANONICAL=st-canonical.msgpack \
         python -m pytest tests/conformance/test_catalog_cross_language.py

   (`PROTODB_ST_STEP6_DATABASE` names the database if it is not `acme`.) Without these variables the two
   cross-language tests skip; the other tests of that file run always and prove the comparator reports a
   one-field difference. On a failure each difference is a line `path: expected ..., actual ...`;
3. the demo's step 6 runs your command: `PROTOBASEST_STEP6_CMD="<command>" python docs/kit/semantic/semantic_demo.py`
   appends `--path`, `--database` and `--out` to it and compares the file it writes. Without the variable the
   step is printed as "external".

This note plans no protoST code and changes no protoBaseST file.
