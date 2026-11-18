# The semantic-model demo

A mini ERP whose database describes itself: a company, customers with a credit limit, products, and orders
with lines. One command runs the six steps, checks every result and prints `every check passed`:

```
pip install -e .[agent]                         # the MCP SDK, for the agent steps (1, 2, 4 and 5)
python docs/kit/semantic/semantic_demo.py       # add --workdir DIR to keep the database
```

The design is `docs/superpowers/specs/2026-11-15-semantic-model-mvp-design.md`; decisions D63-D72 are in
`docs/decisions.md`; the stored form is the chapter "Semantic catalog (format 6)" of `docs/source/format-spec.rst`.
The model is declared in `docs/kit/semantic/acme_sales.py` and `docs/kit/semantic/acme_delivery.py`; every query
the kit asks is named in `docs/kit/semantic/queries.py`. No language model runs in the demo: the agent steps use a
scripted MCP client that prints each tool call and response.

## Step 1: the database describes itself

Installing `acme_sales` writes the catalog (the root `__semanticCatalog`) into the database. Through MCP the
agent lists the entities and reads what an order and a line are, and that `price` on a line is the price agreed
at the sale, not the product's current one.

Where to look: the declarations (`Field`, `Relation`, `Aggregate`, `Invariant`, `intent`) in
`proto_db/semantic/descriptors.py`; their mapping onto registry classes in `proto_db/semantic/classes.py`; the
catalog written at install and verified on use in `proto_db/semantic/store.py`; the operations `list_entities`
and `describe` in `proto_db/agent/operations.py`, served by `proto_db/agent/mcp_server.py`.

## Step 2: a module extends another

`acme_delivery` adds `delivery_date` to the order and relabels `number` "Order no."; `describe` gives each
element's provenance (`["acme_sales", "acme_delivery"]`).

Where to look: `Extension` in `proto_db/semantic/classes.py`; composition and provenance in
`proto_db/semantic/compose.py` (the same catalog whatever the installation order:
`tests/unit/test_semantic_compose.py`).

## Step 3: relations navigable from both sides

24 orders with lines are loaded. An order reaches its products; a product reaches its own lines, with the index
`by_order_date`, with no global structure. After the widget's list price changes, an invoice still shows each
product as sold: the line's `product` role is bound by version.

Where to look: entities, inverses, version binding and maintained aggregates in `proto_db/semantic/entities.py`;
the expression language (`quantity * price`, `state != 'cancelled'`) in `proto_db/semantic/expressions.py`; the
semantic session (`semantic(tr)`, `query`) in `proto_db/semantic/session.py`.

## Step 4: coexist or collide by meaning

Seven threads create orders for one company from one snapshot, in intent mode (`new_transaction(intents=True)`).
The independent orders all commit with no conflict; of Fabrikam's two orders, which fit its limit each but not
together, the one replayed second fails with `ProtoConstraintError`, whose message names the rule. A walk of the
whole history recomputes every customer's committed amount and finds no version over a limit. The agent then
describes the customer: the rule, its meaning and the aggregate are its material to explain the refusal.

Where to look: intents logged and replayed at commit in `proto_db/semantic/intents.py`; invariants checked on
the combined result in `proto_db/semantic/constraints.py`; the history walk is `_verify_history` in the demo,
shown failing on planted damage in `tests/docs/test_kit.py`. The measured concurrency experiment is
`benchmarks/semantic/credit_experiment.py`, published in `docs/source/performance.rst`.

## Step 5: questions with verifiable answers

"How much of the widget did we sell in June?", "Which customer is closest to its limit?" and "How much did we
spend on advertising?". The agent finds where to start through `list_entities`, checks each question's terms
with `check_answerable`, and queries. Each answer carries its query, its `explain()` and the version read; the
customer's name is personal, so the answer omits it and says so. The catalog names neither "advertising" nor
"spend": the agent says it cannot answer. "Closest to its limit" is a query over the customers plus a declared
client-side step, `argmin(credit_limit - committed)`, since the MVP's query tree has no ordering.

Where to look: `check_answerable` and `query` (sensitivity, raw mode) in `proto_db/agent/operations.py`; the
planner's `explain()` comes from `proto_db/query/planner.py`. The agent experiment asks 24 such questions of a
live agent with and without the catalog: `benchmarks/semantic/agent_experiment.py`, with the questions in
`benchmarks/semantic/questions.json` and the method and results in `docs/kit/semantic/agent-experiment.md`.

## Step 6: the same questions in another language (external)

protoBaseST (Smalltalk) opens the same database, reads the same catalog and runs step 5's queries. It lives in
another repository; the demo says so and points at `docs/kit/semantic/protoBaseST-handoff.md`. With
`PROTOBASEST_STEP6_CMD` set, the demo runs it and compares its output with the expected output that
`proto_db/agent/step6.py` writes (`tests/conformance/test_catalog_cross_language.py`).

## Things to know

- A semantic object resolves a missing attribute through the registry of the database it was read from. Outside
  one (an object built in plain Python, or read with no current registry), `getattr(o, "x", None)` raises
  `ProtoNotSupportedError` for an attribute the object does not hold, instead of returning `None`; its own
  fields read normally.
- Raw mode (`python -m proto_db.agent ... --raw`) is the agent experiment's control group: it exposes the stored
  structure only. The control agent still sees the stored type and attribute names (`acme.sales.Customer`,
  `credit_limit`), not their meaning, labels, synonyms, units or computed attributes.
- Personal and secret attributes are listed by `describe` and never returned by `query`, in either mode, also
  through a path that crosses a record the catalog does not describe. The check errs on the safe side: an
  attribute of an uncatalogued record that has the same name as a catalogued personal or secret attribute is
  withheld too.
- Agents never write: intent execution through the agent interface is outside the MVP.
- The MCP server keeps the operator's log (`proto_db.agent`: the original messages an agent is not told) off its
  stderr, which the MCP client collects: pass `--log FILE` to keep it. The log can be noisy: a failure that
  looks like a programming error (`KeyError`, `AttributeError`) during evaluation is logged at WARNING, and a
  closed space or a storage failure at ERROR on every call that meets it.
- Limits of the MVP: field values are not type-checked against their declared kind (spec section 14);
  the history verifier compares decimals by value (`2.00` equals `2`); a semantic object builds one
  operations object per attribute access, and an ordered end is scanned linearly when a member is linked or
  unlinked (quadratic when a load links many members one by one). These costs are listed, not yet measured, in
  `docs/source/performance.rst`.
- A later module may add attributes and constraints to types that already have entities. A maintained aggregate
  it adds is computed for existing entities until their first write stores it; a `required` field or an enforced
  invariant it adds is not checked against existing entities (format spec, "Constraints").
