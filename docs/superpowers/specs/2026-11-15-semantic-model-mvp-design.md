# Semantic model MVP: design

Status: **approved by the maintainer** (2026-11-15). It was written against the brief's restored code examples,
and the maintainer's rulings on its section 17 are applied: subclasses supported with minimal limits, `describe`
takes a snapshot, and every other proposed ruling confirmed as R258-R279.

Sources:
- the semantic-model brief (2026-11-14, the design of record, "the brief");
- the MVP script (2026-11-14, the scope authority, "the script"), whose five suggestions the maintainer accepted;
- the maintainer's rulings of 2026-11-15:
  - the runtime registry of 2.0.0rc4 (D62, format 5) **is** the behaviour part of modules: registry name = the
    entity's `__type_name__`, extensions folded in load order, `_super()`, `__currentModules`, ties broken by
    module name;
  - the semantic catalog is the new feature `catalog` of format 6, in the root `__semanticCatalog` (distinct
    from the registry's `__catalog`);
  - modules are never uninstalled: they evolve, and the database is perennial.

Section 17 lists the confirmed rulings (R258-R280); section 17a lists the two subclass rulings, confirmed on 2026-11-15.

**Scope rule (the script).** The MVP is a demo script, and only what the script needs is built. Everything else
in the brief is respected, and nothing built may contradict it: incomplete but coherent, never lame. The catalog
format is designed complete from the start. A changed scope is an explicit decision, recorded with its date and
reason.

## 1. Principles and what is normative

New principles, added to P1-P7 of `docs/design-principles.md`:
- **S1.** The class describes itself: structure, behaviour and meaning are declared in one place.
- **S2.** Meaning is operational: what is declared is used by the planner, the commit, validation or agents.
- **S3.** The intent is the unit of work: a transaction is what it read plus what it wants to do.
- **S4.** Transactions collide by meaning: only if one changed what the other used to decide, or if together they
  violate a declared invariant.
- **S5.** Everything is optional and degrades cleanly: a class without semantic elements behaves as today.
- **S6.** Language neutral: what is published in the database is read the same way by Python and protoST.
- **S7.** The vector suggests, the data decides: anything inferred is a hypothesis, never a fact.

**Normative** is what lives in the database:
- the catalog (section 8);
- the expression language (section 7);
- the intent log and the commit semantics (section 9).

They are specified as format data with conformance vectors. The declaration syntax of sections 4-6 is
illustrative. Each language declares the same thing its own way and must produce the same catalog (the canonical
form, section 8). A feature that cannot be described as neutral data does not enter the catalog: it stays a local
convenience of one language and is catalogued as such (section 8, `languages`).

## 2. What the MVP contains

| Piece | Depth in the MVP | Steps |
|---|---|---|
| Catalog (feature `catalog`, format 6) | Complete format, with conformance vectors | 1-6 |
| Descriptors | `Field`, `Computed`, `Aggregate`, `Relation`, `RelationTo`, `Role`, `Relationship`; `describe()` | 1, 3 |
| Modules (on D62) | Declarative contributions, provenance, verification against the stored catalog; permanent installs | 2 |
| Relations | Automatic inverse, indexable collections at both ends, relationships with data, `binding="version"` | 3 |
| Intents | Fine read set, intent log, replay at commit, `link`/`unlink`, `add_line`; one node, behind a flag | 4 |
| Invariants | One maintained aggregate (credit) and cardinality; errors with meaning | 4 |
| Expressions | `attr`, `const`, arithmetic, comparison, logic, `sum`/`count` over a role, `via` | 1, 4, 5 |
| Agent interface | MCP: `list_entities`, `describe`, `check_answerable`, `query` (with `explain()` and version) | 1, 2, 4, 5 |
| Interoperability | protoBaseST reads the catalog and queries (another repository) | 6 |
| Experiments | Concurrency (direct against intents); agent (20-30 questions, with and without catalog) | closing |

## 3. The demo script and its domain

A mini ERP: a company, customers with a credit limit, products, orders with lines. One command runs all steps.

1. **The database describes itself.** A database with module `acme_sales` installed. Through MCP, an agent lists
   the entities and explains what an order is, what a line is, and what "price" means on a line (the agreed
   price, not the current one).
2. **A module extends another.** `acme_delivery` is installed: it adds the delivery date to the order and
   redefines the label of the order number. The agent says which module defined each field.
3. **Relations navigable from both sides.** Orders with several lines are loaded. From an order its products are
   reached; from a product, its sales by date, with no global structure. An invoice shows the product as it was
   when sold.
4. **Coexist or collide by meaning.** Several processes create orders for the same company concurrently.
   Independent orders all commit; one that, added to a concurrent one, exceeds the credit limit fails with a
   message that names the rule. The agent explains it and proposes alternatives.
5. **Questions with verifiable answers.** "How much of product X did we sell in June?", "Which customer is
   closest to its limit?", "How much did we spend on advertising?" (not answerable: the agent detects it). Each
   answer carries the query, its `explain()` and the version read.
6. **Another language, the same database.** protoBaseST opens the same database, reads the same catalog and
   queries the same orders.

Closing: two numbers. Conflicts and violations detected in the current (direct) mode against the intent mode,
and correct answers of an agent with and without the catalog.

**The demo is deterministic** *(R266)*: the demo script drives the MCP server with a scripted client and prints
each tool call and response; no language model runs in the demo. A live agent runs only in the agent experiment.

**Domain.** The brief's type names and declarations are used. The demo's text is in English (the repository's
language rule), where the brief's examples are in Spanish. Module `acme_sales` declares:

| Type | Declaration, after the brief |
|---|---|
| `acme.core.Company` | `name`; inverse ends `customers`, `orders`, `products` |
| `acme.sales.Customer` | brief blocks 8 and 7: `name`, `credit_limit`, `company`, `orders` (one-to-many), `committed` (maintained `Aggregate` over `orders`, `where="state != 'cancelled'"`), the credit `Invariant` |
| `acme.catalog.Product` | `name`, `list_price = Field(Decimal)`, `company`; inverse end `order_lines` |
| `acme.sales.Order` | brief block 1 plus `date`; `total` written as a maintained `Aggregate(over="lines", sum="amount")` instead of the lambda (neutral, R276); intents `add_line` and `confirm` (block 6) |
| `acme.sales.OrderLine` | brief block 4; `amount = Computed("quantity * price")` instead of the lambda |

The aggregate's `where` is written `state != 'cancelled'` rather than the brief's `state in ('open','confirmed')`:
the two are equivalent over the declared enumeration, and `in` is outside the MVP subset (section 7).
Module `acme_delivery` (depends on `acme_sales`) is brief block 2, `class OrderDelivery(Extension)`.

## 4. Declaring entities (Python) and how it maps onto rc4

The user-facing declaration is the brief's (blocks 1, 2, 4, 6, 7, 8):

```python
from datetime import date
from decimal import Decimal
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Field, Enum, Relation, RelationTo, Aggregate, Invariant, intent, intent_context

sales = module("acme_sales")

# class OrderLine(Relationship): ... (brief block 4) is declared above; RelationTo also accepts its type name.

class Order(DBObject):
    __type_name__ = "acme.sales.Order"
    __meaning__ = "A customer's order to the company; open, confirmed or cancelled"
    __label__ = "Order"
    __synonyms__ = ["purchase order"]
    __key__ = ("company", "number")
    number = Field(int, required=True, label="Number", meaning="Sequential per company, assigned at creation")
    state = Field(Enum("open", "confirmed", "cancelled"), default="open")
    date = Field(date)
    company = Relation("acme.core.Company", cardinality="many-to-one", inverse="orders")
    customer = Relation("acme.sales.Customer", cardinality="many-to-one", inverse="orders")
    lines = RelationTo(OrderLine, role="order", ordered=True)
    total = Aggregate(over="lines", sum="amount", maintained=True, unit="currency:order")

    @intent(meaning="Add a product to the order with an agreed quantity and price",
            commutes_with=["add_line", "remove_line"])
    def add_line(self, product, quantity, price):
        intent_context().relate("acme.sales.OrderLine", order=self, product=product,
                                quantity=quantity, price=price)

    @intent(meaning="Confirm the order; it accepts no new lines afterwards", requires="state == 'open'")
    def confirm(self):
        return self.with_(state="confirmed")
```

**The mapping onto rc4** *(R259)*. rc4 accepts every spelling above except as noted. The brief's spelling is kept
for every user-facing declaration, and the mapping is internal:
- **The entity class.** A `DBObject` subclass with `__type_name__` and at least one semantic element (a
  descriptor, an intent, `__constraints__` or a semantic dunder) is a **semantic class**.
  - It is a D57 registered atom class: its records are of type `acme.sales.Order`, with today's envelope (no `"p"`).
  - At class creation (`DBObject.__init_subclass__`) its semantic elements are moved out of the Python class into
    an implicit D62 extension of the registry name `__type_name__`. The registry module is the one its Python
    module declared with `module(...)`; `__registry_module__ = "name"` disambiguates when that module declares
    several. None is an error.
  - The Python class keeps its other members. So D62's rule R240 (a registry definition may not shadow a Python
    member) never forbids a later module from redefining a field or an intent.
  - A class with no semantic element is untouched (S5).
- **Resolution.** An instance of a semantic class resolves `__type_name__` as an implicit last parent, after its
  persisted parents; it is not persisted. This is the one rc4 behaviour addition, and it applies to semantic
  classes only. A stored field is an own attribute and is found first, at today's cost. A descriptor found in the
  chain resolves dynamically:
  - a `Field` gives its default (a miss gives `None`, R150);
  - a non-stored `Computed` evaluates;
  - a `Relation` or `Role` navigates.

  Python's descriptor protocol is not what resolves them (the definitions live in the registry chain), but
  `__set_name__` names them as the brief says.
- **`Extension`.** `class OrderDelivery(Extension): __extends__ = "acme.sales.Order"` declares the D62 extension
  `module.extends("acme.sales.Order")` with the class body as definitions. The base class `Extension` replaces the
  decorator spelling; the decorator stays valid and equivalent. rc4 refuses container classes with a base, so the
  semantic layer hands the body to the registry through an internal entry point.
- **`describe()`** keeps the brief's spelling as a class method, `Order.describe(snapshot)`, added only to semantic
  classes (so `DBObject`'s API and reserved names do not change). "Describe as a photograph": the argument is a
  read snapshot, an rc4 `Snapshot` or a `Transaction` (which is one), because composition is per database and
  per version (section 5). Without a snapshot it raises
  `ProtoUserError` naming the reason. It returns neutral data (dicts, lists, strings).
- **Field types** are Python types or `Enum(...)`, mapped to the catalog's `value_type` (section 8): `int`,
  `Decimal`, `str`, `bool`, `bytes`, `float`, `date`, `time`, `datetime`, `timedelta`, `uuid.UUID`.
  `Enum("open", ...)` gives `value_type` `str` with `values` *(R268)*. The MVP catalogues `values` and does not
  enforce them: enforced attribute constraints are outside the MVP, and the catalog marks this.
- **Lambdas and text.** `Computed(lambda self: ...)` (blocks 1, 4) is accepted, as a local convenience: non-stored,
  catalogued with `kind` `computed`, no `expression`, and `languages: ["python"]`. Another language describes it
  but cannot evaluate it (`ProtoNotSupportedError`) *(R276)*. A text form (`Computed("quantity * price")`,
  `Invariant("committed <= credit_limit")`, `requires="state == 'open'"`, `where="state != 'cancelled'"`) is
  parsed into the neutral tree of section 7, which is what is stored *(R277)*. A maintained aggregate needs
  neutral terms all the way down.
- **Intents** keep the brief's signature (`def add_line(self, product, quantity, price)`):
  - the effects go through `intent_context()`, which is valid only while the intent runs (outside it,
    `ProtoUserError`) *(R278)*;
  - an intent that returns a new version of its receiver (`self.with_(...)`, the rc4 idiom) updates the target;
  - calling an intent on a version obtained from a `SemanticSession` logs and applies it (section 9); on any other
    version, `ProtoUserError`.
- **Constraints** are written `__constraints__ = [Invariant(text, meaning=..., enforced=..., message=...)]`, an
  entity-level element. A constraint without `name=` is named `invariant_` plus the first 8 hex digits of the
  SHA-256 of its tree's canonical encoding. That name is deterministic and the same in every language *(R279)*.
  The scope is inferred: `aggregate` when the expression reads a maintained aggregate, else `entity`.
- **`__validity__`** (block 5, D72) raises `ProtoNotSupportedError`: the `Validity` descriptor is out of the MVP.

### Subclasses (R280)

"A subclass adds attributes and constraints" (the brief). Subclasses are supported with one limit, kept for a
fundamental reason.
- **Declaration.** `class RushOrder(Order): __type_name__ = "acme.sales.RushOrder"` (the tests' fixture module
  `acme_rush`; the demo does not need it) plus its own elements is a semantic class (it inherits semantics, even with no semantic element of its own). Multiple inheritance from
  several semantic classes is allowed.
- **Mapping onto D62.** The implicit extension of the subclass's type name declares
  `parents=(supertype names, in Python base order)`. D62's registry-name parents then put the supertypes after the
  subtype in the C3 linearization:
  - an instance resolves its own attributes, then the subtype's chain, then each supertype's chain;
  - an extension of a supertype, by any module, reaches subtype instances;
  - `_super()` inside a subtype's intent reaches the supertype's implementation.

  Python's own MRO still gives the subclass the Python members of its bases (the semantic ones were moved out).
- **Composition.** The composed subtype is the fold of its linearization from the last to the first: the
  supertypes' composed elements (each with its own modules' contributions), then the subtype's chain.
  - `supertypes` lists the direct supertypes.
  - Every inherited element (attribute, role, intent and constraint) keeps its provenance, and carries
    `inherited_from` (the type that declared it) *(R281)*.
  - Of the entity-level fields a subtype inherits only `key`, which it may not redefine (instances are read
    through the supertype's declaration); its `meaning`, `label`, `help`, `synonyms` and `certification` are its
    own, absent unless it declares them.
  - A subclass may add any element, and may redefine what section 5 lets a module redefine (presentation,
    meaning, defaults, a computed's expression, `enforced`, an intent's implementation).
  - With several supertypes, C3 decides which definition comes first, exactly as resolution does. It is never an
    error.
- **Substitutability.** A role or relation typed by a supertype accepts subtype instances: `link` and `relate`
  check that the target's type is the declared type or one of its subtypes (the closure of `supertypes` in the
  composed catalog). A `via` and a query over such a collection yield instances of every subtype. A predicate on
  an attribute only a subtype has gives MISSING on the others, so it is false there, which is sound. A query may
  narrow by type with `"of_type"` (subtypes included). Inherited invariants and maintained aggregates apply to
  subtype instances. Cells are named by the instance's own type.
- **The one limit** *(R282)*. A subclass may not change the `kind`, the `value_type` of a stored field, or the
  `cardinality`, `target` or `binding` of a role it inherits: the same list section 5 refuses to modules. Reason:
  a role, a query, an aggregate or an invariant typed by the supertype reads and writes every subtype instance
  through the supertype's declaration. A changed kind or type would break that for data written through the
  supertype. Narrowing a role's `target` to a subtype would let a write through the supertype store a target the
  subtype's declaration refuses. These are soundness conditions (Liskov substitutability), not implementation
  limits.

## 5. Modules and composition (normative)

The behaviour of modules is D62 as published. This section adds the declarative part.
- **Deterministic order.** Contributions fold in D62's load order. The last module that defines an element wins.
- **Freely redefinable:** `label`, `help`, `group`, `meaning`, `synonyms`, `caveats`, `examples`, `default`,
  `sensitivity`, `deprecated`, the expression of a computed, the `enforced` of a constraint, and an intent's
  implementation (through `_super()`).
- **A redefinition may restate structural fields** (block 2 restates `Field(int, required=True, ...)`). They must
  equal the composed ones. A field the redefinition omits is unchanged.
- **Addable:** attributes, roles, relations, intents, constraints.
- **Refused without a declared migration** (`ProtoValidationError` at install and at composition, naming the
  module, the type, the element and the field): the `kind` of an attribute, the `value_type` of a stored field, the
  `cardinality`, `target` or `binding` of a role. Migrations are outside the MVP: such a change is always refused.
- **Nothing is deleted.** A module never removes another's element; it may mark it `deprecated`. A module is never
  uninstalled. It evolves through a new version or an extending module. The MVP builds no new-version path: code
  whose contributions differ from the stored ones raises (verification below) *(R263)*.
- **Provenance.** Every composed element lists the module that defined it, then the modules that modified it, in
  load order.
- **Per database.** Composition depends on the database's installed modules.
- **Inverses.** A relation's cardinality gives both ends: `many-to-one` is `one` here and `many` at the inverse,
  and `one-to-many` the reverse. `one-to-one`, `many-to-many` and the end forms `one`, `many`, `0..1` and `1..*`
  are accepted too. A `Role` of a relationship is `one` at its end. The inverse end is created on the target
  unless the target declares it (as `RelationTo` does); an explicit declaration must agree on target, inverse
  and cardinality, or composition raises.

Loading (normative, all languages):
1. read `__currentModules`, ordered as in D62;
2. load each module's local code and its catalog contributions;
3. compose;
4. **verify that the local composition equals the stored catalog.** A difference is an explicit
   `ProtoValidationError`, never corrected silently.

`install_module` also adds, add-if-absent at commit, the semantic contributions of the module and its
dependencies, and the recomposed entities. A database whose modules were installed before the semantic layer
existed gets its contributions at its next `install_module`. Until then the semantic layer raises
`ProtoNotSupportedError` ("no semantic catalog").

## 6. Relations

- **`Relation`** (simple link, no data); **`Relationship`** (Chen: `class OrderLine(Relationship)`, whose
  instances are atoms with their own fields; its `Role` descriptors are its roles); **`RelationTo(cls, role=...)`**
  (the inverse end of a relationship's role, declared on the other entity). All are catalogued as
  `proto_db.catalog.Role` records.
- Role properties: `inverse`, `cardinality`, `ordered`, `index_by` (attribute paths), `binding` (`identity`,
  `version`), `on_delete` (`restrict`, `cascade` or `unlink`; `restrict` when unstated, and omitted at that
  default; catalogued only, the MVP deletes no entity).
- **Identity** *(R260)*:
  - an entity that changes lives in a mutable cell named `<type name>#<32 hex digits>`, whose value is a record
    of the semantic class;
  - its `MutableObject` handle is the identity reference;
  - the hex digits are a UUID drawn when the creation is recorded and carried as an argument (replay never draws
    it).
- **Collections at both ends.** A `many` end is an attribute of the entity's version holding a `List` (ordered) or
  a `Set`. A relationship's atom lives once; both ends reference the same atom.
- **`binding="version"`** *(R260)*: the role stores `{"entity": <handle>, "version": <ref>}`. Navigation gives
  the version (block 4: the product as sold), and `.entity` reaches the identity. The bound version is the one
  current when the operation is applied (direct mode: the transaction's view, validated at commit; intent mode:
  at replay): the target's version as of its last *field* change. A binding binds the committed record the transaction read
  while the target's fields equal it; otherwise the transaction's kept record for the target while the fields
  equal it; otherwise the record in view, which becomes the kept record (the rule reads the fields only, so an
  update to the same value keeps the committed record bound, and fields back to a kept state bind the kept
  record again). Writes to the target's own ends and maintained
  aggregates never start a new bound version, and a later field change does not move an earlier binding.
  ("Current at commit" is unsatisfiable: the product's committed record holds the line, and immutable records
  hold no cycle.) The data layout is the format spec's "Entity data" section.
- **`index_by`** keeps the brief's paths *(R262)*:
  - A path of the member's own attributes is a `RangeIndex` named `by_<path>` on that end's collection.
  - A path through an identity role (block 4: `index_by=["order.date"]` on `product`) is a **maintained key**,
    since an index key must be a pure function of the indexed element (the extractor contract, D56). The end
    collection then holds entry records `{member, key}` (plain `DBObject`s, format 4), indexed by
    `RangeIndex("by_order_date", "key")`. An update of `order.date` re-keys the entries of every line of that
    order in the same commit. Navigation returns the members, never the entries.

## 7. Expression language (MVP subset; normative)

An expression is a nested list `[operator, argument...]`, stored as data and evaluated the same way in every
language. Its depth (the outermost list at 1, an operand one deeper than its operator) is at most
`MAX_EXPRESSION_DEPTH` = 128; a deeper tree is `ProtoValidationError`.
- `["const", v]`: a canonical value.
- `["attr", name, ...]`: a path from the current element. Each step but the last is a `one` role or relation. An
  absent step gives MISSING.
- **Absent values**: `["attr", name]` reads the element's stored own attribute, then the field's catalogued
  `default` when there is one, else MISSING. It never yields the registry's `None` (R150).
- Arithmetic `["+"|"-"|"*", a, b]` *(R264)*:
  - int with int gives int; any decimal gives decimal, exact, by the General Decimal Arithmetic: an int is a
    decimal of exponent 0, the result's exponent is the smaller one for `+` and `-` and the sum for `*`;
  - both operands are checked first: bool, float, strings, nil, a non-finite decimal and other kinds raise
    `ProtoValidationError`, even beside MISSING;
  - then MISSING in gives MISSING;
  - division is deferred.
- Comparison `["==", "!=", "<", "<=", ">", ">=", a, b]`: D28's semantics (`values_equal`, type ranks,
  `order_key`). A comparison involving MISSING is false.
- Logic `["and", a, ...]`, `["or", a, ...]` (one operand or more), `["not", a]`. Logic operands are booleans, evaluated left to right, `and` stopping at the first false and `or` at the first
  true. A MISSING operand counts as false (as in a comparison), so `not` of MISSING is true. Any other
  non-boolean operand that is reached raises `ProtoValidationError`.
- Aggregates `["sum", over, term, where?]` and `["count", over, where?]`:
  - `over` is a role name of the current element, a `via` expression, or (in a query) a bare root;
  - `term` and `where` are evaluated with each member as the current element, in the members' order;
  - a member for which `where` is not true (false or MISSING) is not a member; another kind raises;
  - `count` is the int number of members; `sum` is the left fold of `+` from the int 0 over the terms (so the
    int 0 over none). A member whose `term` is MISSING contributes nothing.
- Traversal `["via", origin, role, role...]` (one role or more): the set reached through each end's own
  collection. Origins *(R265)*: `["self"]`; in queries only, `["root", name]` and `["cell", name]`; in a query
  `["root", name]` is also a bare root, the members of that root.
  - Members come in first-reached order (each member of the previous hop in its order, then its end's collection
    in its iteration order); a member reached twice counts once, identified by its handle's cell name, a
    version-bound reference by (entity cell name, version oid), a record by its oid; any other value in a role
    raises `ProtoValidationError`.
  - A handle is followed to its cell's record (an absent cell contributes nothing), a version-bound reference to
    its version.
- **Text form** *(R277)*: a Python convenience, parsed into the tree. It covers identifiers and dotted paths
  (`attr`), literals (`'text'`, integers, decimals), `+ - *`, the six comparisons, `and`/`or`/`not` and
  parentheses. Anything else is `ProtoValidationError` at declaration. The stored tree is the normative form; the
  text is not stored.
- A predicate `[cmp, ["attr", name], ["const", v]]` (or mirrored), and `and`/`or`/`not` of such, translates to the
  planner's `Term`/`And`/`Or`/`Not` (D36, whose syntax is the 0.1 prefix form `path, op, value`), so indexes
  answer it. Any other predicate runs as a residual. `explain()` shows which.

Out of the MVP (the format reserves them): `in`, `is_missing`, `min`, `max`, `exists`, `all`, `reach` and
`valid_at`.

## 8. The catalog (format 6, feature `catalog`; normative)

New built-in atom types, aliases appended to `BUILTIN_TYPE_ALIASES` from 24 on (append-only). A record of one of
them uses the feature `catalog` and is version 6; data that uses none stays version 4 or 5, byte for byte.

| Type | Fields |
|---|---|
| `proto_db.catalog.SemanticCatalog` | value of `__semanticCatalog`: `modules` (ref, Dictionary name -> Module), `module_set` (tuple of names in code-point order), `entities` (ref, Dictionary type name -> Entity or Relationship, the composition for `module_set`) |
| `proto_db.catalog.Module` | `name`, `version` (string; the MVP writes `"1"`), `depends` (tuple), `extensions` (tuple of refs, declaration order) |
| `proto_db.catalog.Extension` | `module`, `extends` (type name), `entity` (map of the entity-level elements it contributes, and `kind`: `entity` or `relationship`), `elements` (tuple of refs to the Attribute, Role, Intent and Constraint records it declares, with the fields it states) |
| `proto_db.catalog.Entity` | `type_name`, `supertypes`, `meaning`, `label`, `help`, `synonyms`, `key`, `attributes` (refs to Attribute and Role), `intents`, `constraints`, `certification`, `provenance` |
| `proto_db.catalog.Relationship` | the Entity fields plus `roles` (refs to its Role records, two or more, each with `participant` true) |
| `proto_db.catalog.Attribute` | `name`, `inherited_from` (composed subtype entities only, R281), `kind` (`field`, `computed`, `computed_stored`), `value_type`, `values` (enumeration, R268), `required`, `default`, `unit`, `indexed`, `expression` (ref), `languages` (R276), `meaning`, `label`, `help`, `group`, `order`, `synonyms`, `examples`, `caveats`, `sensitivity`, `certification`, `deprecated`, `provenance` |
| `proto_db.catalog.Role` | the Attribute fields plus `target`, `inverse`, `cardinality`, `inverse_cardinality`, `participant`, `ordered`, `index_by`, `binding`, `on_delete` |
| `proto_db.catalog.Intent` | `name`, `inherited_from` (R281), `meaning`, `parameters` (tuple of `(name, value_type)`), `requires` (ref), `commutes_with`, `effects`, `languages`, `provenance` |
| `proto_db.catalog.Constraint` | `name`, `inherited_from` (R281), `scope` (`attribute`, `entity`, `relation`, `aggregate`), `expression` (ref), `enforced`, `meaning`, `message`, `provenance` |
| `proto_db.catalog.Expression` | `ast` (the tree of section 7) |
| `proto_db.catalog.IntentRecord` | `intent` (`(type name, intent name)`), `target` (handle), `arguments` (map), `transaction` (the database version the commit produced) |

**Field encodings** (the same in the design spec, section 8). A field is **required** where listed under "Required fields" below;
every other listed field is **optional**, and absent means its default (below) or, where none is
given, "not declared". The value kinds are those of the format (`str`, `bool`, `int`, `@tup`, `@map`,
refs):
- Text fields (`meaning`, `message`, `group`, `unit`, `certification`, `caveats`, `on_delete`,
  `sensitivity`, `scope`, `kind`, `binding`, `value_type`, `type_name`, `name`, `module`, `extends`,
  `version`, `inverse`, `inherited_from`, `target`) are `str`.
- Lists of names are a `@tup` of `str`, in declaration order: `supertypes` (the direct supertypes, in the
  language's base order), `key` (attribute names), `synonyms`, `depends`, `commutes_with` (intent names),
  `index_by` (attribute paths, dotted), `values` (the enumeration), `languages` (language names, non-empty
  when present), `effects` (free text, informative) and `provenance` (module names: the module that defined
  the element first, then those that modified it, in load order). `module_set` is a `@tup` of `str` in
  code-point order.
- Lists of elements are a `@tup` of refs: `attributes` (Attribute and Role records), `intents`,
  `constraints`, `roles`, `extensions`, `elements`. A composed list keeps the position of an element's first
  definition, and an element added by a later module goes after the existing ones, in load order.
  `modules` and `entities` are refs to Dictionaries from `str` to a ref (module name to Module, type name to
  Entity or Relationship).
- `label` and `help` are a `@map` from a language tag to `str` (a plain string is `{"und": text}`).
- `examples` is a `@tup` of canonical values. `default` is a canonical value; its absence means no default
  is declared (a declared default of nil is present as nil). `order` is an `int`. `parameters` is a `@tup`
  of `(name, value_type)` pairs, each a `@tup` of a `str` and a `value_type`: a value kind, an entity type
  name (a parameter that takes an entity, carried as its cell name), or nil for a parameter whose type is not
  declared (an explicit "unknown", never guessed). The first writer writes no `effects`.
- `required`, `indexed`, `ordered`, `participant`, `enforced` and `deprecated` are `bool`. `participant` is
  true for a role of a relationship and false for the end of a plain relation; a Relationship lists its
  participant roles in `roles`, never in `attributes`.
- `cardinality` and `inverse_cardinality` are the two ends of the relation as `str`, one of `one`, `many`,
  `0..1` and `1..*`: `cardinality` is the end at the role's own entity, `inverse_cardinality` the end at its
  `target`. A declaration written as a pair (`many-to-one`, `one-to-many`, `one-to-one`, `many-to-many`)
  is stored as these two. `binding` is `identity` or `version`. `kind` is `field`, `computed` or
  `computed_stored`. `scope` is `attribute`, `entity`, `relation` or `aggregate`.
- `expression` and `requires` are refs to an Expression record; `ast` is the tree of the expression
  language, nested `list` values whose operator and names are `str`.
- `Extension.entity` is a `@map` whose keys are among `kind`, `meaning`, `label`, `help`, `synonyms`, `key`,
  `supertypes` and `certification`: `kind` is `entity` or `relationship` (default `entity`, structural), each
  other key has the encoding of the Entity field of that name. An Extension that declares a participant role
  states `kind` `relationship`, so composing from the contributions reproduces the stored record exactly.
- `IntentRecord.intent` is a `@tup` of two `str`; `target` is the `str` name of the target's cell
  (`<type name>#<32 hex digits>`); `arguments` is a `@map` from parameter name to canonical value (an entity
  argument is carried as its cell name); `transaction` is an `int`.
- **Required fields.** `SemanticCatalog`: `modules`, `module_set`, `entities`. `Module`: `name`, `version`.
  `Extension`: `module`, `extends`. `Entity` and `Relationship`: `type_name` (and `roles` for a
  Relationship). `Attribute`: `name`, `kind`. `Role`: `name`, `target`, `cardinality`. `Intent`: `name`.
  `Constraint`: `name`, `scope`, `expression`. `Expression`: `ast`. `IntentRecord`: all four.
- A Role carries the Attribute fields except `kind`, `value_type`, `values`, `default`, `unit` and
  `expression`: a role is stored and holds entities, not a value of a kind.
- **Omission.** Defaults are omitted, in the records and in the canonical form: absent text, an empty `@tup`
  or `@map`, `required` false, `indexed` false, `enforced` true, `sensitivity` `internal`, `binding`
  `identity`, `on_delete` `restrict`, `ordered` false, `participant` false, `deprecated` false and
  `Extension.entity`'s `kind`
  `entity`; a map of such fields is at its default when they all are. A writer writes no field at its default; a reader
  that meets one accepts it and its canonical form omits it. Every other optional field is written when it is
  declared.

Field rules:
- `values`, `languages`, `inverse_cardinality` and `inherited_from` are additions to the brief's field lists (R268, R276, section
  5).
- `label` and `help` are maps from language to text *(R267)*:
  - a plain string, as in every example of the brief, declares `{"und": text}` (BCP 47 "undetermined"), since the
    examples carry no language;
  - a dict declares languages explicitly;
  - translations are outside the MVP.
- `value_type` names a canonical value kind *(R268)*: `bool`, `int`, `decimal`, `str`, `bytes`, `date`, `time`,
  `datetime`, `timedelta`, `uuid`, `float`, `list`, `tuple`, `map`. Smalltalk's `#integer` and `#decimal` are
  `int` and `decimal`.
- **`ext`.** Every record may carry `ext` (a map from string to value). Readers keep it byte for byte and never
  interpret it.
- **Single-language elements.** An intent or computed implemented in one language only carries `languages`.
  Invoking or evaluating it from another language raises `ProtoNotSupportedError`.
- **Canonical form** *(R258)*: the composed catalog expanded into one value (every ref replaced by its record's
  fields, oids and pointers dropped, Dictionaries as maps sorted by key), encoded canonically. Two implementations
  conform when their canonical forms are equal byte for byte. Record bytes are not compared: oids and placement
  differ by writer.
- **The composed `entities` are a cache.** A reader may use them when `module_set` equals the set of
  `__currentModules`. Python always recomposes and verifies (section 5).
- **Roots by name and type.** `__semanticCatalog` and `__intentLog` are identified by name and by type, as the
  registry roots are.

## 9. Intents (one node, behind a flag)

The flag is `Database.new_transaction(intents=True)` or `Database.run(fn, intents=True)` *(R269)*. Without it
nothing changes. A space with a cluster coordinator refuses the flag (`ProtoNotSupportedError`). `semantic(tr)`
gives a `SemanticSession`, with one API in both modes:
- in **direct mode**, operations write cells at once, under today's rules;
- in **intent mode**, operations are logged and replayed.

What an intent transaction records:
- **Fine read set** *(R270)*: every attribute read through the session, as `(cell, attribute) -> exact token`
  (ABSENT included). Reading a collection attribute is one read of that attribute. Reading a product's price
  therefore does not collide with a line added to its `order_lines`.
- **Condition reads** *(R261)*: a traversal or query whose result is used to decide is recorded as the tokens of
  every collection it scanned, and validated as such. This is coarse, never wrong. Predicate-precise validation
  is deferred.
- **Intent log**: each top-level operation in order, either an intent invocation or a primitive (`create`,
  `update`, `link`, `unlink`, `relate`).

The commit, at the serialized point (the root lock; group-commit rounds build each commit on the previous one):
1. validate the fine read set and the condition reads against the current cells; a change is
   `ProtoConflictError`;
2. replay the log on the current state, maintaining both ends of every relation, maintained keys and maintained
   aggregates;
3. check the affected invariants on the combined result (section 10);
4. publish, writing in `__intentLog` the commit's `IntentRecord`s *(R271)*. There is one root value per version;
   history is the chain of versions.

Requirements:
- **Deterministic.** An intent reads nothing external during replay: external values travel as arguments, as in
  Datomic transaction functions. This is documented, not policed (P2).
- **`requires`** is evaluated at replay, on the target.
- **`commutes_with`** is catalogued only: the MVP always revalidates (in doubt, nothing commutes).
- **Mixed raw writes.** A raw `set_mutable` of a cell the log also touches is refused (`ProtoUserError`). Other
  raw reads and writes keep today's rules.

## 10. Constraints and invariants

- **Maintained aggregates** *(R272)*. An `Aggregate(..., maintained=True)` is catalogued as a `computed_stored`
  attribute with a `sum` or `count` tree.
  - It is stored in the owning entity's version.
  - It is updated by deltas whenever a member is linked, unlinked or changes. The delta comes from the member's
    `term` and `where` before and after the change, in the escrow style (O'Neil, 1986). It is never a full
    recomputation.
  - In intent mode the replay applies the deltas at the serialized point.
  - Its terms may be fields, neutral computeds over the member's own fields, or other maintained aggregates.
- **Cardinality** is enforced at commit for every entity the commit touched.
- **Enforced invariants** are evaluated after replay on every entity whose operands changed. `enforced=False`
  publishes an invariant for agents only.
- **Errors with meaning.** A violation raises `ProtoConstraintError` *(R273)*. It is a `ProtoValidationError`,
  so `Database.run` never retries it.
  - It carries the constraint's `name`, its `meaning`, the rendered `message`, the entity and the values used.
  - Message placeholders *(R279)*: the entity's attribute names; `{entity}`, and the lower-case last component
    of its type name (`{customer}`), both rendering the entity's key; and, for an ordering comparison, `{left}`,
    `{right}` and `{excess}` (left minus right). The brief's `{exceso}` is therefore written `{excess}`.
  - A MISSING operand renders as `<missing name>`, and its name is in the values.
  - Never "conflict on root X". Nothing of the failed commit is published; in a group-commit round only that
    commit fails.
- **Direct mode** checks enforced invariants when the operation is applied, on the transaction's view. Every
  value the check reads enters today's read set, so the commit's validation keeps the check valid.

## 11. Agent interface

Neutral operations over the catalog and the planner (`proto_db.agent`), served by a small MCP server.

| Operation | Input | Output |
|---|---|---|
| `list_entities` | optional filter text | type names with `label` and `meaning` |
| `describe` | type name | the composed entity: attributes, roles, intents, constraints, provenance, certification |
| `check_answerable` | a list of terms *(R274)* | per term its matches (entity, attribute, role) by name, label or synonym (case-folded, exact); the unmatched terms; suggestions by substring, marked as suggestions (S7) |
| `query` | a query tree, optional `as_of` (a version) | rows, `explain()`, the version read, the query, the catalog terms used with their certification |

The query tree is `{"from": <via expression>, "of_type": <type name>? (subtypes included), "where": <predicate>?, "select": [names]?, "aggregate": ["sum",
term] | ["count"]?, "as_of": version?}`.

Rules:
- Every answer carries provenance.
- `personal` and `secret` attributes are listed by `describe` and omitted by `query`.
- Agents never write: intent execution and `propose` are outside the MVP.
- A violated constraint is reported with its `name`, `meaning` and `message`.
- A **raw mode** of the server exposes only the stored structure (root and cell names, raw records, the same
  `query`), for the agent experiment's control group.

## 12. Interoperability (step 6; another repository)

protoBaseST reads `__semanticCatalog`, describes entities and runs the step-6 queries through generated accessors:
no new opcodes, no writes. Brief block 9 is its declaration spelling, and it must reach block 8's canonical form.
Entity records are typed by `__type_name__`, so a process without the class loads them as `OpaqueAtom` (D59), and
they stay describable. ProtoBase hands over the vectors, a porting note and an acceptance test.

## 13. Decisions to record (D63-D72, the brief's order)

| D | Content | MVP |
|---|---|---|
| D63 | The class as the entity's descriptor; S1-S7; what is normative; the mapping onto rc4 and subclasses (section 4) | built |
| D64 | Catalog atom types and their encoding: feature `catalog` of format 6, `__semanticCatalog`, canonical form | built |
| D65 | Neutral expression language: tree, operators, evaluation with the total order; the Python text form | subset |
| D66 | Relations: inverse, collections at both ends, relationships with data, `binding`, maintained keys | built |
| D67 | Intents: fine read set, condition reads, log, replay at commit | one node, flag |
| D68 | Constraints: scope, `enforced`, maintained aggregates, errors with meaning | aggregate + cardinality |
| D69 | protoST `PUSH_DESCRIPTOR` / `STORE_DESCRIPTOR` | designed, deferred |
| D70 | Modules and class extension: the declarative part over D62, `Extension`, provenance, migration refusals, per database, evolution without uninstall | built |
| D71 | Traversal operators and their validation as condition reads | `via` only |
| D72 | The `Validity` descriptor (valid time beside transaction time) | designed, deferred |

## 14. Out of scope (the list for later)

Intents in the cluster; complete condition-read (phantom) validation; catalog migrations (refused explicitly);
protoST descriptor opcodes and intent writes from Smalltalk; RDF/OWL/SKOS/SHACL export; relation discovery by
vectors and `resolve_terms` by similarity; enforced entity and attribute constraints in general (`Enum` values
included); label and help translations; `exists`, `all`, `reach`, `valid_at`, `in` and `Validity`;
`explain_intent` and `propose`; module new versions; the D16-style static test that every element has a
`meaning`; entity deletion.

## 15. Done criteria

- Steps 1-6 run from one script, with one command, with no manual intervention, in under 5 minutes, on a clean
  machine.
- The catalog vectors pass in Python and in protoBaseST, with identical canonical forms.
- Installing the modules in different declaration orders produces the same composed catalog.
- In step 4: zero independent orders rejected by conflict, and every credit-limit violation detected, against a
  verifier that walks the history.
- The concurrency experiment publishes its numbers with the commit that produced them, as `performance.rst` does.
- The agent experiment publishes the questions, both modes' answers and their correctness.
- The agent detects the advertising question as not answerable.
- A one-page demo README says what each step shows and where to look in the code.
- The existing test suite still passes: code that uses no semantic layer keeps today's API, bytes and behaviour.

## 16. The brief's open questions

- *Maintained aggregates: in the entity or in a commuting structure?* In the entity's version, by deltas at replay
  (R272). Additions do not contend; a reader of the aggregate conflicts, correctly.
- *Minimal granularity of the fine read set?* `(cell, top-level attribute)` (R270); Task 10 measures its cost.
- *Intents with the cluster coordinator?* Out of the MVP (the script); the flag is refused on a clustered space.
- *Is a catalog migration an intent?* Out of the MVP (the script): such changes are refused explicitly.
- *Minimal expression subset?* Answered by the script (section 7).

## 17. Rulings (confirmed 2026-11-15)

Those marked **changes the brief** alter what the brief's examples wrote; the maintainer accepted them.
- **R258** Cross-language conformance compares the catalog's canonical form, not record bytes.
- **R259** The brief's declarations map onto rc4 (section 4):
  - a semantic `DBObject` subclass moves its semantic elements into an implicit extension of `__type_name__`, and
    its instances resolve that name as an implicit last parent;
  - `Extension` with `__extends__` is the decorator's equivalent.

  **Changes the brief:** `describe()` takes a read snapshot (`Snapshot` or `Transaction`), "describe as a
  photograph" (maintainer).
- **R260** Identity is a mutable cell `<type name>#<32 hex>` holding a record of the semantic class. A version-bound
  role stores `{"entity", "version"}`.
- **R261** Condition reads are validated by the tokens of the collections they scanned.
- **R262** `index_by` through an identity role (`order.date`) is a maintained key over entry records, re-keyed in
  the commit that changes the keyed attribute.
- **R263** Module evolution is designed, not built: `Module.version` is written as `"1"`, and differing code
  contributions raise.
- **R264** Arithmetic is `+ - *` over int and decimal; division and float are deferred.
- **R265** The expression origins are `self`, and `root` and `cell` in queries.
- **R266** The demo is scripted and deterministic; only the agent experiment runs a language model.
- **R267** `label` and `help` are language maps; the brief's plain strings are stored as `{"und": text}`.
- **R268** `value_type` is the canonical value kinds, mapped from Python types. **Changes the brief's field
  list:** `Enum(...)` adds the Attribute field `values`, catalogued and not enforced in the MVP.
- **R269** The flag is `intents=True` on `new_transaction` and `run`, refused on a clustered space.
- **R270** The fine read set is `(cell, top-level attribute) -> exact token`.
- **R271** `__intentLog` holds the IntentRecords of the version that wrote it.
- **R272** Maintained aggregates live in the owning entity's version, updated by deltas.
- **R273** `ProtoConstraintError` subclasses `ProtoValidationError` and is never retried.
- **R274** `check_answerable` takes terms (exact matches on name, label, synonyms).
- **R275** The concurrency baseline is direct mode under today's rules. The brief expects write skew there, but
  today's read set validates every cell the check reads. The experiment reports what it measures, even zero
  violations in both modes.
- **R276** A lambda `Computed` (blocks 1, 4) is a Python-only local convenience. **Changes the brief's field
  list:** the Attribute field `languages`. The demo writes neutral forms instead.
- **R277** Text expressions (`"committed <= credit_limit"`, `requires="state == 'open'"`) are a Python surface
  syntax over the stored tree, limited to the MVP subset.
- **R278** **Changes the brief's example bodies:** an intent keeps its signature and reaches its effects through
  `intent_context()`. Returning `self.with_(...)` updates the target.
- **R279** **Changes the brief:** an unnamed `Invariant` gets a name derived from its tree's hash, and message
  placeholders are attribute names, `{entity}`, the type's short name, `{left}`, `{right}` and `{excess}`.
- **R280** (maintainer) Subclasses are supported with minimal limits (section 4, "Subclasses").

### 17a. Subclass rulings (confirmed 2026-11-15)

- **R281** (confirmed 2026-11-15) Inherited elements of a composed subtype carry the new field `inherited_from`.
- **R282** (confirmed 2026-11-15) The one limit kept, for soundness: a subclass cannot change an inherited element's `kind`, stored
  `value_type`, or a role's `cardinality`, `target` or `binding` (section 4 gives the reason).

Conflicts with the brief found while writing, resolved above:
- the brief places the catalog in format 5 (superseded by the format-6 ruling);
- block 4's `index_by=["order.date"]` cannot be a plain index under the pure-extractor contract (R262);
- the brief calls the expression tree "the one the planner already uses", but D36's syntax differs (section 7);
- block 7's `where="state in (...)"` needs `in`, which is outside the script's subset;
- the brief's validation section expects write skew in the current mode (R275).
