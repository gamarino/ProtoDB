# 5. Modules and composition

A model is rarely one module. A sales module declares opportunities; a forecasting module, written later and
perhaps by another team, adds a close date to them; a customer-care module adds a kind of contact. None of
them edits the others. Each is a registry module that **depends** on the ones it builds on, and what the
database describes is the **composition** of the modules installed in it, with each element's provenance.

This chapter installs `examples/tutorial/erp_forecast.py` on top of `erp_model`:

```text
forecast = module("erp_forecast", depends=("erp_model",))

class OpportunityForecast(Extension):
    __extends__ = "erp.Opportunity"
    close_date = Field(date, meaning="The date the sale is expected to close")
    title = Field(str, required=True, label="Subject")        # the same field, a new label

class KeyAccount(Person):                                     # a subtype of erp.Person
    __type_name__ = "erp.KeyAccount"
    __meaning__ = "A contact at a customer the company manages as a key account"
    tier = Field(Enum("gold", "silver"), default="silver")
```

## Before and after

```python
from datetime import date
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deal = s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"), owner=ann, contact=pat)
    return ann, pat, deal

ann, pat, deal = db.run(load)

def opportunity_attributes():
    tr = db.new_transaction()
    described = semantic(tr).describe("erp.Opportunity")
    tr.abort()
    return {a["name"]: (a.get("label", {}).get("und"), a["provenance"]) for a in described["attributes"]}

before = opportunity_attributes()
db.run(lambda tr: tr.install_module("erp_forecast"))
after = opportunity_attributes()

tr = db.new_transaction()
print(tr.installed_modules())
tr.abort()
for name in ("title", "close_date"):
    print(f"{name:11} before {str(before.get(name)):34} after {after[name]}")
```

Output:

```text
('base', 'erp_model', 'erp_forecast')
title       before ('Title', ['erp_model'])           after ('Subject', ['erp_model', 'erp_forecast'])
close_date  before None                               after (None, ['erp_forecast'])
```

The installation wrote a new catalog into the database, composed from both modules. `title` keeps the type
and the requirement `erp_model` declared, takes the label `erp_forecast` gives it, and lists both modules in
its provenance; `close_date` is new. A later module may change an element's presentation and meaning, and add
elements; it may not change what an element *is* (its kind, its value type, a relation's target or
cardinality): a module that redeclares `title` as an `int` is refused at installation with
`ProtoValidationError` ("a structural field is refused without a declared migration").

The entities created before the installation are not rewritten. Reading the new field on one of them gives its
default:

```python
def plan(tr):
    s = semantic(tr)
    print("before setting it:", s.get(deal).close_date)
    s.update(deal, close_date=date(2026, 12, 15))
    print("after:", s.get(deal).close_date)

db.run(plan)
```

Output:

```text
before setting it: None
after: 2026-12-15
```

## A subtype from another module

`erp.KeyAccount` declares `erp.Person` as its supertype. It inherits every element of the person, including
the relation to opportunities, and an opportunity's `contact`, declared as a person, accepts one:

```python
def key_account(tr):
    s = semantic(tr)
    acme = s.create("erp.KeyAccount", name="Lee Wong", company="Acme", tier="gold")
    s.create("erp.Opportunity", number=2, title="Support contract", amount=Decimal("40000"), owner=ann, contact=acme)
    return acme

acme = db.run(key_account)
tr = db.new_transaction()
s = semantic(tr)
described = s.describe("erp.KeyAccount")
print("supertypes:", described["supertypes"])
for attribute in described["attributes"]:
    print(f"  {attribute['name']:14} inherited from {str(attribute.get('inherited_from')):11} provenance {attribute['provenance']}")
print("the key account's deals:", [(o.number, o.contact.tier) for o in s.get(acme).opportunities])
tr.abort()
```

Output:

```text
supertypes: ['erp.Person']
  name           inherited from erp.Person  provenance ['erp_model']
  company        inherited from erp.Person  provenance ['erp_model']
  opportunities  inherited from erp.Person  provenance ['erp_model']
  quotes         inherited from erp.Person  provenance ['erp_model']
  tier           inherited from None        provenance ['erp_forecast']
the key account's deals: [(2, 'gold')]
```

A subtype's elements are the composition of its supertypes' and its own, in the C3 order of its supertypes
(the same linearization Python uses for classes); an element a supertype defined says so in `inherited_from`.
A subtype may add elements and change presentation, never the structure of an inherited element, and it may
not redefine the supertype's `__key__`.

## Behaviour from a module

An `Extension` adds declarations. A plain registry module adds **behaviour** to the same type name: methods
and data defaults, resolved through the registry, as chapter 8 of the complete tutorial shows with
`erp_labels` and `erp_crm`. The versions a semantic session returns resolve them too:

```python
db.run(lambda tr: tr.install_module("erp_labels"))
tr = db.new_transaction()
s = semantic(tr)
print([s.get(deal).label(), s.get(deal).currency])
tr.abort()
space.close()
```

Output:

```text
['#1 ERP licences (lead)', 'ARS']
```

## The rules of composition

* A module names the modules it builds on (`depends=`), and installing it installs those first when they are
  not installed yet. The database stores the module names in installation order; the code lives in the
  Python modules.
* The composed catalog depends on the set of modules, not on the order they were installed in
  (`tests/unit/test_semantic_compose.py` checks it).
* Every process that opens the database must be able to import the modules it lists, and their code must
  match the stored catalog: a process whose code declares an element differently is refused with
  `ProtoValidationError`, naming the module and the element (chapter 6 shows it).
* The parents of a registry name, their C3 linearization, `_super()` and the errors the registry raises are
  the subject of [extensible models](../../source/tutorials/extensible-models.rst), sections 5 to 8; the
  design and its trade-offs are in [the registry chapter of the design guide](../../source/design-guide/registry.rst).

Next: [6. Schema evolution](06-schema-evolution.md)
