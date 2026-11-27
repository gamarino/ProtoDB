# 1. Declaring a model

A semantic model is a **registry module**: a Python module that calls `proto_db.registry.module(name)` and
declares classes whose attributes are *descriptors*. Installing the module into a database writes its
description, the **catalog**, into that database. From then on the database can say what each entity is, which
fields it has, how it relates to others, what is computed and what is checked, to any program that opens it,
including one that has never seen the Python code.

This tutorial uses one model throughout: `examples/tutorial/erp_model.py`, the ERP of the
[complete tutorial](../README.md) (users, groups, persons, opportunities), declared semantically.

## The module

The first lines of `erp_model.py`, and one of its classes:

```text
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Aggregate, Computed, Enum, Field, Invariant, Relation, RelationTo, Relationship, Role, intent

erp = module("erp_model")


class Person(DBObject):
    __type_name__ = "erp.Person"
    __meaning__ = "A contact at a customer company"
    __label__ = "Person"
    name = Field(str, required=True, label="Name", sensitivity="personal")
    company = Field(str, label="Company")
    opportunities = Relation("erp.Opportunity", cardinality="one-to-many", inverse="contact")
    quotes = RelationTo("erp.Quote", role="recipient")
```

* `module("erp_model")` declares the registry module. Its name is the Python import path (the tutorials put
  `examples/tutorial` on `sys.path`), because a database stores only the name and another process must be able
  to import the code from it. One name, one Python module, per process.
* `__type_name__` is the **registry name** of the type, the name the database stores. It is required: a class
  with semantic elements and no type name is refused when it is defined.
* `__meaning__`, `__label__`, `__synonyms__` and `__key__` describe the type: what it is, what to call it,
  other words for it (an opportunity is also a "deal"), and the fields that identify one.
* The class attributes are descriptors. They leave the Python class when it is defined and become the type's
  declaration in the registry; the class keeps its plain methods.

## The descriptors

| Descriptor | Declares | Chapter |
|---|---|---|
| `Field(type, required=, default=, unit=, label=, meaning=, sensitivity=)` | stored data | this one |
| `Enum("a", "b", ...)` as a field's type | a string field with listed values | this one |
| `Relation(target, cardinality=, inverse=)` | a link between two entities, navigable from both sides | 2 |
| `Relationship` with `Role`s, seen from the other side as `RelationTo` | a relation that carries data | 2 |
| `Computed(expression)` | a value computed from the entity's own data | 3 |
| `Aggregate(over=, sum= or count=, where=, maintained=)` | a sum or a count over a relation | 3 |
| `Invariant(expression)` in `__constraints__` | a rule every committed version satisfies | 4 |
| `@intent(meaning=, requires=)` on a method | a named operation with a precondition | 4 |
| `Extension` with `__extends__` | additions to a type declared by another module | 5 |

## Installing

Installing is a write, and commits like any other:

```python
from proto_db import MemoryStorage, ObjectSpace
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

tr = db.new_transaction()
print(tr.installed_modules())
s = semantic(tr)
for entity in s.list_entities():
    print(f"{entity['type_name']:16} {entity['meaning']}")
```

Output:

```text
('base', 'erp_model')
erp.Group        A group of users that share permissions
erp.Membership   A user belongs to a group, in a role
erp.Opportunity  A possible sale to a customer, owned by a user, moving through stages
erp.Person       A contact at a customer company
erp.Quote        An offer sent to a contact for an opportunity, with the terms of that moment
erp.User         A person who signs in to the ERP and owns opportunities
```

`semantic(tr)` opens the **semantic session** of a transaction (or of a snapshot): the operations of this
tutorial go through it. `list_entities(text)` finds types by name, label, synonym or meaning:

```python
print([entity["type_name"] for entity in s.list_entities("deal")], [entity["type_name"] for entity in s.list_entities("sale")])
```

Output:

```text
['erp.Opportunity'] ['erp.Opportunity']
```

## What the catalog says

`describe(type_name)` returns the composed declaration of a type as plain data: its fields with their types,
defaults, units and meanings, its relations with their inverses and cardinalities, its computed attributes as
expression trees, its intents and constraints, and for every element the modules that defined it (its
*provenance*):

```python
described = s.describe("erp.Opportunity")
print(described["meaning"], "| also called:", described["synonyms"])
for attribute in described["attributes"]:
    kind = attribute.get("kind") or f"relation to {attribute['target']} ({attribute['cardinality']})"
    details = {k: attribute[k] for k in ("value_type", "required", "default", "values", "unit") if k in attribute}
    print(f"  {attribute['name']:12} {kind:36} {details}")
print("intents:", [i["name"] for i in described["intents"]], "| constraints:", [c["name"] for c in described["constraints"]])
```

Output:

```text
A possible sale to a customer, owned by a user, moving through stages | also called: ['deal']
  number       field                                {'value_type': 'int', 'required': True}
  title        field                                {'value_type': 'str', 'required': True}
  stage        field                                {'value_type': 'str', 'default': 'lead', 'values': ['lead', 'qualified', 'proposal', 'won', 'lost']}
  amount       field                                {'value_type': 'decimal', 'required': True, 'unit': 'currency:company'}
  probability  field                                {'value_type': 'decimal', 'default': Decimal('0.1')}
  expected     computed                             {'unit': 'currency:company'}
  owner        relation to erp.User (one)           {}
  contact      relation to erp.Person (one)         {}
  quotes       relation to erp.Quote (many)         {}
intents: ['move_to'] | constraints: ['probability_range']
```

The catalog is data in the database, under the root `__semanticCatalog`, and its format is specified ([format
spec](../../source/format-spec.rst), "Semantic catalog (format 6)"). A tool, an agent or a runtime in another
language can read it without the Python module; the [semantic-model kit](../../kit/semantic/README.md) shows
an agent doing so over MCP.

## What a declaration does not enforce

Two parts of a field declaration are description only, in this release:

```python
from decimal import Decimal

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    return s.create("erp.Opportunity", number="seven", title="ERP licences", stage="maybe",
                    amount=Decimal("12000"), owner=ann, contact=pat)

odd = db.run(load)
tr.abort()
tr = db.new_transaction()
print(semantic(tr).get(odd).stage, semantic(tr).get(odd).number)
tr.abort()
space.close()
```

Output:

```text
maybe seven
```

An `Enum` lists the values, and a field's type is catalogued, but neither is checked when a value is stored.
What is checked: required fields, unknown names, the targets' types of relations, cardinalities, invariants
and intents' preconditions (chapter 4). A rule that matters is written as an `Invariant`.

Next: [2. Entities and relations](02-entities-and-relations.md)
