# 6. Schema evolution

A database outlives its first model. New fields, new aggregates and new rules arrive as new modules, installed
into a database that already holds entities. Nothing is rewritten at installation: the catalog changes in one
commit, and the existing entities meet the new declarations as they are read and written. This chapter shows
what that means for each kind of addition, and how to migrate the data when the model needs it.

The change is `examples/tutorial/erp_regions.py`:

```text
regions = module("erp_regions", depends=("erp_model",))

class OpportunityRegion(Extension):
    __extends__ = "erp.Opportunity"
    region = Field(str, required=True, label="Region")

class UserWon(Extension):
    __extends__ = "erp.User"
    won = Aggregate(over="opportunities", where="stage == 'won'", sum="amount", maintained=True)
```

## A database with history

```python
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoValidationError
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deals = [s.create("erp.Opportunity", number=n, title=f"deal {n}", amount=Decimal(amount), stage=stage,
                      owner=ann, contact=pat)
             for n, amount, stage in ((1, "12000", "proposal"), (2, "3000", "won"), (3, "5000", "won"))]
    return ann, pat, deals

ann, pat, deals = db.run(load)
db.run(lambda tr: tr.install_module("erp_regions"))

tr = db.new_transaction()
s = semantic(tr)
print(tr.installed_modules())
print("ann won:", s.get(ann).won, "| stored:", s.get(ann).record.has_defined_attr("won"))
print("regions:", [s.get(deal).region for deal in deals])
tr.abort()
```

Output:

```text
('base', 'erp_model', 'erp_regions')
ann won: 8000 | stored: False
regions: [None, None, None]
```

## A new maintained aggregate

The existing user has no stored `won`. Until its first write stores it, reading it computes it from the
relation, so the value is right from the first read; the first change that moves it stores it, and from then
on it is maintained like any other:

```python
db.run(lambda tr: semantic(tr).update(deals[2], amount=Decimal("5500")))
tr = db.new_transaction()
s = semantic(tr)
print("ann won:", s.get(ann).won, "| stored:", s.get(ann).record.has_defined_attr("won"))
tr.abort()
```

Output:

```text
ann won: 8500 | stored: True
```

## A new required field

`region` is required, but the three opportunities were created before it existed. A required field is checked
when an entity is **created**, so new opportunities must have it, while the old ones remain valid and can still
be changed without it:

```python
def create_without(tr):
    semantic(tr).create("erp.Opportunity", number=4, title="deal 4", amount=Decimal(1), owner=ann, contact=pat)

try:
    db.run(create_without)
except ProtoValidationError as error:
    print("a new one without a region:", str(error).split(": ", 1)[1])
db.run(lambda tr: semantic(tr).update(deals[0], title="deal 1, revised"))
print("an old one changed without a region: committed")
```

Output:

```text
a new one without a region: 'erp.Opportunity' has the required fields ['region'] with no value; required fields are given at creation
an old one changed without a region: committed
```

The [format spec](../../source/format-spec.rst) states the rule ("Constraints"). Checking a new requirement against every existing entity would
make installing a module a full scan and a write of the whole database, and would refuse the installation of
any requirement the old data cannot meet yet. The same holds for an invariant that a later module adds. The
consequence is the one to plan for: until the data is migrated, a reader must expect `None` where the
catalog says "required" for entities older than the module.

## Migrating the data

A migration is ordinary code: a transaction that reads the entities and writes the new field. Run it through
`Database.run`: a concurrent change of the same entities makes the commit conflict, as any commit would, and
`run` redoes the backfill on the new version instead of failing:

```python
def backfill(tr):
    s = semantic(tr)
    changed = 0
    for opportunity in s.get(ann).opportunities:
        if opportunity.region is None:
            # .entity: the handle of the version's cell
            s.update(opportunity.entity, region="south" if opportunity.number % 2 else "north")
            changed += 1
    return changed

print("backfilled:", db.run(backfill))
tr = db.new_transaction()
s = semantic(tr)
print([(s.get(deal).number, s.get(deal).region) for deal in deals])
tr.abort()
```

Output:

```text
backfilled: 3
[(1, 'south'), (2, 'north'), (3, 'south')]
```

A large migration is better split into batches, each its own `Database.run`: one transaction that writes every
entity conflicts with every concurrent writer of any of them (the [concurrency chapter](../05-concurrency.md)
of the complete tutorial).

## What a module cannot change

* **Structure.** A later module may add elements, change labels, meanings and defaults, and add rules. It may
  not change an element's kind, value type, a relation's target or cardinality, or a role's binding: that is
  refused at installation ("a structural field is refused without a declared migration", chapter 5). Such a
  change is a new element (a new field, a new relation) and a migration that moves the data.
* **The code in other processes.** The database stores module names, not code. A process whose code declares
  a listed module differently from the stored catalog is refused with `ProtoValidationError`, naming the module
  and the element (the refusal is in [extensible models](../../source/tutorials/extensible-models.rst),
  section 8, "A refused definition"). A process that started before an installation keeps the registry it
  built until `ObjectSpace.reload_registry(database)` is called; chapter 8 of the complete tutorial shows it. Deploy the
  module's code everywhere before installing it.
* **The past.** Old versions keep the catalog they were committed with (chapter 7).

```python
space.close()
```

Next: [7. History](07-history.md)
