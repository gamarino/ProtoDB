# 2. Entities and relations

An entity of the semantic model is a **cell**: a `MutableObject` whose value is the entity's current record.
The session creates it, reads its current **version**, changes its fields, and links it to other entities. A
relation is declared once, with its inverse, and the session keeps both ends consistent in the same
transaction.

## Creating and reading

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
    bob = s.create("erp.User", login="bob", name="Bob Ruiz")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    licences = s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"),
                        owner=ann, contact=pat)
    return ann, bob, pat, licences

ann, bob, pat, licences = db.run(load)
print(type(licences).__name__, licences.name.split("#")[0])

tr = db.new_transaction()
s = semantic(tr)
deal = s.get(licences)
print(type(deal).__name__, deal.number, deal.title, deal.stage, deal.probability)
print("owner:", deal.owner.login, "| contact:", deal.contact.name, deal.contact.company)
tr.abort()
```

Output:

```text
MutableObject erp.Opportunity
Version 1 ERP licences lead 0.1
owner: ann | contact: Pat Doe Globex
```

`create` returns the entity's handle, a cell named after its type and a random identity (`erp.Opportunity#`
followed by a UUID, so the tutorial never prints it whole). `get` returns a **`Version`**: the record the
cell holds now, read through the catalog. Its fields read as attributes, a field it does not store reads as
its declared default (`stage`, `probability`), and a relation reads as the version of the entity at the other
end.

Fields change with `update`; relations do not:

```python
def change(tr):
    s = semantic(tr)
    s.update(licences, title="ERP licences, 40 seats", probability=Decimal("0.4"))
    try:
        s.update(licences, owner=bob)
    except ProtoValidationError as error:
        print("update(owner=...):", str(error).split(": ", 1)[1])

db.run(change)
```

Output:

```text
update(owner=...): 'owner' is a role of 'erp.Opportunity'; change it with link and unlink
```

## Both ends of a relation

`owner` on the opportunity and `opportunities` on the user are the two ends of one relation, declared as
`Relation("erp.User", cardinality="many-to-one", inverse="opportunities")` and its mirror. `link` and
`unlink` change it, and both ends move together:

```python
def reassign(tr):
    s = semantic(tr)
    s.unlink(licences, "owner", ann)
    s.link(licences, "owner", bob)

db.run(reassign)

tr = db.new_transaction()
s = semantic(tr)
print("owner:", s.get(licences).owner.login)
print("ann's:", [o.number for o in s.get(ann).opportunities], "| bob's:", [o.number for o in s.get(bob).opportunities])
print("navigate:", s.navigate(licences, "owner").login, type(s.navigate(bob, "opportunities")).__name__)
tr.abort()
```

Output:

```text
owner: bob
ann's: [] | bob's: [1]
navigate: bob Members
```

`navigate(entity, role)` is the same read by name: a version for a `one` end, `None` when nothing is linked,
and the members of a `many` end. The checks run at once:

```python
import re

def wrong(tr, action):
    try:
        action(semantic(tr))
    except ProtoValidationError as error:                     # hide the random part of cell names
        return re.sub(r"#[0-9a-f]{32}", "#...", str(error).split(": ", 1)[-1])

for label, action in (("a second owner", lambda s: s.link(licences, "owner", ann)),
                      ("a person as owner", lambda s: s.create("erp.Opportunity", number=2, title="x",
                                                               amount=Decimal(1), owner=pat, contact=pat))):
    tr = db.new_transaction()
    print(f"{label:18}", wrong(tr, action))
    tr.abort()
```

Output:

```text
a second owner     the end 'owner' of 'erp.Opportunity' is linked to 'erp.User#...'; unlink it first
a person as owner  the target 'erp.Person#...' is a 'erp.Person', not a 'erp.User' (or one of its subtypes)
```

## Relations that carry data

A user belongs to a group *in a role*. That relation has data of its own, so it is a **`Relationship`**: a
type whose `Role`s are its ends. Each end entity sees it through `RelationTo`:

```text
class Membership(Relationship):
    __type_name__ = "erp.Membership"
    user = Role("erp.User", inverse="memberships")
    group = Role("erp.Group", inverse="memberships")
    role = Field(Enum("member", "manager"), default="member")

class Group(DBObject):          # and User likewise
    memberships = RelationTo("erp.Membership", role="group")
```

`relate` creates one, linked at every end:

```python
def groups(tr):
    s = semantic(tr)
    sales = s.create("erp.Group", name="sales")
    s.relate("erp.Membership", user=ann, group=sales, role="manager")
    s.relate("erp.Membership", user=bob, group=sales)
    return sales

sales = db.run(groups)
tr = db.new_transaction()
s = semantic(tr)
print("sales:", [(m.user.login, m.role) for m in s.get(sales).memberships])
print("ann:", [(m.group.name, m.role) for m in s.get(ann).memberships])
tr.abort()
```

Output:

```text
sales: [('ann', 'manager'), ('bob', 'member')]
ann: [('sales', 'manager')]
```

A membership is one stored record, referenced from both ends; it is not an entity with a cell of its own.

## Binding to a version

A quote is sent to a contact with the terms of that moment. If the contact's company changes later, an old
quote must still show the company it was sent to. The `recipient` role of `erp.Quote` is declared with
`binding="version"`: it stores the entity **and** the version it had when the quote was made. A role bound by
identity (the default) always reads the current version.

```python
db.run(lambda tr: semantic(tr).relate("erp.Quote", opportunity=licences, recipient=pat, amount=Decimal("12000")))
db.run(lambda tr: semantic(tr).update(pat, company="Initech"))       # the contact moves
db.run(lambda tr: semantic(tr).relate("erp.Quote", opportunity=licences, recipient=pat, amount=Decimal("11500")))

tr = db.new_transaction()
s = semantic(tr)
for quote in s.get(licences).quotes:                                  # ordered: the role says ordered=True
    print(quote.amount, "sent to", quote.recipient.name, "at", quote.recipient.company)
print("the contact now:", s.get(licences).contact.company)
tr.abort()
```

Output:

```text
12000 sent to Pat Doe at Globex
11500 sent to Pat Doe at Initech
the contact now: Initech
```

## Querying the model

`query` takes a query tree in the JSON form an agent or another language uses too: where to start (`from`, a
path of relations from a cell), a condition, a projection or an aggregate. Every answer carries the version it
read and the plan it ran:

```python
tr = db.new_transaction()
s = semantic(tr)
answer = s.query({"from": ["via", ["cell", sales.name], "memberships"],
                  "where": ["==", ["attr", "role"], ["const", "member"]],
                  "select": ["user.login", "role"]})
print(answer["rows"], "| version", answer["version"]["number"], "| plan", answer["explain"]["plan"]["plan"])
quotes = s.query({"from": ["via", ["cell", licences.name], "quotes"], "select": ["amount", "recipient.company"]})
print(quotes["rows"])
total = s.query({"from": ["via", ["cell", sales.name], "memberships", "user", "opportunities"],
                 "aggregate": ["sum", ["attr", "amount"]]})
print(total["rows"], [term["name"] for term in total["terms"]])
tr.abort()
space.close()
```

Output:

```text
[{'user.login': 'bob', 'role': 'member'}] | version 8 | plan CollectionScan
[{'amount': Decimal('12000'), 'recipient.company': 'Globex'}, {'amount': Decimal('11500'), 'recipient.company': 'Initech'}]
[{'sum': Decimal('12000')}] ['memberships', 'user', 'opportunities', 'amount']
```

Values come back as Python values (the agent interface serialises them to JSON), and `terms` lists the
catalog elements the query used, so an answer can be checked against the catalog. Starting from a cell is the
point: a query walks relations from an entity, and the collection at a `many` end is where it filters (its
plan above is a scan of the group's two memberships). The tree's grammar is section 11 of the
[semantic model design](../../superpowers/specs/2026-11-15-semantic-model-mvp-design.md); the [semantic-model kit](../../kit/semantic/README.md) names every query an agent asks of its
model in `docs/kit/semantic/queries.py`. Inside Python code, the LINQ front end of the
[queries tutorial](../queries/README.md) works on the same records.

Next: [3. Computed attributes and aggregates](03-computed-and-aggregates.md)
