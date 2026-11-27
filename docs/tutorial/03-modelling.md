# 3. Modelling

This chapter builds the ERP's model twice. First with plain records and collections, deciding for each thing
whether it is a **value** or an **entity**. Then with the **semantic model**, where the database itself
describes its entities, their relations in both directions, what is computed and what is checked. The rest of
the complete tutorial uses the plain model; the [semantic model tutorial](semantic-model/README.md) goes deep
into the second.

## Values and entities

For each thing in the domain, decide what other records should see when it changes:

* An **opportunity** is a **value**. Each change (a new stage, a new amount) makes a new version that points
  at the one it replaced (`previous`) and says who made it (`changed_by`). Old versions stay as they were, so
  the chain is the opportunity's history.
* A **user** is an **entity**. When a name is corrected, every opportunity the user owns should show the
  corrected name. A `MutableObject` is a named cell whose value changes by commit; records hold a small
  handle to it, and the handle always reads the current value.
* A **person** (a contact) is a plain record in a `Dictionary`, and a **group** is a `Set` of logins in a
  `Dictionary` keyed by group name.

```python
from proto_db import MemoryStorage, MutableObject, ObjectSpace

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")

with db.new_transaction() as tr:
    ann = tr.new_mutable("user:ann", tr.new_object(login="ann", name="Ann Lee"))
    bob = tr.new_mutable("user:bob", tr.new_object(login="bob", name="Bob Ruiz"))
    tr.set_root_object("groups", tr.new_dictionary().set_at("sales", tr.new_set().add("ann").add("bob")))
    tr.set_root_object("persons", tr.new_dictionary().set_at(7, tr.new_object(name="Pat Doe", company="Globex")))
    deal = tr.new_object(number=1, title="ERP licences", stage="lead", amount=12_000,
                         owner=ann,                      # an entity: the handle of the cell
                         contact=7,                      # a person, by key
                         changed_by="import", previous=None)
    tr.set_root_object("opportunities", tr.new_dictionary().set_at(1, deal))

def move(number, stage, who, **changes):
    """The next version of an opportunity: a new record that remembers the one it replaces."""
    def work(tr):
        deals = tr.get_root_object("opportunities")
        current = deals.get_at(number)
        revised = current.with_(previous=current, stage=stage, changed_by=who, **changes)
        tr.set_root_object("opportunities", deals.set_at(number, revised))
    db.run(work)

move(1, "qualified", "ann")
move(1, "proposal", "bob", amount=11_500)
with db.new_transaction() as tr:                         # a correction of the user, committed once
    tr.set_mutable("user:ann", tr.get_mutable("user:ann").with_(name="Ann Lee-Smith"))

tr = db.new_transaction()
deal = tr.get_root_object("opportunities").get_at(1)
print("owner now:", deal.owner.get(tr).name)
history = []
while deal is not None:
    history.append((deal.stage, deal.amount, deal.changed_by))
    deal = deal.previous
print("history:", list(reversed(history)))
tr.abort()
```

Output:

```text
owner now: Ann Lee-Smith
history: [('lead', 12000, 'import'), ('qualified', 12000, 'ann'), ('proposal', 11500, 'bob')]
```

The opportunity reads the owner's current name through the handle, and its own past through `previous`. The
collection holds only the current version of each opportunity, so a query for "proposals" never meets a
superseded version, and needs no "is current" condition. The relational equivalent, a table of versions with
a validity column, is shown and measured in [modelling data](../source/tutorials/modelling-data.rst).

The choice is per relationship, not per database. A value is right for a fact at a moment (a quote, an
invoice line); an entity is right when others must see the current state.

## The model the other chapters use

`examples/tutorial/erp_app.py` packages this model at a size where indexes matter: three users, three groups,
200 persons and 1,000 opportunities, with the data of the [ERP topic tutorial](../source/tutorials/erp.rst).
The other chapters start from its `seed`:

```python
import erp_app

shop = space.new_database("shop")
erp_app.seed(shop)
tr = shop.new_transaction()
print(erp_app.pipeline(tr.get_root_object("opportunities")))
print(sorted(tr.get_root_object("groups").get_at("sales")), tr.get_mutable("user:bob").name)
tr.abort()
```

Output:

```text
[('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]
['ann', 'bob'] Bob Ruiz
```

## The semantic model: a database that describes itself

In the plain model, what an opportunity *is* lives in your code: the database stores records with an
attribute `owner`, and nothing in it says that `owner` is a user, that a user has opportunities, or that the
stage is one of five words. The **semantic model** declares that in a registry module, and installing the
module writes the description, the *catalog*, into the database. The module `examples/tutorial/erp_model.py`
declares the same ERP. An excerpt:

```text
class Opportunity(DBObject):
    __type_name__ = "erp.Opportunity"
    __meaning__ = "A possible sale to a customer, owned by a user, moving through stages"
    number = Field(int, required=True, label="Number")
    title = Field(str, required=True, label="Title")
    stage = Field(Enum("lead", "qualified", "proposal", "won", "lost"), default="lead", label="Stage")
    amount = Field(Decimal, required=True, unit="currency:company", meaning="The amount of the sale if it is won")
    probability = Field(Decimal, default=Decimal("0.1"), meaning="The chance of winning, from 0 to 1")
    expected = Computed("amount * probability", unit="currency:company", ...)
    owner = Relation("erp.User", cardinality="many-to-one", inverse="opportunities")
    contact = Relation("erp.Person", cardinality="many-to-one", inverse="opportunities")

class User(DBObject):
    __type_name__ = "erp.User"
    login = Field(str, required=True, label="Login")
    name = Field(str, required=True, label="Name", sensitivity="personal")
    memberships = RelationTo("erp.Membership", role="user")
    opportunities = Relation("erp.Opportunity", cardinality="one-to-many", inverse="owner")
    pipeline = Aggregate(over="opportunities", where="stage != 'lost'", sum="amount", maintained=True)

class Membership(Relationship):                   # a relation that carries data: a user in a group, in a role
    __type_name__ = "erp.Membership"
    user = Role("erp.User", inverse="memberships")
    group = Role("erp.Group", inverse="memberships")
    role = Field(Enum("member", "manager"), default="member")
```

Four kinds of declaration carry the model:

* a **`Field`** is stored data, with its type, whether it is required, a default and its meaning;
* a **`Relation`** links two entities and names its **inverse**, so it is navigable from both sides: an
  opportunity has an `owner`, and the user has `opportunities`;
* a **`Relationship`** is a relation that carries data. Each `Role` is one of its ends, and `RelationTo` is the
  end seen from the other entity: a `Membership` relates a user and a group, *in a role*;
* a **`Computed`** attribute is an expression over the entity's own data (`amount * probability`), and an
  **`Aggregate`** sums or counts over a relation; a *maintained* aggregate is stored and updated by every
  change of the relation, in the same transaction.

The semantic session, `semantic(tr)`, creates and reads entities through the catalog:

```python
from decimal import Decimal

from proto_db.semantic import semantic

model = space.new_database("model")
model.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    sales = s.create("erp.Group", name="sales")
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    s.relate("erp.Membership", user=ann, group=sales, role="manager")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    for number, amount, stage in ((1, "12000", "proposal"), (2, "3000", "lead"), (3, "8000", "lost")):
        s.create("erp.Opportunity", number=number, title=f"deal {number}", amount=Decimal(amount),
                 stage=stage, probability=Decimal("0.5"), owner=ann, contact=pat)
    return ann

ann = model.run(load)                                 # the handle of Ann's cell

tr = model.new_transaction()
s = semantic(tr)
user = s.get(ann)
print(user.name, "| groups:", [(m.group.name, m.role) for m in user.memberships])
print("pipeline:", user.pipeline, "| expected:", sorted(o.expected for o in user.opportunities))
print("owner of each:", sorted((o.number, o.owner.login) for o in user.opportunities))
print(s.describe("erp.Opportunity")["meaning"])
tr.abort()
```

Output:

```text
Ann Lee | groups: [('sales', 'manager')]
pipeline: 15000 | expected: [Decimal('1500.0'), Decimal('4000.0'), Decimal('6000.0')]
owner of each: [(1, 'ann'), (2, 'ann'), (3, 'ann')]
A possible sale to a customer, owned by a user, moving through stages
```

`pipeline` is 12,000 + 3,000: the lost opportunity is excluded by the aggregate's `where`. The catalog is
data in the database: another process, a tool or an agent can ask what an opportunity is, what `expected`
means and which module defined it, without the Python code.

```python
space.close()
```

## Which to use

The plain model is the whole database: records, collections, cells and indexes, queried with the LINQ front
end (chapter 4). The semantic model is built on it, and adds a self-description, relations maintained on both
sides, computed attributes, aggregates and checked rules. Use the semantic model when the meaning of the data
matters to more than one program; use plain records when the code is the only reader. Both commit through the
same transactions and the same concurrency model.

Depth: [modelling data](../source/tutorials/modelling-data.rst), [the ERP example](../source/tutorials/erp.rst),
and the [semantic model tutorial](semantic-model/README.md).

Next: [4. Indexes and queries](04-indexes-and-queries.md)
