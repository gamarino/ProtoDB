# 4. Validation and intents

A model says what may be stored. The semantic session checks it at two moments: an **operation** (`create`,
`update`, `link`, `relate`, `invoke`) is refused at once when its result would break a declaration, and a
**commit** is refused when the transaction's final state leaves an entity incomplete. An **intent** names an
operation of the domain and states when it applies.

```python
import re
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoConstraintError, ProtoValidationError
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

def refused(action):
    """Run ``action`` on a session of a new transaction; the refusal, with the random part of cell names hidden."""
    tr = db.new_transaction()
    try:
        action(semantic(tr))
        tr.commit()
        return "accepted"
    except (ProtoValidationError, ProtoConstraintError) as error:
        if tr.is_active:                                      # a refused operation; a refused commit ended it
            tr.abort()
        return f"{type(error).__name__}: " + re.sub(r"#[0-9a-f]{32}", "#...", str(error))
```

## Refused by the operation

```python
cases = {
    "a required field missing": lambda s: s.create("erp.Opportunity", number=2, amount=Decimal(1), owner=ann, contact=pat),
    "a name the type does not declare": lambda s: s.update(deal, colour="red"),
    "a computed attribute written": lambda s: s.update(deal, expected=Decimal(5)),
    "a probability of 2": lambda s: s.update(deal, probability=Decimal(2)),
}
for label, action in cases.items():
    print(f"{label:34}", refused(action))
```

Output:

```text
a required field missing           ProtoValidationError: create('erp.Opportunity'): 'erp.Opportunity' has the required fields ['title'] with no value; required fields are given at creation
a name the type does not declare   ProtoValidationError: update('erp.Opportunity#...'): 'erp.Opportunity' has no attribute or role 'colour'
a computed attribute written       ProtoValidationError: update('erp.Opportunity#...'): 'expected' of 'erp.Opportunity' is computed; it is never written
a probability of 2                 ProtoConstraintError: The probability 2 is not between 0 and 1
```

The last one is the model's **invariant**, declared on the opportunity:

```text
__constraints__ = [
    Invariant("probability >= 0 and probability <= 1", name="probability_range",
              meaning="A probability is between 0 and 1",
              message="The probability {probability} is not between 0 and 1"),
]
```

An invariant is an expression of the same language as a computed attribute, checked on the version an
operation produces. Its `message` may name attributes, and its `meaning` is catalogued, so a program that is
refused can explain the rule to its user. A refused operation changes nothing: the transaction stays usable.
`enforced=False` catalogues a rule without checking it (a rule another system enforces, for instance).

## Refused by the commit

A relation end of cardinality `one` must be linked when the transaction commits, not at every step: an entity
may be created first and linked afterwards, in the same transaction.

```python
def linked_later(s):
    second = s.create("erp.Opportunity", number=2, title="CRM", amount=Decimal("3000"))
    s.link(second, "owner", ann)
    s.link(second, "contact", pat)

print("linked in the same transaction:", refused(linked_later))
print("an owner removed:              ", refused(lambda s: s.unlink(deal, "owner", ann)))
```

Output:

```text
linked in the same transaction: accepted
an owner removed:               ProtoConstraintError: The opportunity erp.Opportunity#... has no owner (cardinality one)
```

A refused commit ends the transaction, as any failed commit does: nothing of it is published. `Database.run`
does not retry a `ProtoConstraintError`, because running the same function again would break the same rule.

## Intents

`move_to` is an **intent**: a method of the model with a meaning and a precondition, catalogued like the
fields.

```text
@intent(meaning="Move the opportunity to another stage; a won or lost opportunity is closed",
        requires="stage != 'won' and stage != 'lost'", parameters={"stage": "str"})
def move_to(self, stage):
    return self.with_(stage=stage)
```

It is invoked through the session, by name, or called on a version the session gave; an intent that returns a
new version of its receiver updates the entity:

```python
def advance(tr):
    s = semantic(tr)
    s.get(deal).move_to("proposal")                           # a call on a version
    return s.invoke(deal, "move_to", stage="won").stage       # the same, by name

print("now:", db.run(advance))
print("once won:", refused(lambda s: s.invoke(deal, "move_to", stage="lead")))
print("no such intent:", refused(lambda s: s.invoke(deal, "close")))
```

Output:

```text
now: won
once won: ProtoConstraintError: The intent 'move_to' does not apply to 'erp.Opportunity#...': erp.Opportunity.move_to.requires is false on its current version (Move the opportunity to another stage; a won or lost opportunity is closed)
no such intent: ProtoValidationError: The intent 'close' of 'erp.Opportunity#...': 'erp.Opportunity' has no intent named 'close'
```

`requires` is checked on the entity's current version before the method runs. Inside the method,
`proto_db.semantic.intent_context()` gives the session's operations (create, relate, link) for intents that do
more than return a new version; the [semantic-model kit](../../kit/semantic/README.md) has an order whose
`add_line` intent relates a product to it.

An intent is the model's statement of how an entity should change, not a lock on its fields: `update` remains
available to code that holds the session.

```python
print("a direct update:", refused(lambda s: s.update(deal, stage="lead")))
space.close()
```

Output:

```text
a direct update: accepted
```

Route the application's changes through intents, and keep `update` for corrections and migrations: the
catalog then tells an agent or another program which operations exist and when each applies.

## What is not checked

Chapter 1 showed that a field's type and an `Enum`'s values are catalogued, not enforced, in this release. A
`required` field or an invariant that a *later* module adds is not checked against the entities that existed
before it (chapter 6). Everything else in this chapter is checked on every operation and commit, under
concurrency too: an invariant over a maintained aggregate (a credit limit over the total of a customer's
orders) is checked on the state the transaction would commit, and two concurrent orders that each fit but
not together end with one refused; the kit's step 4 runs it on seven threads.

Next: [5. Modules and composition](05-modules-and-composition.md)
