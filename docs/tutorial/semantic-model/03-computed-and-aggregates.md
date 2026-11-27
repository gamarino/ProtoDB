# 3. Computed attributes and aggregates

Some attributes are not stored by the application: they follow from other data. A **`Computed`** attribute is
an expression over the entity's own fields, evaluated when it is read. An **`Aggregate`** sums or counts over a
relation; a **maintained** aggregate is stored on the entity and moved by every change of the relation, in the
same transaction. Both are declared in the catalog as expressions, so a program in another language reads the
same definitions.

```python
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoConflictError
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deals = [s.create("erp.Opportunity", number=n, title=f"deal {n}", amount=Decimal(amount), stage=stage,
                      probability=Decimal(p), owner=ann, contact=pat)
             for n, amount, stage, p in ((1, "12000", "proposal", "0.5"), (2, "3000", "lead", "0.1"),
                                         (3, "8000", "lost", "0"))]
    return ann, pat, deals

ann, pat, (first, second, third) = db.run(load)

def show(label):
    tr = db.new_transaction()
    s = semantic(tr)
    user = s.get(ann)
    print(f"{label:28} pipeline {user.pipeline:>6} | expected {sorted(o.expected for o in user.opportunities)}")
    tr.abort()

show("after the load")
```

Output:

```text
after the load               pipeline  15000 | expected [Decimal('0'), Decimal('300.0'), Decimal('6000.0')]
```

## Computed attributes

`expected = Computed("amount * probability")` is evaluated on each read, from the version being read. The
expression language is a small neutral subset, the same in every language: attribute paths, constants, `+`,
`-` and `*` (no division in this revision), the six comparisons, `and`, `or` and `not`. An attribute the
version does not store reads as its declared default. The catalog stores the parsed tree, not the text:

```python
tr = db.new_transaction()
expected = next(a for a in semantic(tr).describe("erp.Opportunity")["attributes"] if a["name"] == "expected")
print(expected["kind"], expected["expression"]["ast"])
tr.abort()
```

Output:

```text
computed ['*', ['attr', 'amount'], ['attr', 'probability']]
```

A `Computed` may also be a Python callable. Then it is catalogued as Python-only, with no expression: Python
programs evaluate it and others see that they cannot. Prefer an expression whenever one can say it.

## Maintained aggregates

`pipeline = Aggregate(over="opportunities", where="stage != 'lost'", sum="amount", maintained=True)` is the
total of the user's opportunities that are not lost. Every operation that changes what it sums moves it by the
difference, in the same transaction:

```python
db.run(lambda tr: semantic(tr).update(first, amount=Decimal("15000")))
show("first deal raised to 15000")
db.run(lambda tr: semantic(tr).update(second, stage="lost"))
show("second deal lost")
db.run(lambda tr: semantic(tr).update(third, stage="qualified"))
show("third deal revived")

def unassign(tr):
    s = semantic(tr)
    s.unlink(third, "owner", ann)
    bob = s.create("erp.User", login="bob", name="Bob Ruiz")
    s.link(third, "owner", bob)
    return bob

bob = db.run(unassign)
show("third deal handed to bob")
tr = db.new_transaction()
print("bob's pipeline:", semantic(tr).get(bob).pipeline, "| stored on ann's record:",
      semantic(tr).get(ann).record.has_defined_attr("pipeline"))
tr.abort()
```

Output:

```text
first deal raised to 15000   pipeline  18000 | expected [Decimal('0'), Decimal('300.0'), Decimal('7500.0')]
second deal lost             pipeline  15000 | expected [Decimal('0'), Decimal('300.0'), Decimal('7500.0')]
third deal revived           pipeline  23000 | expected [Decimal('0'), Decimal('300.0'), Decimal('7500.0')]
third deal handed to bob     pipeline  15000 | expected [Decimal('300.0'), Decimal('7500.0')]
bob's pipeline: 8000 | stored on ann's record: True
```

The value is stored on the user's record, so reading it costs nothing however many opportunities the user
has; a non-maintained aggregate (`maintained=False`, the default) is computed by walking the relation on each
read instead. The terms of a maintained aggregate must be neutral all the way down: summing a Python-only
computed attribute is refused when the model is composed.

## The price: the aggregate's owner is written

A maintained aggregate turns a change of an opportunity into a write of its owner as well. Two transactions
that each change a different opportunity of the same user both write that user's cell, and under the
[precondition model](../05-concurrency.md) the second to commit conflicts on it:

```python
writers = [db.new_transaction() for _ in range(2)]
semantic(writers[0]).update(first, amount=Decimal("16000"))
semantic(writers[1]).update(second, amount=Decimal("3500"))
writers[0].commit()
try:
    writers[1].commit()
except ProtoConflictError as error:
    print("second commit:", error.unit.kind.name, "on", error.unit.key.split("#")[0])

attempts = []
def raise_second(tr):
    attempts.append(1)
    if len(attempts) == 1:                            # the first deal changes again meanwhile
        db.run(lambda other: semantic(other).update(first, amount=Decimal("16500")))
    semantic(tr).update(second, amount=Decimal("3500"))
db.run(raise_second)
print("through Database.run:", len(attempts), "attempts")
show("both changes in")
space.close()
```

Output:

```text
second commit: CELL on erp.User
through Database.run: 2 attempts
both changes in              pipeline  16500 | expected [Decimal('350.0'), Decimal('8250.0')]
```

`second` is lost, so its amount does not count in the pipeline, but the update still read and wrote Ann's
record. `Database.run` retries a conflict by running the function again on the new version, which is the normal
way to write semantic entities; [the concurrency chapter](../05-concurrency.md) of the complete tutorial
explains the retry and its limits. When a hot entity collects many concurrent changes (a busy user, a company
with every order), a maintained aggregate serialises them; leave it unmaintained, and pay the walk on reads,
if that contention costs more than the reads do.

Next: [4. Validation and intents](04-validation-and-intents.md)
