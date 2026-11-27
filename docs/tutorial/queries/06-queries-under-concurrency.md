# 6. Queries under concurrency

Inside a transaction a query is a read, and under the [precondition model](../05-concurrency.md) a read is a
declaration: what the query saw must still be what it was when the transaction commits. How much a query
declares depends on **the plan that runs**, not on the indexes that exist. This chapter measures it, unit by
unit, and shows the reads that declare more than they seem to.

## The probe

`after(read, change)` runs `read` in one transaction, commits `change` in another, then commits the first.
The reading transaction also writes an unrelated root: a transaction that writes nothing has nothing to
publish, never validates, and never conflicts.

```python
from proto_db import F, MemoryStorage, ObjectSpace, ProtoConflictError, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)

def after(read, change):
    reader = db.new_transaction()
    read(reader.get_root_object("opportunities"))
    reader.set_root_object("report", "done")
    with db.new_transaction() as writer:
        deals = writer.get_root_object("opportunities")
        writer.set_root_object("opportunities", change(writer, deals))
    try:
        reader.commit()
        return "committed"
    except ProtoConflictError as error:
        unit = error.unit
        index = f" of index {unit.index!r}" if unit.index else ""
        key = f" {unit.key!r}" if unit.key is not None and unit.kind.name != "RANGE" else ""
        return f"conflict on {unit.kind.name}{key}{index}"

def stage_of(number, stage):
    return lambda tr, deals: deals.set_at(number, deals.get_at(number).with_(stage=stage))

def amount_of(number, amount):
    return lambda tr, deals: deals.set_at(number, deals.get_at(number).with_(amount=amount))

def add_deal(number):
    return lambda tr, deals: deals.set_at(number, erp_app.opportunity_record(tr, number, {
        login: erp_app.user(login) for login in erp_app.LOGINS}))

with db.new_transaction() as tr:
    deals = tr.get_root_object("opportunities")
    print(deals.get_at(3).stage, deals.get_at(4).stage, deals.get_at(5).stage, "| amounts of 242 and 7:",
          deals.get_at(242).amount, deals.get_at(7).amount)
```

Output:

```text
lead qualified proposal | amounts of 242 and 7: 9954 1259
```

## Equality lookups declare a group

A query answered by an equality index declares the **group** of its key: the set of elements with that key.
It conflicts only with a change that adds an element to the group or removes one from it:

```python
leads = lambda deals: from_collection(deals).where(F.stage == "lead").count()
print(plan_of(from_collection(db.new_transaction().get_root_object("opportunities")).where(F.stage == "lead")))
print("a qualified deal is lost:   ", after(leads, stage_of(4, "lost")))
print("a lead's amount changes:    ", after(leads, amount_of(3, 2000)))
print("a lead is qualified:        ", after(leads, stage_of(3, "qualified")))
print("a deal becomes a lead:      ", after(leads, stage_of(5, "lead")))
```

Output:

```text
IndexLookup
a qualified deal is lost:    committed
a lead's amount changes:     conflict on INDEX_GROUP 'lead' of index 'by_stage'
a lead is qualified:         conflict on INDEX_GROUP 'lead' of index 'by_stage'
a deal becomes a lead:       conflict on INDEX_GROUP 'lead' of index 'by_stage'
```

The second line conflicts although no lead entered or left the group: the group holds the elements
themselves, and a new version of a lead is a change of a member. A lookup declares its group's content, not
only its size.

## Range scans declare a range

A query answered by a range index declares the range of keys it scanned:

```python
large = lambda deals: from_collection(deals).where(F.amount >= 9900).count()
print("a small deal changes:         ", after(large, stage_of(7, "won")))
print("a small deal grows into range:", after(large, amount_of(7, 9950)))
print("a large deal is won:          ", after(large, stage_of(242, "won")))
print("a new small deal:             ", after(large, add_deal(5000)))
```

Output:

```text
a small deal changes:          committed
a small deal grows into range: conflict on RANGE of index 'by_amount'
a large deal is won:           conflict on RANGE of index 'by_amount'
a new small deal:              committed
```

## Scans declare the whole collection

A query no index answers, a `PythonFilter`, `len`, iteration, `values()`, `items()` and an ordered scan declare
the whole collection: any change of it is a conflict.

```python
by_title = lambda deals: from_collection(deals).where(F.title == "opportunity 9").count()
by_lambda = lambda deals: from_collection(deals).where(lambda d: d.amount % 1000 == 0).count()
print(plan_of(from_collection(db.new_transaction().get_root_object("opportunities")).where(F.title == "x")))
print("title scan, another deal changes: ", after(by_title, stage_of(7, "won")))
print("a lambda, another deal changes:   ", after(by_lambda, stage_of(8, "won")))
print("len(), another deal changes:      ", after(len, stage_of(9, "won")))
```

Output:

```text
Filter <- CollectionScan
title scan, another deal changes:  conflict on WHOLE
a lambda, another deal changes:    conflict on WHOLE
len(), another deal changes:       conflict on WHOLE
```

## The plan decides, so `explain()` tells

The same query declares a group or the whole collection depending on the plan. On a collection of one
opportunity the planner prefers a scan even for `==`, and the scan declares everything:

```python
from proto_db import EqualityIndex

with db.new_transaction() as tr:
    tr.set_root_object("tiny", tr.new_dictionary().add_index(EqualityIndex("by_stage", "stage"))
                       .set_at(1, tr.new_object(stage="lead")))

def on_tiny(read, change):
    reader = db.new_transaction()
    read(reader.get_root_object("tiny"))
    reader.set_root_object("report", "done")
    with db.new_transaction() as writer:
        writer.set_root_object("tiny", change(writer, writer.get_root_object("tiny")))
    try:
        reader.commit()
        return "committed"
    except ProtoConflictError as error:
        return f"conflict on {error.unit.kind.name}"

lead = lambda source: from_collection(source).where(F.stage == "lead")
print("1 element:    ", plan_of(lead(db.new_transaction().get_root_object("tiny"))),
      "|", on_tiny(lambda t: lead(t).count(), lambda tr, t: t.set_at(2, tr.new_object(stage="lost"))))
print("1000 elements:", plan_of(lead(db.new_transaction().get_root_object("opportunities"))),
      "|", after(lambda d: lead(d).count(), stage_of(4, "lost")))
print("!= on 1000:   ", plan_of(from_collection(db.new_transaction().get_root_object("opportunities")).where(F.stage != "lead")),
      "|", after(lambda d: from_collection(d).where(F.stage != "lead").count(), amount_of(3, 2500)))
```

Output:

```text
1 element:     Filter <- CollectionScan | conflict on WHOLE
1000 elements: IndexLookup | committed
!= on 1000:    IndexComplement | conflict on WHOLE
```

On one element the scan declared the whole collection, so adding an opportunity that is not a lead
conflicted; on a thousand the lookup declared only the group of `"lead"`, and losing a qualified deal did not
touch it. The third line is
the rule for reads the model does not list: `!=`, answered by an `IndexComplement`, declares the whole
collection (a read with no fine unit proven safe takes the coarser one), so even a change of a lead, which the
query excluded, conflicts.

## Windows and key walks on a Dictionary

A `Dictionary` is ordered by key. Walking it with `keys(after=k)` declares the range of keys from `k` to the
last key returned; a window by **offset** depends on every key before it, so it declares the range from the
**first** key:

```python
walk = lambda deals: list(deals.keys(after=990))
window = lambda deals: from_collection(deals).skip(10).take(5).to_list()
counted = lambda deals: from_collection(deals).skip(10).take(5).count()
print("keys(after=990), key 10 changes:     ", after(walk, stage_of(10, "won")))
print("keys(after=990), key 995 changes:    ", after(walk, stage_of(995, "won")))
print("skip(10).take(5), key 3 changes:     ", after(window, stage_of(3, "won")))
print("skip(10).take(5), key 500 changes:   ", after(window, stage_of(500, "won")))
print("the same window counted, key 500:    ", after(counted, stage_of(501, "won")))
```

Output:

```text
keys(after=990), key 10 changes:      committed
keys(after=990), key 995 changes:     conflict on RANGE
skip(10).take(5), key 3 changes:      conflict on RANGE
skip(10).take(5), key 500 changes:    committed
the same window counted, key 500:     conflict on WHOLE
```

The offset window (keys 10 to 14) conflicted with a change of key 3, which it never returned: removing key 3
would have shifted the window. A change past the window's last key does not conflict. The same window
*counted* declared the whole collection: `count()` puts a `Count` node over the window, and a read the model
does not list takes the whole collection, so list a window rather than count it inside a transaction that
writes. `keys(after=k)` also covers `k` itself, which is stricter than needed and safe.

## Reads that declare more than they seem to

```python
def explain_only(deals):
    from_collection(deals).where(F.title == "opportunity 9").explain()   # a scan's explain shows its row count

def many_keys(deals):
    for number in range(1100):                                          # more than 1024 fine reads
        deals.get_at(number)

print("explain() of a scan:      ", after(explain_only, stage_of(12, "won")))
print("1100 get_at, key 1099 too:", after(many_keys, add_deal(5001)))
print("from a Snapshot:          ", end=" ")
reader = db.new_transaction()
snapshot = db.snapshot_at(reader.snapshot_ref)
from_collection(snapshot.get_root_object("opportunities")).where(F.title == "opportunity 9").count()
reader.set_root_object("report", "from a snapshot")
with db.new_transaction() as writer:
    deals = writer.get_root_object("opportunities")
    writer.set_root_object("opportunities", deals.set_at(13, deals.get_at(13).with_(stage="won")))
reader.commit()
print("committed")
space.close()
```

Output:

```text
explain() of a scan:       conflict on WHOLE
1100 get_at, key 1099 too: conflict on WHOLE
from a Snapshot:           committed
```

* `explain()` of a plan that scans reports the collection's row count, so it records the whole collection.
* **Escalation**: a transaction records at most 1,024 fine preconditions and ranges per collection root
  (`PRECONDITION_ESCALATION_LIMIT`); the next one replaces them all with the whole collection. After 1,100
  `get_at`, a new key that none of them read conflicts.
* A read from a **`Snapshot`** records nothing: the deliberate way out, for a summary that may be slightly
  stale.

## What to take from it

| The query | It declares |
|---|---|
| answered by an equality index (`==`, `in`) | the group of each key it looked up |
| `!=` (an `IndexComplement`) | the whole collection |
| answered by a range index (`<`, `between`, ...) | the scanned key range |
| a scan, a `PythonFilter`, `len`, iteration, `explain()` of a scan | the whole collection |
| `keys(after=k)` on a Dictionary | the keys from `k` to the last one returned |
| an offset window over a Dictionary, listed | the keys from the first one to the window's last |
| the same window, counted | the whole collection |
| through a `Snapshot` | nothing |

Indexes narrow conflicts as well as reads. A transaction that must read a lot and then write little conflicts
less if it reads through indexes, reads only what its decision needs, or reads the rest from a snapshot when a
slightly stale view is acceptable. The rules are in [the concurrency model](../../source/concurrency-model.rst),
section "Reads".

Next: [7. Performance pitfalls](07-performance-pitfalls.md)
