# 6. Time: as-of reads, audits and stable pages

A commit never overwrites. Every version of the database stays readable, and each version of a record can
point at the one it replaced. This chapter turns that into three answers an ERP normally needs extra
machinery for: *what did the pipeline look like last Tuesday*, *who changed this opportunity and what was it
before*, and *give me the report page by page while people keep working*. None of them needs a history
table, a trigger or a server-side cursor.

## The week's work

```python
import datetime as dt
import time

from proto_db import F, MemoryStorage, ObjectSpace, from_collection

import erp_app

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)

def work_on(number, stage, who, **changes):
    erp_app.advance(db, number, stage, who, **changes)
    time.sleep(0.01)                                  # commits get distinct timestamps on a fast machine

before_week = dt.datetime.now(dt.timezone.utc)
time.sleep(0.02)
work_on(3, "qualified", "ann")
work_on(3, "proposal", "ann", amount=7500)
work_on(3, "won", "bob")
work_on(4, "lost", "cy")
mid_week = dt.datetime.now(dt.timezone.utc)
time.sleep(0.02)
work_on(6, "qualified", "ann")
print(len(db.history()), "versions:", [entry.version for entry in db.history()])
```

Output:

```text
7 versions: [6, 5, 4, 3, 2, 1, 0]
```

## As of a moment

`Database.history()` lists the versions, newest first, each with its `committed_at` (UTC). The version in
force at a moment is the newest one committed at or before it, and `snapshot_at` opens it, read-only, with
the same reading API as a transaction:

```python
def as_of(moment):
    """The database as it was at ``moment``."""
    for entry in db.history():                        # newest first
        if entry.committed_at <= moment:
            return db.snapshot_at(entry)
    raise LookupError("the database did not exist yet")

now = db.new_transaction()
print("now:     ", erp_app.pipeline(now.get_root_object("opportunities")))
print("mid-week:", erp_app.pipeline(as_of(mid_week).get_root_object("opportunities")))
print("before:  ", erp_app.pipeline(as_of(before_week).get_root_object("opportunities")))
now.abort()
```

Output:

```text
now:      [('lead', 332, 1806488), ('lost', 1, 1148), ('proposal', 333, 1795500), ('qualified', 333, 1783253), ('won', 1, 7500)]
mid-week: [('lead', 333, 1807710), ('lost', 1, 1148), ('proposal', 333, 1795500), ('qualified', 332, 1782031), ('won', 1, 7500)]
before:   [('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]
```

The same function ran on three versions of the data. Nothing was rewritten for the past: a snapshot of an old
version *is* the database as it was, indexes included, so an as-of query uses the indexes it had then.

## An audit without an audit table

Each version of an opportunity points at the one it replaced, and `erp_app.advance` stores who made the
change. The chain is the audit trail:

```python
def audit(deal):
    """Every version of an opportunity, oldest first: (stage, amount, changed_by)."""
    trail = []
    while deal is not None:
        trail.append((deal.stage, deal.amount, deal.changed_by))
        deal = deal.previous
    return list(reversed(trail))

tr = db.new_transaction()
for row in audit(tr.get_root_object("opportunities").get_at(3)):
    print(row)
tr.abort()
```

Output:

```text
('lead', 1111, 'import')
('qualified', 1111, 'ann')
('proposal', 7500, 'ann')
('won', 7500, 'bob')
```

Users are entities, so a correction of a user is a new value of the user's cell, and a snapshot reads the
cell as it was:

```python
with db.new_transaction() as tr:
    tr.set_mutable("user:ann", tr.get_mutable("user:ann").with_(name="Ann Lee-Smith"))

tr = db.new_transaction()
deal = tr.get_root_object("opportunities").get_at(3)
print("owner now:", deal.owner.get(tr).name, "| before the week:", as_of(before_week).get_mutable("user:ann").name)
tr.abort()
```

Output:

```text
owner now: Ann Lee-Smith | before the week: Ann Lee
```

What the trail does not hold, it cannot tell: *who* and *when* exist only if a version stores them (here
`changed_by`; the commit time is in `history()`). Nothing is ever deleted either, so data that must be erased
for legal reasons needs another mechanism (encryption with an external key, for instance).

## Stable pages while others write

A report of the open opportunities, the most valuable first, 250 rows per page, while Cy keeps closing deals.
The usual way, a new query per page with an offset, moves rows across page boundaries when the data changes
between pages:

```python
def top_open(source):
    return (from_collection(source).where(F.stage != "won")
            .order_by(F.amount, descending=True).then_by(F.number))

def offset_page(k, size=250):
    with db.new_transaction() as tr:                  # a fresh snapshot per page, as a web request would
        return [d.number for d in top_open(tr.get_root_object("opportunities")).skip(k * size).take(size).to_list()]

with db.new_transaction() as tr:
    truth = [d.number for d in top_open(tr.get_root_object("opportunities")).to_list()]
seen = offset_page(0)
for number in seen[:40]:                              # Cy wins the 40 best deals meanwhile
    erp_app.advance(db, number, "won", "cy")
seen += offset_page(1)
print(len(seen), "rows in two offset pages;", len(set(truth[:500]) - set(seen)), "of the first 500 never shown")
```

Output:

```text
500 rows in two offset pages; 40 of the first 500 never shown
```

A **pager** is bound to one snapshot when it is made, so every page comes from the same version. Its token is
a portable reference to that snapshot and query, which another request, or another process, can resume:

```python
with db.new_transaction() as tr:
    pager = tr.pager("opportunities", where=[["stage", "!=", "won"]], order_by=[("amount", True), "number"],
                     page_size=250)
    report = [d.number for d in pager.page(0)]
    token = pager.token()
    rows_at_start = pager.rows

for number in report[:40]:                            # the 40 best deals of this report are won meanwhile
    erp_app.advance(db, number, "won", "cy")

resumed = db.resume_pager(token)                      # a later request
for k in range(1, resumed.pages):
    report += [d.number for d in resumed.page(k)]
print(len(report), "rows,", len(set(report)), "distinct,", rows_at_start, "at the start")
space.close()
```

Output:

```text
959 rows, 959 distinct, 959 at the start
```

The report holds exactly the rows that were open when it started, none repeated and none skipped. There is
no cursor held open on a server and no long transaction: a snapshot is stable because versions never change.

Depth: [the ERP example](../source/tutorials/erp.rst) (the same three questions, with the storage cost of
history measured), [pagination in the queries tutorial](queries/04-pagination.md),
[history links in the design guide](../source/design-guide/history-links.rst).

Next: [7. Durability and operations](07-durability-and-operations.md)
