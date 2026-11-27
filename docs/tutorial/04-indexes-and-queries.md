# 4. Indexes and queries

A query is built with the LINQ-style front end, `from_collection(...).where(...)`, and turned into a plan by
one planner that every front end shares. The planner picks the cheapest way to read among the indexes the
collection carries, keeps every condition it did not use as a filter, and never drops one. `explain()` shows
the plan that runs. This chapter is the introduction; the [queries tutorial](queries/README.md) covers every
operator, reading `explain()` in full, pagination, custom indexes, queries under concurrency and the
performance pitfalls.

## A query without an index

The ERP of chapter 3, built without indexes:

```python
from proto_db import F, MemoryStorage, ObjectSpace, from_collection

import erp_app

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db, indexes=False)

def plan_of(query):
    """The node names of a query's plan, from the root down."""
    names, node = [], query.explain()
    while node:
        names.append(node["plan"])
        node = node["children"][0] if node.get("children") else None
    return " <- ".join(names)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
proposals = from_collection(deals).where(F.stage == "proposal")
print(plan_of(proposals), "->", proposals.count(), "of", len(deals))
tr.abort()
```

Output:

```text
Filter <- CollectionScan -> 333 of 1000
```

With no index, the only access path is a scan of every element, with the condition as a `Filter` over it.
`F.stage` names an attribute; comparisons, `&`, `|` and `~` combine conditions.

## Adding indexes

An index belongs to a collection. `add_index` builds it over the elements the collection holds and returns a
new version of the collection; from then on every change updates the index in the same step.

```python
from proto_db import EqualityIndex, RangeIndex

with db.new_transaction() as tr:
    deals = (tr.get_root_object("opportunities")
             .add_index(EqualityIndex("by_stage", "stage"))
             .add_index(RangeIndex("by_amount", "amount"))
             .add_index(EqualityIndex("by_owner", "owner")))
    tr.set_root_object("opportunities", deals)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(deals.index_names())
for label, query in [
    ("stage == proposal", from_collection(deals).where(F.stage == "proposal")),
    ("amount >= 9000", from_collection(deals).where(F.amount >= 9000)),
    ("owned by Ann", from_collection(deals).where(F.owner == erp_app.user("ann"))),
    ("proposal and amount >= 9000", from_collection(deals).where((F.stage == "proposal") & (F.amount >= 9000))),
]:
    print(f"{label:28} {plan_of(query):24} {query.count()} rows")
tr.abort()
```

Output:

```text
['by_amount', 'by_owner', 'by_stage']
stage == proposal            IndexLookup              333 rows
amount >= 9000               IndexRange               108 rows
owned by Ann                 IndexLookup              334 rows
proposal and amount >= 9000  Filter <- IndexRange     36 rows
```

* An `EqualityIndex` answers `==` and `in`; a `RangeIndex` also answers `<`, `<=`, `>`, `>=` and `between`,
  and yields its elements in key order.
* `F.owner == erp_app.user("ann")` compares a `MutableObject` handle: it indexes by the entity's identity, so
  "Ann's opportunities" is a lookup however often Ann's record changes.
* With two conditions, the planner used the index it estimated cheaper (the 108 amounts of 9,000 or more) and kept
  the stage as a residual `Filter`. It never drops a condition.

An index is used because the planner estimated it cheaper, not because it exists: on a small collection a
scan can win. Whatever the plan, the answer is the same set of elements a full scan gives; the test suite
checks that invariant on generated data (`tests/property/test_index_equals_scan.py`).

## Shaping the result

`order_by`, `then_by`, `skip`, `take` and `select` shape the result; `group_by` aggregates.

```python
from proto_db import Agg

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
top = (from_collection(deals).where(F.stage == "proposal")
       .order_by(F.amount, descending=True).then_by(F.number).take(3)
       .select({"number": F.number, "amount": F.amount}))
print(top.to_list())
per_owner = (from_collection(deals)
             .group_by(F.owner, {"owner": Agg.key(), "n": Agg.count(), "largest": Agg.max(F.amount)})
             .to_list())
print(sorted((row["owner"].get(tr).name, row["n"], row["largest"]) for row in per_owner))
print(erp_app.pipeline(deals))
tr.abort()
```

Output:

```text
[{'number': 242, 'amount': 9954}, {'number': 485, 'amount': 9945}, {'number': 728, 'amount': 9936}]
[('Ann Lee', 334, 9991), ('Bob Ruiz', 333, 9917), ('Cy Park', 333, 9954)]
[('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]
```

The same front end works over plain Python iterables (`from_collection([...])`), which makes it easy to try a
query before any data is stored.

## Reading `explain()`

`explain()` returns the plan as a dictionary: the node, the index it reads, the planner's estimate and the
children.

```python
tr = db.new_transaction()
plan = from_collection(tr.get_root_object("opportunities")).where(F.amount.between(9000, 9100)).explain()
print(plan["plan"], plan["index"]["index"], plan["index"]["kind"], "| estimated rows:", plan["estimate"]["rows"])
tr.abort()
```

Output:

```text
IndexRange by_amount RangeIndex | estimated rows: 11.0
```

The [queries tutorial](queries/03-indexes-and-the-planner.md) reads every field of it, and
[its chapter 6](queries/06-queries-under-concurrency.md) shows how the plan decides what a query declares to a
concurrent commit.

## A stable report

A long report read page by page while other people commit is a **pager**: it is bound to one snapshot, so no
page repeats or skips a row, whatever is committed between pages. Its conditions are `[path, operator, value]`
lists, a form a resumable token can describe ([queries chapter 4](queries/04-pagination.md)).

```python
with db.new_transaction() as tr:
    pager = tr.pager("opportunities", where=[["stage", "==", "proposal"]], order_by=[("amount", True), "number"],
                     page_size=100)
    print(pager.rows, "rows in", pager.pages, "pages; the first starts with", pager.page(0)[0].number)
space.close()
```

Output:

```text
333 rows in 4 pages; the first starts with 242
```

Chapter 6 shows the pager while another user keeps writing.

Depth: [the queries tutorial](queries/README.md), [indexing and
querying](../source/tutorials/indexing-querying.rst), [the query planner](../source/query-planner.rst).

Next: [5. Concurrency](05-concurrency.md)
