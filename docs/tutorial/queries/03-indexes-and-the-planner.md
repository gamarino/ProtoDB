# 3. Indexes and the planner

Every front end hands its conditions to one planner. The planner lists the access paths the collection's
indexes offer for each condition, estimates the cost of each, takes the cheapest, and keeps every condition it
did not use as a residual `Filter`; it never drops one. This chapter shows how it chooses, how to read
`explain()` field by field, and the indexes it can choose from.

```python
from proto_db import EqualityIndex, F, MemoryStorage, ObjectSpace, RangeIndex, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)                                      # by_stage, by_amount and by_owner on the opportunities
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(deals.index_names())
for name in deals.index_names():
    print(deals.get_index(name).explain())
```

Output:

```text
['by_amount', 'by_owner', 'by_stage']
{'index': 'by_amount', 'kind': 'RangeIndex', 'key': 'amount', 'entries': 1000, 'keys': 1000, 'unorderable': 0}
{'index': 'by_owner', 'kind': 'EqualityIndex', 'key': 'owner', 'entries': 1000, 'keys': 3, 'unorderable': 0}
{'index': 'by_stage', 'kind': 'EqualityIndex', 'key': 'stage', 'entries': 1000, 'keys': 3, 'unorderable': 0}
```

An index's own `explain()` gives its kind, its key, how many elements it holds (`entries`) and how many
distinct keys (`keys`); `unorderable` counts keys that have no order (they are kept, and found by a scan).
`index.validate(collection.elements())` checks it against the collection and raises `ProtoCorruptionError` on
a mismatch.

## Chosen by cost, not by existence

The same condition and the same index, on a collection of three opportunities and on one of a thousand:

```python
with db.new_transaction() as small_tr:
    small = small_tr.new_dictionary().add_index(EqualityIndex("by_stage", "stage"))
    for n in range(3):
        small = small.set_at(n, small_tr.new_object(number=n, stage=erp_app.STAGES[n % 3]))
    small_tr.set_root_object("small", small)

small = tr.database.new_transaction().get_root_object("small")
for label, source in (("3 opportunities", small), ("1000 opportunities", deals)):
    query = from_collection(source).where(F.stage != "lead")
    plan = query.explain()
    print(f"{label:19} {plan_of(query):26} estimated rows {plan['estimate']['rows']:6.1f}, cost {plan['estimate']['cost']:7.1f}")
```

Output:

```text
3 opportunities     Filter <- CollectionScan   estimated rows    1.5, cost     6.0
1000 opportunities  IndexComplement            estimated rows  666.0, cost  1004.0
```

On three elements, reading them all is cheaper than going through the index; on a thousand, the index wins
(an `IndexComplement` reads every group of the index except the one excluded). The choice is what the cost
model estimates; it changes with the data, and `explain()` is how you find out. It matters beyond speed:
what a query declares to a concurrent commit follows the plan that runs (chapter 6).

## Reading `explain()`

`explain()` returns the plan that will run, as nested dictionaries: `plan` (the node), its parameters, an
`estimate` (`rows` and `cost`), and `children`. The helper prints one line per node:

```python
def show(query):
    def walk(node, depth):
        what = {k: v for k, v in node.items() if k not in ("plan", "children", "estimate")}
        if "index" in what:
            what["index"] = what["index"]["index"]
        print("  " * depth + f"{node['plan']}  rows~{node['estimate']['rows']:.0f}  {what}")
        for child in node.get("children", []):
            walk(child, depth + 1)
    walk(query.explain(), 0)

show(from_collection(deals).where((F.stage == "proposal") & (F.amount >= 9000)).order_by(F.amount).take(3))
```

Output:

```text
Limit  rows~3  {'limit': 3, 'offset': 0}
  Sort  rows~54  {'keys': [{'key': 'amount', 'descending': False}]}
    Filter  rows~54  {'predicate': {'path': 'stage', 'op': '==', 'value': 'proposal'}}
      IndexRange  rows~108  {'index': 'by_amount', 'term': {'path': 'amount', 'op': '>=', 'value': 9000}}
```

Read it from the bottom up. The `IndexRange` on `by_amount` reads the 108 opportunities of 9,000 or more (its
`term`); the `Filter` keeps the proposals among them (its `predicate`, the residual condition); the `Sort`
orders by amount; the `Limit` keeps three. The stage index was not used, because the planner estimated 333
proposals against 108 large amounts. Each estimate is the planner's, not a count: `rows~54` on the `Filter` is
its guess, and the query returns what it returns.

## The alternatives it compared

Asking for each condition alone shows the estimates the planner weighed:

```python
for label, condition in (("stage == proposal", F.stage == "proposal"), ("amount >= 9000", F.amount >= 9000),
                         ("stage in (lead, won)", F.stage.in_(["lead", "won"])), ("stage != lead", F.stage != "lead"),
                         ("owner is Ann", F.owner == erp_app.user("ann"))):
    query = from_collection(deals).where(condition)
    print(f"{label:22} {plan_of(query):26} estimated {query.explain()['estimate']['rows']:6.1f}, actual {query.count()}")
```

Output:

```text
stage == proposal      IndexLookup                estimated  333.0, actual 333
amount >= 9000         IndexRange                 estimated  108.0, actual 108
stage in (lead, won)   IndexLookup                estimated  334.0, actual 334
stage != lead          IndexComplement            estimated  666.0, actual 666
owner is Ann           IndexLookup                estimated  334.0, actual 334
```

An `EqualityIndex` answers `==`, `in`, `!=` (as an `IndexComplement`) and the presence tests; a `RangeIndex`
also answers `<`, `<=`, `>`, `>=` and the `between` family. Here every estimate equals the count, because each
answer is a whole index group or range whose size the index knows; with residual filters, the estimate is a
guess.

## Ordered reads

A `RangeIndex` yields its elements in key order, so an `order_by` on its key needs no sort, and a `take` stops
early:

```python
for label, query in (("ordered by amount, first 5", from_collection(deals).order_by(F.amount).take(5)),
                     ("ordered by title, first 5", from_collection(deals).order_by(F.title).take(5))):
    print(f"{label:28} {plan_of(query):34} {[d.number for d in query.to_list()]}")
```

Output:

```text
ordered by amount, first 5   Limit <- IndexOrderedScan          [0, 973, 730, 487, 244]
ordered by title, first 5    Limit <- Sort <- CollectionScan    [0, 1, 10, 100, 101]
```

The first plan walks the amount index in order (`IndexOrderedScan`) and stops after five elements; the second
sorts all thousand by title (in string order: `"opportunity 0"`, `"opportunity 1"`, `"opportunity 10"`)
before it keeps five.

## Counting

`count()` runs the plan under a `Count` node; `explain_count()` shows that plan without running it:

```python
query = from_collection(deals).where(F.stage == "lead")
print(query.explain_count()["plan"], "<-", query.explain_count()["children"][0]["plan"], "|", query.count())
```

Output:

```text
Count <- IndexLookup | 334
```

## Keys that are computed

An index key can be a path into a nested value (`"address.zip"`) or a **named function** of the element,
registered once per process with `register_extractor`. Only the name (and a `version`) is stored in the
database; every process that opens it must register the same function again, and the function must be pure:
the same element always gives the same key, and it must not read the database (an extractor that reads
through a transaction is refused with `ProtoUserError`).

```python
from proto_db import KeySpec, register_extractor

register_extractor("amount_band", lambda deal: deal.amount // 1000, version="1")

with db.new_transaction() as wtr:
    with_band = wtr.get_root_object("opportunities").add_index(
        EqualityIndex("by_band", KeySpec.extractor("amount_band")))
    wtr.set_root_object("opportunities", with_band)

tr.abort()
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
band_9 = from_collection(deals).where(F["@amount_band"] == 9)
print(plan_of(band_9), band_9.count(), "opportunities between 9,000 and 9,999")
```

Output:

```text
IndexLookup 108 opportunities between 9,000 and 9,999
```

`F["@name"]` refers to a registered extractor in a query. If the function's meaning changes, register it under
a new name or a new `version`: indexes and pager tokens remember both.

## Unique indexes

`EqualityIndex(name, key, unique=True)` holds at most one element per key. A duplicate in the transaction's own
view is refused at once:

```python
from proto_db import ProtoValidationError

with db.new_transaction() as wtr:
    wtr.set_root_object("logins", wtr.new_dictionary().add_index(EqualityIndex("by_login", "login", unique=True))
                        .set_at(1, wtr.new_object(login="ann")))
wtr = db.new_transaction()
try:
    wtr.get_root_object("logins").set_at(2, wtr.new_object(login="ann"))
except ProtoValidationError:
    print("a second 'ann' is refused: ProtoValidationError")
wtr.abort()
tr.abort()
space.close()
```

Output:

```text
a second 'ann' is refused: ProtoValidationError
```

Two transactions that add the same key concurrently conflict on that key's group, even if neither read it,
and the retry through `Database.run` sees the duplicate and fails with `ProtoValidationError`: the race is in
"Check and add", in [chapter 5 of the complete tutorial](../05-concurrency.md).

## The invariant

Whatever the plan, the answer is the same multiset of elements a full scan gives: the planner chooses *how* to
read, never *what* the answer is. There is one comparator for collections, indexes and queries (one total
order across types, `1 == 1.0 == Decimal(1)`, `True != 1`), so an index and a scan cannot disagree about which
of two values is smaller. The test suite checks the invariant on generated data
(`tests/property/test_index_equals_scan.py`); the rules are on [the query planner](../../source/query-planner.rst)
and [the index framework](../../source/indexing-framework.rst) pages.

Next: [4. Pagination](04-pagination.md)
