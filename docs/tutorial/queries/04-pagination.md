# 4. Pagination

A report of a thousand opportunities is read a page at a time. `proto_db` gives three ways to do it, and they
answer different questions: an **offset window** (`skip` and `take`), a **pager** bound to one snapshot, and
**key-ordered access** on a `Dictionary` (`keys(after=k)`, `key_at_position`, `position_of`).

```python
from proto_db import F, MemoryStorage, ObjectSpace, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)
```

## Offset windows

`skip(n).take(size)` is a window over the ordered result. Over a range index it is answered from the index in
key order, so a deep page does not sort the whole collection:

```python
def page(k, size=5):
    with db.new_transaction() as tr:
        query = (from_collection(tr.get_root_object("opportunities"))
                 .order_by(F.amount, descending=True).skip(k * size).take(size))
        return plan_of(query), [(d.number, d.amount) for d in query.to_list()]

for k in (0, 1, 150):
    print(k, *page(k))
```

Output:

```text
0 Limit <- IndexOrderedScan [(243, 9991), (486, 9982), (729, 9973), (972, 9964), (242, 9954)]
1 Limit <- IndexOrderedScan [(485, 9945), (728, 9936), (971, 9927), (241, 9917), (484, 9908)]
150 Limit <- IndexOrderedScan [(542, 3054), (785, 3045), (55, 3035), (298, 3026), (541, 3017)]
```

An offset window is fine for a result that does not change while it is read. Each page above ran in its own
transaction, as a web request would, so a commit between two pages moves rows across the boundary: a row can
be skipped or shown twice. The [Time chapter](../06-time.md) of the complete tutorial measures it.

## A pager: every page from one snapshot

`Transaction.pager(root, where=..., order_by=..., page_size=...)` binds the query to the transaction's
snapshot. Its conditions use the list form (`[path, operator, value]`), which a token can describe; `order_by`
takes paths, or `(path, True)` for descending:

```python
with db.new_transaction() as tr:
    pager = tr.pager("opportunities", where=[["stage", "==", "proposal"], ["amount", ">=", 5000]],
                     order_by=[("amount", True), "number"], page_size=50)
    first = [(d.number, d.amount) for d in pager.page(0)][:3]
    token = pager.token()
    print(pager.rows, "rows,", pager.pages, "pages; first rows:", first, "| token:", type(token).__name__)

erp_app.advance(db, 242, "won", "cy")                 # the best proposal is won meanwhile

resumed = db.resume_pager(token)                      # a later request, or another process
print("page 0 again:", [(d.number, d.amount) for d in resumed.page(0)][:3])
print("last page:", len(resumed.page(resumed.pages - 1)), "rows")
with db.new_transaction() as tr:
    print("a new query now finds", from_collection(tr.get_root_object("opportunities"))
          .where((F.stage == "proposal") & (F.amount >= 5000)).count(), "rows")
```

Output:

```text
180 rows, 4 pages; first rows: [(242, 9954), (485, 9945), (728, 9936)] | token: bytes
page 0 again: [(242, 9954), (485, 9945), (728, 9936)]
last page: 30 rows
a new query now finds 179 rows
```

The resumed pager still answers from the snapshot the token names, so the report is consistent from its
first page to its last, while a new query sees the change. A token is bytes: store it in a session or a URL
and resume it anywhere `db` is open. The [pager and rank](../../source/design-guide/pager-and-rank.rst)
page of the design guide explains how a page is found without reading the pages before it.

## Key-ordered access on a Dictionary

A `Dictionary` is ordered by key, so it can be walked from a key (*keyset* pagination) and addressed by
position:

```python
with db.new_transaction() as tr:
    deals = tr.get_root_object("opportunities")
    keys = deals.keys(after=995)
    print("keys from 995:", list(keys))
    print("key at position 10:", deals.key_at_position(10), "| position of key 500:", deals.position_of(500))
```

Output:

```text
keys from 995: [996, 997, 998, 999]
key at position 10: 10 | position of key 500: 500
```

`keys(after=k)` returns the keys after `k`, in order. Keyset pagination remembers the last key of a page and
asks for the keys after it: the walk starts at that key in the tree, without counting the rows before it, and
a concurrent insertion never shifts the next page. Positional access runs in O(log n) from the tree's node counts.

## Which to use

| Need | Use |
|---|---|
| a short result, or one that does not change while it is read | `skip` / `take` |
| a long report that must not repeat or skip rows while others write | a pager and its token |
| walking a dictionary in key order, page after page | `keys(after=k)` |
| "row number 10" of a dictionary | `key_at_position` / `position_of` |

Under concurrency the choice also decides what a read declares: a window over a `Dictionary` by offset depends
on every key before it, `keys(after=k)` only on the keys it read, and positional access on the whole
collection ([chapter 6](06-queries-under-concurrency.md)).

```python
space.close()
```

Next: [5. Range, vector and custom indexes](05-range-vector-and-custom-indexes.md)
