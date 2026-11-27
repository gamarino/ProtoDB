# 7. Performance pitfalls

Every figure in this chapter is measured: either here, by the code beside it, or by the repository's
performance suite, cited with its source. Most of the cost of a query is the number of stored objects it has to
decode, so the chapter counts those first: `ObjectSpace.atom_cache_stats()["loads"]` is the number of atoms
read from storage and decoded, and a freshly opened space starts with an empty cache, which makes the count
exact and repeatable. Times follow as samples of one run, all taken on one machine: an AMD Ryzen 5
5500U, CPython 3.14.6 free-threaded, the spaces in a temporary directory on tmpfs, at 2.0.0rc5 (this tutorial's
branch). The suite figures cited come from another commit and are labelled with it.

**About the cited suite figures.** The repository's performance suite was last run at commit `57fd351`
(2.0.0.dev0, on the same model of machine, spaces on ZFS), **before the precondition model**: none of its
figures measures the 2.0.0rc5 commit path, and they are to be re-measured before 2.0.0
([performance](../../source/performance.rst), "How to read these numbers"). They are cited here for the
shape of a cost (an index against a scan, a page by rank), not as the cost in this release.

## The space

5,000 opportunities on disk, with the indexes of the ERP (`by_stage`, `by_amount`, `by_owner`):

```python
import os
import tempfile
import time

from proto_db import F, FileStorage, ObjectSpace, from_collection

import erp_app
from erp_app import plan_of

workdir = tempfile.TemporaryDirectory(prefix="proto_db-tutorial-")   # this chapter's own directory
home = os.getcwd()
os.chdir(workdir.name)                                # its files stay out of yours

space = ObjectSpace(FileStorage("perf-space"))
erp_app.seed(space.new_database("erp"), opportunities=5000)
space.close()

def cold(read):
    """Open the space afresh, run ``read`` on the opportunities; return (result, atoms decoded, seconds)."""
    fresh = ObjectSpace(FileStorage("perf-space"))
    tr = fresh.open_database("erp").new_transaction()
    deals = tr.get_root_object("opportunities")
    before, start = fresh.atom_cache_stats()["loads"], time.perf_counter()
    result = read(deals)
    elapsed = time.perf_counter() - start
    loads = fresh.atom_cache_stats()["loads"] - before
    tr.abort()
    fresh.close()
    return result, loads, elapsed

with ObjectSpace(FileStorage("perf-space")) as opened:
    print(len(opened.open_database("erp").new_transaction().get_root_object("opportunities")), "opportunities")
```

Output:

```text
5000 opportunities
```

## Pitfall 1: a scan where an index would do

The same question, through an index and through a scan:

```python
shapes = {
    "amount == 9991 (range index)":     lambda d: from_collection(d).where(F.amount == 9991).count(),
    "title == ... (no index: a scan)":  lambda d: from_collection(d).where(F.title == "opportunity 4242").count(),
    "a lambda (a PythonFilter)":        lambda d: from_collection(d).where(lambda o: o.amount == 9991).count(),
    "title.startswith (a PythonFilter)": lambda d: from_collection(d).where(F.title.startswith("opportunity 4242")).count(),
}
measured = {label: cold(read) for label, read in shapes.items()}
for label, (rows, loads, _) in measured.items():
    print(f"{label:36} {rows:3} rows, {loads:6} atoms decoded")
```

Output:

```text
amount == 9991 (range index)           1 rows,     16 atoms decoded
title == ... (no index: a scan)        1 rows,  10007 atoms decoded
a lambda (a PythonFilter)              1 rows,  10007 atoms decoded
title.startswith (a PythonFilter)      1 rows,  10007 atoms decoded
```

```python
for label, (_, _, elapsed) in measured.items():
    print(f"{label:36} {elapsed * 1000:8.1f} ms (cold)")
```

Sample output (one run on the machine described above; the atom counts above are exact):

```text
amount == 9991 (range index)              2.6 ms (cold)
title == ... (no index: a scan)         727.4 ms (cold)
a lambda (a PythonFilter)               714.8 ms (cold)
title.startswith (a PythonFilter)       707.7 ms (cold)
```

A lookup decodes the few tree nodes on the path to its key; a scan decodes every element (two atoms each: the
tree node and the record, 10,007 for 5,000 elements). A Python predicate, however selective, is a scan: the
time goes to reading and decoding, and the three scans cost about the same. The suite shows the same shape at
20,000 records: a point read through a secondary index at 78.1 us per query, the same read with no index
at 47.1 ms ([performance](../../source/performance.rst), "Point reads"; commit `57fd351`, before the precondition
model). Index what you query, and write conditions with `F` and a value so that an index can answer them.

## Pitfall 2: sorting what an index already orders

```python
for label, read in (("order_by(amount).take(10)", lambda d: from_collection(d).order_by(F.amount).take(10).to_list()),
                    ("order_by(title).take(10)", lambda d: from_collection(d).order_by(F.title).take(10).to_list())):
    rows, loads, _ = cold(read)
    print(f"{label:28} {len(rows)} rows, {loads:6} atoms decoded")
```

Output:

```text
order_by(amount).take(10)    10 rows,     35 atoms decoded
order_by(title).take(10)     10 rows,  10007 atoms decoded
```

The amount index yields its elements in order and stops after ten; ordering by an attribute without a range
index reads and sorts everything before it keeps ten. Add a `RangeIndex` on the attributes your reports sort
by.

## Pitfall 3: counting with a query

```python
for label, read in (("len(collection)", len), ("query count, no condition", lambda d: from_collection(d).count()),
                    ("count of a lookup", lambda d: from_collection(d).where(F.stage == "lead").count())):
    rows, loads, _ = cold(read)
    print(f"{label:28} {rows:5}, {loads:6} atoms decoded")
```

Output:

```text
len(collection)               5000,      0 atoms decoded
query count, no condition     5000,      7 atoms decoded
count of a lookup             1667,      9 atoms decoded
```

A collection knows its size: `len` reads the count the root node already holds, and a `count()` with no condition
and a `count()` answered by a lookup read only a few nodes, because the planner counts from the tree and the
index instead of reading elements. What is expensive is a count over a scan (pitfall 1). Inside a transaction
that writes, `len` declares the whole collection (chapter 6).

## Pitfall 4: indexes cost every write

An index is updated in the same step as its collection, so every write pays for every index. The block times
200 single-opportunity updates, each its own commit, with no index and with the ERP's three (durability
`none`, so that the device's `fsync` does not hide the difference):

```python
def update_cost(indexes):
    path = f"writes-{indexes}"
    store = ObjectSpace(FileStorage(path, durability="none"))
    db = store.new_database("erp")
    erp_app.seed(db, opportunities=5000, indexes=indexes)
    size, start = sum(os.path.getsize(os.path.join(path, n)) for n in os.listdir(path)), time.perf_counter()
    for n in range(200):
        with db.new_transaction() as tr:
            deals = tr.get_root_object("opportunities")
            tr.set_root_object("opportunities", deals.set_at(n, deals.get_at(n).with_(amount=1234, stage="won")))
    elapsed = time.perf_counter() - start
    grown = sum(os.path.getsize(os.path.join(path, n)) for n in os.listdir(path)) - size
    store.close()
    return elapsed / 200, grown / 200

plain, indexed = update_cost(False), update_cost(True)
assert indexed[1] > plain[1]                         # the indexes' paths are written too
print(f"no index:  {plain[0] * 1e6:6.0f} us and {plain[1]:6.0f} bytes per update")
print(f"3 indexes: {indexed[0] * 1e6:6.0f} us and {indexed[1]:6.0f} bytes per update")
```

Sample output (one run on the machine described above; the bytes vary slightly because each version stores
its commit time):

```text
no index:    2922 us and   3050 bytes per update
3 indexes:  12388 us and  19544 bytes per update
```

In this run three indexes made each update about four times slower and six times larger: each index adds the
path from its root to the changed entry to every commit. The figure is this sample's, at this size and with
`durability="none"`; the suite measures other workloads and is not comparable. Index what you query, not
every attribute.

## Pitfall 5: reads that widen conflicts

Under concurrency, a read costs more than its time: it is a precondition. Chapter 6 measured the reads that
declare the whole collection (scans, `len`, `explain()` of a scan, `!=`, a counted window) and the escalation
past 1,024 fine reads per root. A transaction that scans and then writes conflicts with every concurrent
writer of that collection and redoes its work on each retry; the [concurrency chapter of the complete
tutorial](../05-concurrency.md) measured the attempts.

## Pitfall 6: deep offsets without a pager

A deep page by `skip` is cheap over an ordered index (chapter 4 shows the plan), and the pager seeks by rank:
the suite shows a pager page at 222-251 us from offset 0 to offset 900,000 on 10^6 rows
([performance](../../source/performance.rst), "Deep pages"; commit `57fd351`, before the precondition model), so
a page's cost does not grow with its offset. What costs is an offset over
a sort the planner has to do itself (an `order_by` on an attribute without a range index), which reads the
whole result for every page.

```python
_, loads_ordered, _ = cold(lambda d: from_collection(d).order_by(F.amount).skip(4000).take(10).to_list())
_, loads_sorted, _ = cold(lambda d: from_collection(d).order_by(F.title).skip(4000).take(10).to_list())
print(f"page at offset 4000: {loads_ordered} atoms over the amount index, {loads_sorted} sorting by title")

os.chdir(home)
workdir.cleanup()                                     # removes only the directory this chapter created
```

Output:

```text
page at offset 4000: 40 atoms over the amount index, 10007 sorting by title
```

## Checklist

* `explain()` every query that runs often: a `CollectionScan` or `PythonFilter` in a hot path is the first
  thing to fix.
* Index the attributes your conditions and your sorts use, and no others.
* Prefer `F` comparisons with values to lambdas, `startswith` and computed values; a computed key with an index
  answers most of what a lambda would.
* Use `len` to size a collection, a pager for long reports, `keys(after=k)` to walk a dictionary.
* Inside a transaction that writes, read only what the decision needs.

Back to [the queries tutorial](README.md).
