# 5. Range, vector and custom indexes

The three built-in index kinds are equality, range and vector. This chapter goes through range queries and
their bounds, vector queries combined with ordinary conditions, and the two ways to go beyond the built-in
kinds: an index over a **computed key**, and an **`Index` subclass** of your own.

```python
from proto_db import F, MemoryStorage, ObjectSpace, RangeIndex, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
```

## Range bounds

`between(low, high)` includes both bounds; the variants exclude one or both, and `range(low, high, bounds)`
spells the bounds as an interval. The two smallest amounts in the data are 1,000 and 1,001:

```python
amounts = lambda query: sorted(d.amount for d in query.to_list())
for label, condition in [
    ("between(1000, 1001)", F.amount.between(1000, 1001)),
    ("between_open", F.amount.between_open(1000, 1001)),
    ("between_left_open", F.amount.between_left_open(1000, 1001)),
    ("between_right_open", F.amount.between_right_open(1000, 1001)),
    ('range(1000, 1001, "[)")', F.amount.range(1000, 1001, "[)")),
    ("amount < 1001", F.amount < 1001),
]:
    query = from_collection(deals).where(condition)
    print(f"{label:24} {plan_of(query):12} {amounts(query)}")
```

Output:

```text
between(1000, 1001)      IndexRange   [1000, 1001]
between_open             IndexRange   []
between_left_open        IndexRange   [1001]
between_right_open       IndexRange   [1000]
range(1000, 1001, "[)")  IndexRange   [1000]
amount < 1001            IndexRange   [1000]
```

A range is over one total order across types, the same for indexes and scans: numbers of every type compare
exactly (`1 == 1.0 == Decimal(1)`), strings in code-point order, dates and datetimes in their own ranks.

## A prefix is a range

`startswith` is a `PythonFilter`, which no index can answer. Over a `RangeIndex` on a string, the same
question is a range from the prefix to the prefix followed by the next character, and the index answers it:

```python
with db.new_transaction() as wtr:
    wtr.set_root_object("opportunities", wtr.get_root_object("opportunities").add_index(RangeIndex("by_title", "title")))
tr.abort()
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")

prefix = "opportunity 99"
as_filter = from_collection(deals).where(F.title.startswith(prefix))
as_range = from_collection(deals).where(F.title.range(prefix, prefix[:-1] + chr(ord(prefix[-1]) + 1), "[)"))
print(plan_of(as_filter), as_filter.count(), "|", plan_of(as_range), as_range.count(),
      "| same rows:", {d.number for d in as_filter.to_list()} == {d.number for d in as_range.to_list()})
```

Output:

```text
PythonFilter <- CollectionScan 11 | IndexRange 11 | same rows: True
```

## Vector queries

A `VectorIndex` answers `near`: the k nearest, the ones above a similarity threshold, or both. Combined with
other conditions by `&`, the other conditions go to the vector index, which finds the nearest **among** the
elements that satisfy them. The embeddings below are seeded random vectors, one cluster per topic (an
application gets them from an embedding model; `proto_db` never computes them). This section needs numpy.

```python
import random

from proto_db import VectorIndex

rng = random.Random(5)
centres = [[rng.uniform(-1, 1) for _ in range(4)] for _ in range(3)]
with db.new_transaction() as wtr:
    notes = wtr.new_dictionary().add_index(VectorIndex("by_meaning", "vec", 4, metric="cosine"))
    for n in range(300):
        vec = tuple(x + rng.gauss(0, 0.3) for x in centres[n % 3])
        notes = notes.set_at(n, wtr.new_object(number=n, topic=n % 3, amount=1000 + (n * 37) % 9000, vec=vec))
    wtr.set_root_object("notes", notes)

notes = db.new_transaction().get_root_object("notes")
probe = notes.get_at(7).vec
for label, condition in [
    ("near, k=4", F.vec.near(probe, k=4)),
    ("near, k=4, amount >= 8000", F.vec.near(probe, k=4) & (F.amount >= 8000)),
    ("near, threshold 0.99", F.vec.near(probe, threshold=0.99)),
]:
    query = from_collection(notes).where(condition)
    found = query.to_list()
    print(f"{label:28} {plan_of(query):30} {[d.number for d in found]} topics {sorted({d.topic for d in found})}")
```

Output:

```text
near, k=4                    VectorSearch <- VectorExact    [7, 130, 22, 67] topics [1]
near, k=4, amount >= 8000    VectorSearch <- VectorExact    [208, 214, 232, 211] topics [1]
near, threshold 0.99         VectorSearch <- VectorExact    [7, 130, 22, 67, 154, 208, 214, 46, 31, 295, 79, 124] topics [1]
```

The second query returned four elements of 8,000 or more, nearest first; filtering the four nearest
afterwards would have returned fewer. The backends (`exact`, `ivf_flat`, optional `hnsw`), their recall and
their behaviour under concurrency are in the [vector search tutorial](../../source/tutorials/vector-search.rst)
and in chapter 9 of the [complete tutorial](../09-vector-search.md).

## An index over a computed key

Most needs beyond "this attribute" are a function of the element: a lower-cased name, a bucket, a combination
of two fields. A registered extractor gives it a name; an equality or range index over it is an ordinary
index, and `F["@name"]` queries it:

```python
from proto_db import EqualityIndex, KeySpec, register_extractor

register_extractor("stage_and_band", lambda deal: (deal.stage, deal.amount // 1000), version="1")

with db.new_transaction() as wtr:
    wtr.set_root_object("opportunities", wtr.get_root_object("opportunities")
                        .add_index(EqualityIndex("by_stage_band", KeySpec.extractor("stage_and_band"))))
tr.abort()
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
both = from_collection(deals).where(F["@stage_and_band"] == ("proposal", 9))
apart = from_collection(deals).where((F.stage == "proposal") & (F.amount.between(9000, 9999)))
print(plan_of(both), both.count(), "|", plan_of(apart), apart.count())
```

Output:

```text
IndexLookup 36 | Filter <- IndexRange 36
```

`proto_db` has no composite index; a tuple key over an extractor is one, answered by a single lookup instead
of an index and a residual filter. The rules for extractors are those of chapter 3: registered in every
process, pure, versioned.

## An `Index` subclass

A question no equality, range or vector index can answer (substrings, geometry, a custom similarity) needs a
new index kind. A subclass of `proto_db.Index` implements a few hooks, and the planner, the collections and
the storage need no change:

| Hook | What it does |
|---|---|
| `__init__(name, key_spec)` | an empty index; its public attributes are its persisted state |
| `with_added(element)`, `with_removed(element)` | return a **new** index; never modify the receiver |
| `supports(term)` | can this index answer this term *exactly*? |
| `estimate(term)`, `plan(term)` | the planner compares estimates, then runs the chosen plan |
| `supports_order` / `ordered_plan`, `supports_filtered` / `filtered_plan` | optional: ordered reads, filtered searches |

Two rules carry the correctness. The answer must equal a scan's for **every** element, including the awkward
ones (a missing attribute, a list where a string was expected), so a plan prunes and then checks each
candidate with the term's own rule. And the class must be importable in every process that opens the
database, under a stable `__type_name__`.

Under the [precondition model](../05-concurrency.md), a custom index keeps the fine units of its collection
only if it opts in (`_keyed_reapplication = True` and the `with_added_key`, `with_removed_key` and
`with_replaced_keys` hooks); otherwise every write to its collection takes the whole collection.

[Writing a custom index](../../source/tutorials/custom-index.rst) builds a trigram index for substring search
step by step, and proves it with the repository's index contract (`tests/contracts/index_contract.py`), which
also rejects a deliberately broken variant. Before writing one, check whether a computed key answers the
question: it usually does.

```python
tr.abort()
space.close()
```

Next: [6. Queries under concurrency](06-queries-under-concurrency.md)
