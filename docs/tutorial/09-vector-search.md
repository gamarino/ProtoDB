# 9. Vector search

A vector index finds the elements whose embedding is nearest to a query vector. In the ERP it answers "which
opportunities are like this one?" from an embedding of each opportunity's description. It is the third index
kind and works like the other two: it belongs to a collection, every change keeps it current, and it takes
part in the same planner, so `near` combines with ordinary conditions in one query. This chapter needs numpy:
`pip install --pre "proto_db[vectors]"`.

## Opportunities with embeddings

`proto_db` stores and indexes the vectors you give it; it never computes them. In an application they come
from an embedding model. Here they are seeded random vectors, so the chapter is deterministic, and each
opportunity's vector is a small perturbation of one of three "topics" so that neighbours exist:

```python
import math
import random

from proto_db import F, MemoryStorage, ObjectSpace, VectorIndex, from_collection

DIM = 8
rng = random.Random(11)
TOPICS = {name: [rng.uniform(-1, 1) for _ in range(DIM)] for name in ("licences", "hardware", "services")}

def embedding(topic):
    return tuple(x + rng.gauss(0, 0.35) for x in TOPICS[topic])

def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
rows = [(n, list(TOPICS)[n % 3], ("lead", "qualified", "proposal", "won")[n % 4]) for n in range(600)]
with db.new_transaction() as tr:
    deals = tr.new_dictionary().add_index(VectorIndex("by_meaning", "vec", DIM, metric="cosine"))
    for number, topic, stage in rows:
        deals = deals.set_at(number, tr.new_object(number=number, topic=topic, stage=stage, vec=embedding(topic)))
    tr.set_root_object("opportunities", deals)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(len(deals), deals.index_names(), deals.get_index("by_meaning").explain()["kind"])
tr.abort()
```

Output:

```text
600 ['by_meaning'] VectorIndex
```

`VectorIndex(name, path, dim, metric, backend)` fixes the dimension, the metric (`"cosine"`, `"l2"` or
`"ip"`, the inner product) and the backend, `"exact"` by default. An element whose vector is missing or of
another dimension stays in the collection and is never returned by a search.

## The nearest opportunities

```python
def plan_of(query):
    names, node = [], query.explain()
    while node:
        names.append(node["plan"])
        node = node["children"][0] if node.get("children") else None
    return " <- ".join(names)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
target = deals.get_at(42).vec
like_42 = from_collection(deals).where(F.vec.near(target, k=5))
found = [d.number for d in like_42.to_list()]
brute = [n for n, _ in sorted(((n, cosine(target, deals.get_at(n).vec)) for n in range(600)), key=lambda p: -p[1])][:5]
print(plan_of(like_42), "|", found, "| same as brute force:", found == brute)
print("topics:", {deals.get_at(n).topic for n in found})
```

Output:

```text
VectorSearch <- VectorExact | [42, 378, 384, 573, 348] | same as brute force: True
topics: {'licences'}
```

The `exact` backend ranks every vector and re-scores the candidates exactly, so its answer equals brute force.
Results come nearest first, and opportunity 42 is its own first neighbour.

## Nearest *among* the ones that match

"The three won deals most like this one" is not "the three nearest, then keep the won ones", which here
returns none: none of the five nearest is won. The other conditions go to the index, which finds the k nearest
among the elements that satisfy them:

```python
won_like_42 = from_collection(deals).where(F.vec.near(target, k=3) & (F.stage == "won"))
got = [d.number for d in won_like_42.to_list()]
expected = [n for n in sorted(range(600), key=lambda n: -cosine(target, deals.get_at(n).vec))
            if deals.get_at(n).stage == "won"][:3]
print(got, [deals.get_at(n).stage for n in got], got == expected)
close = from_collection(deals).where(F.vec.near(target, threshold=0.9)).to_list()
print(len(close), "with cosine similarity >= 0.9:",
      {d.number for d in close} == {n for n in range(600) if cosine(target, deals.get_at(n).vec) >= 0.9})
tr.abort()
space.close()
```

Output:

```text
[87, 327, 183] ['won', 'won', 'won'] True
2 with cosine similarity >= 0.9: True
```

Give `k`, a `threshold`, or both ("the k nearest among those at least this similar").

## Choosing a backend

* `exact` is the default and never approximate; its cost grows with the collection, because it reads every
  vector.
* `ivf_flat` clusters the vectors and searches only the `nprobe` nearest of `nlist` clusters: it trades
  recall for speed. Measure recall on your own data before choosing it; the
  [vector search tutorial](../source/tutorials/vector-search.rst) shows how.
* `hnsw` is optional (`pip install --pre "proto_db[hnsw]"`); `hnswlib` re-enables the GIL on free-threaded
  CPython when it is imported.
* Under concurrency the `exact` and `hnsw` indexes keep the fine units of chapter 5: writers of different
  opportunities do not conflict. An `ivf_flat` index cannot (its lists depend on the whole collection), so
  every write to a collection that carries one takes the whole collection. A vector search itself reads the
  whole collection.

Measured latency and recall by backend and size are in [performance](../source/performance.rst).

Depth: [vector search](../source/tutorials/vector-search.rst) and [range and vector queries in the queries
tutorial](queries/05-range-vector-and-custom-indexes.md).

Next: [10. Cluster](10-cluster.md)
