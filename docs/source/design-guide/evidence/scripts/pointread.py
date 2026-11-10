"""Point-read cost on a 100k-entry Dictionary, warm shared cache, and the cost of one decode.

python -X gil=0 pointread.py   (run from the tree to measure)
Reports us per get_at (same transaction, warm), the number of node edges per lookup
(tree depth + value), so edge-level overheads can be scaled, and us per atom decode (a miss).
"""
import os
import random
import sys
import time

sys.path.insert(0, os.getcwd())
from proto_db import MemoryStorage, ObjectSpace  # noqa
from proto_db.collections import avl  # noqa

N = 100_000
space = ObjectSpace(MemoryStorage(), atom_cache_entries=400_000)
db = space.new_database("d")


def load(tr):
    d = tr.new_dictionary()
    for i in range(N):
        d = d.set_at(i, tr.new_object(id=i, name=f"n{i}"))
    tr.set_root_object("d", d)


db.run(load)
tr = db.new_transaction()
d = tr.get_root_object("d")
keys = [random.randrange(N) for _ in range(50_000)]
for k in keys:
    assert d.get_at(k).id == k
t0 = time.perf_counter()
for k in keys:
    d.get_at(k)
warm = (time.perf_counter() - t0) * 1e6 / len(keys)


def depth(k):
    n, dep, ok = d.root, 0, avl.order_key(k)
    while n is not None:
        dep += 1
        nk = n._order_key()
        if ok < nk:
            n = n.left
        elif ok > nk:
            n = n.right
        else:
            return dep
    return dep


edges = sum(depth(k) for k in keys[:5000]) / 5000 + 1
tr.abort()
# decode cost: a space without a decoded-atom cache, every node a fresh stub
space0 = ObjectSpace(space.storage, atom_cache_entries=0)
db0 = space0.open_database("d")
t = db0.new_transaction()
d0 = t.get_root_object("d")
t0 = time.perf_counter()
for k in keys[:5000]:
    d0.get_at(k)
first = time.perf_counter() - t0
nodes = sum(1 for o in __import__("gc").get_objects() if type(o) is avl.KeyNode and o.__dict__.get("_loaded") and o.context is t)
print({"warm_us_per_get": round(warm, 2), "edges_per_get": round(edges, 1),
       "cold_5000_gets_s": round(first, 3), "decoded_nodes": nodes,
       "us_per_decoded_node_approx": round(first * 1e6 / max(nodes, 1), 1)})
