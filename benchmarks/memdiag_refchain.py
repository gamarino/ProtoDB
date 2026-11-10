"""Referrer chain of an early-built DBObject at the end of a build (default atom cache)."""
import gc, os, sys
sys.path.insert(0, os.getcwd())
from benchmarks.workloads.inventory import Model, ProtoInventory, site_rows
from proto_db.core.dbobject import DBObject
SITES, WORK = int(sys.argv[1]), sys.argv[2]
m = Model(SITES); os.makedirs(WORK)
p = ProtoInventory(os.path.join(WORK, "protodb")); p.create()
for s in range(SITES):
    p.add_site(site_rows(m, s))
gc.collect()
cache = p.space._atoms._cache
items = cache._items
cached = set(id(v) for v in items.values())
objs = [o for o in gc.get_objects() if type(o) is DBObject]
early = [o for o in objs if getattr(o, "site", None) == 0 and getattr(o, "model", None) is not None]
print("live DBObjects", len(objs), "cache entries", len(items), "cached DBObjects", sum(id(o) in cached for o in objs))
target = early[0]
print("target: equipment of site 0, in cache:", id(target) in cached)
# BFS over referrers until a cached atom is reached
frontier, seen, parent = [target], {id(target)}, {}
ignore = {id(objs), id(early), id(frontier)}
hit = None
for depth in range(40):
    nxt = []
    for o in frontier:
        for r in gc.get_referrers(o):
            if id(r) in seen or id(r) in ignore or type(r).__name__ in ("frame", "list"):
                continue
            seen.add(id(r)); parent[id(r)] = o; nxt.append(r)
            if id(r) in cached:
                hit = r; break
        if hit is not None: break
    if hit is not None or not nxt: break
    frontier = nxt
ignore.add(id(nxt))
chain, o = [], hit
while o is not None and o is not target:
    chain.append(type(o).__name__); o = parent.get(id(o))
print("reached a cached atom:", hit is not None, "after", len(chain), "hops; chain from cached atom down:", " <- ".join(reversed(chain[:12])) if chain else None)
print("cached atom type:", type(hit).__name__ if hit is not None else None, "is a Dictionary root/AVL node of the latest root:", hit is not None)
