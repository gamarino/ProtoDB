"""Edge census: which edges keep loaded atoms reachable from the decoded-atom cache.

python -X gil=0 census.py SITES WORKDIR [--atoms=N]
Run from the repository (or an export) directory.  Builds the inventory site by site, then:
- counts loaded atoms alive (gc) by type, and the cache entries (loaded / stub);
- BFS from the cache entries through loaded atoms only, walking each atom's persisted state
  (attributes, history links, nested tuples / lists / dicts); every edge parent -> loaded child
  is classified as (parent type, attribute, nested?, child type);
- reports loaded atoms reachable from the cache vs. alive.
"""
import collections
import gc
import json
import os
import sys
import time

sys.path.insert(0, os.getcwd())
if "--resident" in sys.argv:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import resident  # noqa
from benchmarks.workloads.inventory import Model, ProtoInventory, site_rows  # noqa
from proto_db.core.atom import Atom, RESERVED_NAMES  # noqa

SITES, WORK = int(sys.argv[1]), sys.argv[2]
ATOMS = next((int(a.split("=")[1]) for a in sys.argv if a.startswith("--atoms=")), None)


def loaded(a):
    return a.__dict__.get("_loaded", True)


def tname(o):
    return type(o).__name__


def children(atom):
    """Yield (attr, nested, child) for every Atom in atom's persisted state."""
    d = atom.__dict__
    for k, v in list(d.items()):
        if k.startswith("_") or k in RESERVED_NAMES:
            continue
        yield from walk(k, v, False)
    links = d.get("_links") or {}
    for k, ref in links.items():
        if isinstance(ref, Atom):
            yield (k + "(link)", False, ref)


def walk(attr, v, nested):
    if isinstance(v, Atom):
        yield (attr, nested, v)
    elif isinstance(v, (tuple, list)):
        for x in v:
            yield from walk(attr, x, True)
    elif isinstance(v, dict):
        for x in v.values():
            yield from walk(attr, x, True)


m = Model(SITES)
os.makedirs(WORK, exist_ok=False)
p = ProtoInventory(os.path.join(WORK, "protodb"))
if ATOMS is not None:
    from proto_db import FileStorage, ObjectSpace
    p.space = ObjectSpace(FileStorage(os.path.join(WORK, "protodb"), durability="none"), atom_cache_entries=ATOMS)
p.create()
t0 = time.perf_counter()
built = 0
for s in range(SITES):
    rows = site_rows(m, s)
    p.add_site(rows)
    built += 1 + sum(len(rows[k]) for k in ("equipment", "cards", "ports", "links"))
el = time.perf_counter() - t0
gc.collect()
alive = [o for o in gc.get_objects() if isinstance(o, Atom)]
alive_loaded = collections.Counter(tname(o) for o in alive if loaded(o))
alive_stub = collections.Counter(tname(o) for o in alive if not loaded(o))
atoms = p.space._atoms
entries = [] if atoms is None else [v[0] for v in atoms._cache._items.values()]
seen = set()
frontier = [e for e in entries if loaded(e)]
for e in frontier:
    seen.add(id(e))
edges = collections.Counter()
reach = collections.Counter()
depth = 0
while frontier:
    nxt = []
    for a in frontier:
        reach[tname(a)] += 1
        for attr, nested, c in children(a):
            if not loaded(c):
                continue
            edges[(tname(a), attr, nested, tname(c))] += 1
            if id(c) not in seen:
                seen.add(id(c))
                nxt.append(c)
    frontier = nxt
    depth += 1
cached_ids = set(id(e) for e in entries)
out = {
    "sites": SITES, "objects": built, "build_s": round(el, 1), "atom_cache": ATOMS,
    "resident": "--resident" in sys.argv, "stats": None if atoms is None else atoms.stats(),
    "cache_entries": len(entries), "cache_loaded": sum(loaded(e) for e in entries),
    "alive_loaded_total": sum(alive_loaded.values()), "alive_stub_total": sum(alive_stub.values()),
    "alive_loaded": alive_loaded.most_common(), "alive_stub": alive_stub.most_common(),
    "reachable_loaded_from_cache": sum(reach.values()), "reachable_not_cached": len(seen - cached_ids),
    "reachable_by_type": reach.most_common(), "bfs_depth": depth,
    "pinning_edges_to_loaded_children": [[*k, n] for k, n in edges.most_common()],
}
print(json.dumps(out, indent=1, default=str))
