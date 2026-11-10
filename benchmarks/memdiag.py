"""Who holds the memory during the inventory build?  Run from the clean export directory:

    python -X gil=0 memdiag.py SITES WORKDIR [--no-sqlite] [--tracemalloc]

Builds the inventory site by site exactly as the workload does, and at checkpoints (25 %, 50 %,
100 % of the sites) reports:
- RSS (from /proc/self/status) and the number of gc-tracked objects by type (top 25 and deltas);
- the size of every container reachable within 3 attribute levels from the ObjectSpace and the
  Database (lengths > 1000), to locate an unbounded structure;
- the atom cache and query cache stats;
- with --tracemalloc, the top 15 allocation sites by size growth between checkpoints.
"""
import collections
import gc
import json
import os
import sys
import time
import tracemalloc

sys.path.insert(0, os.getcwd())
from benchmarks.workloads.inventory import Model, ProtoInventory, SqlInventory, site_rows, OBJECTS_PER_SITE  # noqa

SITES = int(sys.argv[1])
WORK = sys.argv[2]
NO_SQL = "--no-sqlite" in sys.argv
TM = "--tracemalloc" in sys.argv
ATOMS = next((int(a.split("=")[1]) for a in sys.argv if a.startswith("--atoms=")), None)


def rss_mb():
    for line in open("/proc/self/status"):
        if line.startswith("VmRSS"):
            return int(line.split()[1]) / 1024


def by_type():
    c = collections.Counter(f"{type(o).__module__!s}.{type(o).__name__}" for o in gc.get_objects())
    return c


def containers(root, name, depth=3, seen=None, out=None):
    seen = set() if seen is None else seen
    out = [] if out is None else out
    if id(root) in seen or depth < 0:
        return out
    seen.add(id(root))
    if isinstance(root, (dict, list, set, tuple, collections.OrderedDict, collections.deque)):
        try:
            n = len(root)
        except Exception:
            n = -1
        if n > 1000:
            out.append((name, type(root).__name__, n))
        return out
    d = getattr(root, "__dict__", None)
    items = list(d.items()) if isinstance(d, dict) else []
    for slot in getattr(type(root), "__slots__", ()) or ():
        if isinstance(slot, str) and hasattr(root, slot):
            items.append((slot, getattr(root, slot)))
    for k, v in items:
        if isinstance(v, (int, float, str, bytes, bool, type(None))):
            continue
        containers(v, f"{name}.{k}", depth - 1, seen, out)
    return out


m = Model(SITES)
os.makedirs(WORK, exist_ok=False)
proto = ProtoInventory(os.path.join(WORK, "protodb"))
if ATOMS is not None:  # same storage, a space with a different atom cache size (public parameter)
    from proto_db import FileStorage, ObjectSpace
    proto.space.close() if hasattr(proto.space, "close") else None
    proto.space = ObjectSpace(FileStorage(os.path.join(WORK, "protodb"), durability="fsync"), atom_cache_entries=ATOMS)
sql = None if NO_SQL else SqlInventory(os.path.join(WORK, "inventory.sqlite"))
proto.create()
if sql:
    sql.create()
if TM:
    tracemalloc.start(8)
checkpoints = sorted({max(1, SITES // 4), SITES // 2, SITES})
prev_types, prev_snap = None, None
t0 = time.perf_counter()
built = 0
for s in range(SITES):
    rows = site_rows(m, s)
    proto.add_site(rows)
    if sql:
        sql.add_site(rows)
    built += 1 + len(rows["equipment"]) + len(rows["cards"]) + len(rows["ports"]) + len(rows["links"])
    if s + 1 in checkpoints:
        gc.collect()
        types = by_type()
        rep = {"atom_cache_entries": ATOMS, "sites": s + 1, "objects": built, "s": round(time.perf_counter() - t0, 1), "rss_mb": round(rss_mb()),
               "gc_objects": sum(types.values()), "top_types": types.most_common(25)}
        if prev_types is not None:
            delta = types.copy()
            delta.subtract(prev_types)
            rep["delta_types"] = [kv for kv in delta.most_common(15)]
        cs = containers(proto.space, "space") + containers(proto.db, "db")
        rep["big_containers"] = sorted(cs, key=lambda x: -x[2])[:25]
        try:
            rep["atom_cache"] = proto.space.atom_cache_stats()
        except Exception as e:
            rep["atom_cache"] = repr(e)
        if TM:
            snap = tracemalloc.take_snapshot()
            if prev_snap is not None:
                rep["tracemalloc_growth"] = [str(st) for st in snap.compare_to(prev_snap, "traceback")[:12]]
            else:
                rep["tracemalloc_top"] = [str(st) for st in snap.statistics("lineno")[:12]]
            prev_snap = snap
        prev_types = types
        print(json.dumps(rep, default=str), flush=True)
