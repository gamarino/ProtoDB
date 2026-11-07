"""Residency micro-probe (Task 8.3m, rulings R105 M1 and R114 N5): point lookups with a decoded-atom
cache that fits the working set, and with one about 20 % smaller than it.

A 20,000-record dictionary with one equality index is built, the cache is emptied, and the same
4,000 random keys are looked up repeatedly.  The working set is the number of atoms resident
after one pass with a capacity that fits.  For each capacity, after two warm-up passes, seven
timed passes run; the probe reports the best pass in microseconds per lookup, the unloads and
loads per lookup over the seven passes, and ``unloads / loads`` (the thrash signal, D53).  Each
pass verifies the sum of the ids it read.

Run from the root of the tree to measure (it imports ``proto_db`` from the current directory)::

    python -X gil=0 -m benchmarks.residency_probe WORKDIR [LABEL]

It prints one JSON line.
"""
import json
import os
import random
import shutil
import sys
import time

sys.path.insert(0, os.getcwd())

from proto_db import EqualityIndex, FileStorage, ObjectSpace  # noqa: E402

N = 20_000
LOOKUPS = 4_000
TIMED = 7


def _build(path, capacity):
    shutil.rmtree(path, ignore_errors=True)
    space = ObjectSpace(FileStorage(path, durability="none"), atom_cache_entries=capacity)
    db = space.new_database("b")
    with db.new_transaction() as tr:
        d = tr.new_dictionary().add_index(EqualityIndex("by_c", "c"))
        for i in range(N):
            d = d.set_at(i, tr.new_object(id=i, c=i % 100, s=f"s{i}"))
        tr.set_root_object("d", d)
    return space, db


def _pass(db, keys):
    tr = db.new_transaction()
    try:
        d = tr.get_root_object("d")
        t = time.perf_counter()
        total = sum(d.get_at(k).id for k in keys)
        elapsed = time.perf_counter() - t
    finally:
        tr.abort()
    assert total == sum(keys), "a lookup returned the wrong record"
    return elapsed


def main(workdir, label=""):
    rng = random.Random(5)
    keys = [rng.randrange(N) for _ in range(LOOKUPS)]
    space, db = _build(os.path.join(workdir, "probe"), 1_000_000)
    space._atoms._unload_all()
    _pass(db, keys)
    working_set = space.atom_cache_stats()["entries"]
    space.close()
    out = {"label": label, "gil": sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else None,
           "records": N, "lookups_per_pass": LOOKUPS, "timed_passes": TIMED, "working_set": working_set}
    for name, capacity in (("fit", 1_000_000), ("over20", int(working_set / 1.2))):
        space, db = _build(os.path.join(workdir, name), capacity)
        space._atoms._unload_all()
        _pass(db, keys)
        _pass(db, keys)
        before = space.atom_cache_stats()
        best = min(_pass(db, keys) for _ in range(TIMED))
        after = space.atom_cache_stats()
        space.close()
        unloads = after["unloads"] - before["unloads"]
        loads = after.get("loads", 0) - before.get("loads", 0)
        lookups = LOOKUPS * TIMED
        out[name] = {"capacity": capacity, "us_per_lookup": round(best / LOOKUPS * 1e6, 2),
                     "unloads_per_lookup": round(unloads / lookups, 3),
                     "loads_per_lookup": round(loads / lookups, 3),
                     "unloads_per_load": round(unloads / loads, 3) if loads else 0.0}
    print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "")
