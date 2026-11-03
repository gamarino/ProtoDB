"""Cold open / recovery time and cache hit rates.

A :class:`~proto_db.FileStorage` space holding ``n`` indexed records and a history of
``commits`` small commits is closed, then reopened by a fresh :class:`~proto_db.ObjectSpace`:

* open: constructing the storage (validates the published root), opening the database and
  reading one record (time to first read);
* cold walk: iterating every record right after the open;
* warm walk: the same in a new transaction on the same open space (the page cache is
  warm; atoms are decoded again: transactions do not share decoded atoms);
* the page cache's hits and misses during each walk (``FileStorage.cache_stats()``).

"Cold" means a new process-level state (new storage instance, empty page cache), not an
empty OS page cache: dropping the kernel cache needs root and is not attempted.

The recovery part opens a :class:`~proto_db.TieredStorage` local tier written by a
one-node cluster with ``commits`` commits: its constructor scans and repairs its logs and
own segments (D46), the cost a restarted node pays before it can serve.  It is verified by
reading the committed counter through a restarted one-node cluster.
"""
import os
import random

from benchmarks.harness import BenchResult, benchmark, measure
from benchmarks.support import MemoryObjectStore, records


@benchmark("cold_open", quick={"n": 300, "commits": 10, "lookups": 50},
           full={"n": 20_000, "commits": 500, "lookups": 2_000})
def cold_open(ctx, p):
    """Cold open (time to first read), cold vs warm full walk, page-cache hit rates, tiered recovery."""
    from proto_db import EqualityIndex, FileStorage, ObjectSpace, RangeIndex
    rows = records(p["n"])
    path = ctx.tempdir("cold")
    space = ObjectSpace(FileStorage(path, durability="none"))
    db = space.new_database("bench")
    with db.new_transaction() as tr:
        coll = tr.new_dictionary().add_index(EqualityIndex("by_code", "code")) \
            .add_index(RangeIndex("by_score", "score"))
        for r in rows:
            coll = coll.set_at(r["id"], tr.new_object(**r))
        tr.set_root_object("rows", coll)
        tr.set_root_object("n", 0)
    for _ in range(p["commits"]):
        db.run(lambda t: t.set_root_object("n", t.get_root_object("n") + 1))
    space.close()
    expected = (len(rows), sum(r["id"] for r in rows))
    result = BenchResult({"rows": len(rows), "history_commits": p["commits"], "random_lookups": p["lookups"]},
                         None, expected)
    probe = rows[len(rows) // 2]

    def open_first_read():
        sp = ObjectSpace(FileStorage(path, durability="none"))
        tr = sp.open_database("bench").new_transaction()
        rec = tr.get_root_object("rows").get_at(probe["id"])
        ok = rec.code == probe["code"] and tr.get_root_object("n") == p["commits"]
        tr.abort()
        sp.close()
        return ok

    samples, ok = measure(open_first_read, ctx.repeat, ctx.warmup, expect=True)
    result.check("first read after open", ok, "")
    result.add_timing("open + first read", samples)

    def walk(sp):
        tr = sp.open_database("bench").new_transaction()
        ids = [rec.id for rec in tr.get_root_object("rows").elements()]
        tr.abort()
        return len(ids), sum(ids)

    hit_rates = {"cold walk": [], "warm walk": []}
    walked = {}
    rng = random.Random(61)
    keys = [rng.randrange(len(rows)) for _ in range(p["lookups"])]
    for _ in range(ctx.repeat):
        storage = FileStorage(path, durability="none")
        sp = ObjectSpace(storage)
        for label in ("cold walk", "warm walk"):
            before = storage.cache_stats()
            walked[label] = walk(sp)
            after = storage.cache_stats()
            hits, misses = after["hits"] - before["hits"], after["misses"] - before["misses"]
            hit_rates[label].append(hits / max(1, hits + misses))
        sp.close()
    got_cold, got_warm = walked["cold walk"], walked["warm walk"]

    # timed separately, so the cache accounting above is not inside the timed region
    def setup_cold():
        return ObjectSpace(FileStorage(path, durability="none"))

    def timed_walk(sp):
        res = walk(sp)
        sp.close()
        return res

    samples, got = measure(timed_walk, ctx.repeat, 0, setup=setup_cold, expect=expected)
    result.add_timing("cold walk (new storage instance)", samples, per=len(rows))
    sp = ObjectSpace(FileStorage(path, durability="none"))
    walk(sp)
    samples, got_w = measure(lambda: walk(sp), ctx.repeat, 0, expect=expected)
    result.add_timing("warm walk (new transaction, warm page cache)", samples, per=len(rows))

    def lookups():
        tr = sp.open_database("bench").new_transaction()
        c = tr.get_root_object("rows")
        s = sum(c.get_at(k).id for k in keys)
        tr.abort()
        return s

    before = sp.storage.cache_stats()
    samples, s = measure(lookups, ctx.repeat, ctx.warmup, expect=sum(keys))
    after = sp.storage.cache_stats()
    sp.close()
    result.add_timing("random get_at, new transaction per batch", samples, per=len(keys))
    result.check("lookups id sum", s == sum(keys), f"{s} != {sum(keys)}")
    hits, misses = after["hits"] - before["hits"], after["misses"] - before["misses"]
    result.metrics["page cache hit rate"] = {k: round(sum(v) / len(v), 4) for k, v in hit_rates.items()}
    result.metrics["page cache hit rate, random lookups"] = round(hits / max(1, hits + misses), 4)
    lookups_done = len(keys) * (ctx.repeat + ctx.warmup)
    result.metrics["page cache reads per lookup"] = round((hits + misses) / max(1, lookups_done), 2)
    for name, value in (("cold walk", got_cold), ("warm walk", got_warm), ("timed cold walk", got),
                        ("timed warm walk", got_w)):
        result.check(f"{name} count and id sum", value == expected, f"{value} != {expected}")
    result.checksum = got

    # -- tiered recovery -------------------------------------------------------------------
    from benchmarks.workloads.cluster import InProcCluster
    from proto_db import TieredStorage
    store = MemoryObjectStore()
    root = ctx.tempdir("tiered-recovery")
    cl = InProcCluster(root, 1, store, durability="fsync")
    try:
        for _ in range(p["commits"]):
            cl.dbs[0].run(lambda t: t.set_root_object("n", (t.get_root_object("n") or 0) + 1))
        cl.spaces[0].sync(timeout=120)
    finally:
        cl.close()

    def reopen():
        return TieredStorage(os.path.join(root, "n0"), store)

    def serves_counter(storage):
        """Outside the timed region: the reopened tier serves the committed counter."""
        reader = ObjectSpace(storage)          # closes the storage with it
        try:
            tr = reader.open_database("db").new_transaction()
            value = tr.get_root_object("n")
            tr.abort()
        finally:
            reader.close()
        return value == p["commits"] or f"counter {value} != {p['commits']}"

    samples, _ = measure(reopen, ctx.repeat, 0, check=serves_counter)
    result.add_timing(f"TieredStorage reopen (log + segment scan), {p['commits']} commits", samples)
    n = p["commits"]
    result.check("reopened local tier serves the committed counter", n == p["commits"], f"{n} != {p['commits']}")
    return result
