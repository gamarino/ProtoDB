"""Concurrent commit throughput and conflict rate, concurrent read scaling, parallel scan.

Thread counts come from ``--threads`` (default 1, 2, 4, 8, 16).  Run the suite once on the
free-threaded build with the GIL off and once with ``-X gil=1`` to compare (spec section 12).

Commit variants (``FileStorage`` with ``durability="none"``, so the figure is the engine's
CPU and lock cost, not the disk):

* ``conflicting``: every commit reads and increments one counter root, so concurrent
  commits conflict and ``Database.run`` retries them; attempts per commit are counted;
* ``blind merge``: every commit adds a unique id to a ``CountedSet`` root written without
  being read: never a conflict, the op log is replayed onto the current value under the
  root lock (D9, D33);
* ``blind merge, indexed dict``: the same onto a 1000-entry ``Dictionary`` carrying a
  RangeIndex (carry item "blind merges queue on the root lock", ~8 ms per commit when it
  was noted): the replay maintains the index inside the critical section;
* ``disjoint roots``: every thread increments its own counter root (no conflict; commits
  still serialise on the root lock).

Every variant checks the final state exactly: the counter equals the commits, every id is
present once, the index validates against a full scan.
"""
import itertools
import math
import random

from benchmarks.harness import BenchResult, benchmark, measure
from benchmarks.support import run_threads

RUN_KW = dict(retries=100_000, backoff=(0.0002, 0.005))


@benchmark("concurrent_commits", quick={"commits": 8, "base_entries": 50},
           full={"commits": 240, "base_entries": 1_000})
def concurrent_commits(ctx, p):
    """Commit throughput and conflict rate with 1..N threads (conflicting, blind merge, disjoint)."""
    from proto_db import FileStorage, ObjectSpace, RangeIndex
    total_target = p["commits"]
    result = BenchResult({"threads": ctx.threads, "commits_per_run": total_target,
                          "storage": "FileStorage durability=none", "indexed_base_entries": p["base_entries"]},
                         None, None)
    checksum, expected = {}, {}

    def setup_space():
        space = ObjectSpace(FileStorage(ctx.tempdir("concurrent"), durability="none"))
        db = space.new_database("bench")
        with db.new_transaction() as tr:
            tr.set_root_object("n", 0)
            base = tr.new_dictionary().add_index(RangeIndex("by_score", "score"))
            for i in range(p["base_entries"]):
                base = base.set_at(-1 - i, tr.new_object(id=-1 - i, score=-1 - i))
            tr.set_root_object("people", base)
            for t in range(max(ctx.threads)):
                tr.set_root_object(f"n{t}", 0)
        return space, db

    for variant in ("conflicting", "blind merge", "blind merge, indexed dict", "disjoint roots"):
        for threads in ctx.threads:
            per_thread = max(1, math.ceil(total_target / threads))
            total = per_thread * threads
            attempts = [0] * threads
            spaces = []

            def setup():
                space, db = setup_space()
                spaces.append(space)
                for i in range(threads):
                    attempts[i] = 0
                return db

            def run(db):
                uids = itertools.count()

                def worker_for(t):
                    def worker():
                        for _ in range(per_thread):
                            uid = next(uids)

                            def fn(tr, uid=uid):
                                attempts[t] += 1
                                if variant == "conflicting":
                                    tr.set_root_object("n", tr.get_root_object("n") + 1)
                                elif variant == "blind merge":
                                    tr.set_root_object("ids", tr.new_counted_set().add(uid))
                                elif variant == "blind merge, indexed dict":
                                    tr.set_root_object("people", tr.new_dictionary().set_at(
                                        uid, tr.new_object(id=uid, score=uid)))
                                else:
                                    tr.set_root_object(f"n{t}", tr.get_root_object(f"n{t}") + 1)
                            db.run(fn, **RUN_KW)
                    return worker
                run_threads([worker_for(t) for t in range(threads)])
                return db

            def final_state(db, variant=variant, total=total, threads=threads):
                """``(got, want)`` for the state a run left (read by a new transaction)."""
                tr = db.new_transaction()
                try:
                    if variant == "conflicting":
                        return tr.get_root_object("n"), total
                    if variant == "blind merge":
                        ids = tr.get_root_object("ids")
                        return ((sorted(ids.elements()) == list(range(total)),
                                 all(c == 1 for _, c in ids.items())), (True, True))
                    if variant == "blind merge, indexed dict":
                        people = tr.get_root_object("people")
                        elements = list(people.elements())
                        people.get_index("by_score").validate(elements)
                        return ((people.count, sorted(e.id for e in elements if e.id >= 0) == list(range(total))),
                                (p["base_entries"] + total, True))
                    return sum(tr.get_root_object(f"n{t}") for t in range(threads)), total
                finally:
                    tr.abort()

            def state_ok(db):
                got, want = final_state(db)
                return got == want or f"final state {got} != {want}"

            samples, db = measure(run, ctx.repeat, ctx.warmup, setup=setup, check=state_ok)
            got, want = final_state(db)
            for space in spaces:
                space.close()
            key = f"{variant}, {threads} thread(s)"
            result.check(f"{key}: final state", got == want, f"{got} != {want}")
            checksum[key], expected[key] = got, want
            result.add_rate(f"{variant}: commits/s, {threads} thread(s)", [total / s for s in samples], "commits/s")
            result.metrics[f"{variant}: attempts per commit, {threads} thread(s)"] = round(sum(attempts) / total, 2)
    result.checksum, result.expected = checksum, expected
    return result


@benchmark("concurrent_reads", quick={"n": 300, "lookups": 100, "queries": 10},
           full={"n": 20_000, "lookups": 4_000, "queries": 200})
def concurrent_reads(ctx, p):
    """Read scaling with 1..N threads (own transaction per thread): get_at and index queries; dict baseline."""
    from proto_db import EqualityIndex, F, FileStorage, ObjectSpace, from_collection
    from benchmarks.support import records
    rows = records(p["n"])
    space = ObjectSpace(FileStorage(ctx.tempdir("reads"), durability="none"))
    db = space.new_database("bench")
    with db.new_transaction() as tr:
        coll = tr.new_dictionary().add_index(EqualityIndex("by_city", "city"))
        for r in rows:
            coll = coll.set_at(r["id"], tr.new_object(**r))
        tr.set_root_object("rows", coll)
    rng = random.Random(41)
    keys = [rng.randrange(len(rows)) for _ in range(p["lookups"])]
    cities = [f"city{rng.randrange(100):03d}" for _ in range(p["queries"])]
    want_keys = sum(keys)
    by_city = {}
    for r in rows:
        by_city.setdefault(r["city"], []).append(r["id"])
    want_cities = sum(sum(by_city.get(c, [])) for c in cities)
    plain = {r["id"]: r for r in rows}
    result = BenchResult({"rows": len(rows), "lookups_per_thread": len(keys), "queries_per_thread": len(cities),
                          "threads": ctx.threads}, None, None)
    checksum, expected = {}, {}
    for threads in ctx.threads:
        def lookups():
            tr = db.new_transaction()
            c = tr.get_root_object("rows")
            s = sum(c.get_at(k).id for k in keys)
            tr.abort()
            return s

        def queries():
            tr = db.new_transaction()
            c = tr.get_root_object("rows")
            s = sum(rec.id for city in cities for rec in from_collection(c).where(F.city == city).to_list())
            tr.abort()
            return s

        def dict_lookups():
            return sum(plain[k]["id"] for k in keys)

        for label, fn, want, ops in (("protodb get_at", lookups, want_keys, len(keys)),
                                     ("protodb EqualityIndex query", queries, want_cities, len(cities)),
                                     ("python dict lookup", dict_lookups, want_keys, len(keys))):
            def run(fn=fn):
                _, res = run_threads([fn] * threads)
                return res
            samples, res = measure(run, ctx.repeat, ctx.warmup,
                                   check=lambda res, want=want: all(r == want for r in res) or f"{res} != {want}")
            key = f"{label}, {threads} thread(s)"
            ok = all(r == want for r in res)
            result.check(f"{key}: every thread's sum", ok, f"{res} != {want}")
            checksum[key], expected[key] = ok, True
            result.add_rate(f"{label}: ops/s, {threads} thread(s)",
                            [threads * ops / s for s in samples], "ops/s")
    for label in ("protodb get_at", "protodb EqualityIndex query", "python dict lookup"):
        base = result.timings[f"{label}: ops/s, {ctx.threads[0]} thread(s)"]["median"]
        result.metrics[f"{label}: speedup vs {ctx.threads[0]} thread(s)"] = {
            t: round(result.timings[f"{label}: ops/s, {t} thread(s)"]["median"] / base, 2) for t in ctx.threads}
    result.checksum, result.expected = checksum, expected
    space.close()
    return result


def _spin(x: int, spin: int) -> int:
    h = x
    for i in range(spin):
        h = (h * 1_000_003 + i) & 0xFFFFFFFF
    return h


@benchmark("parallel_scan", quick={"items": 200, "spin": 200}, full={"items": 2_000, "spin": 4_000})
def parallel_scan(ctx, p):
    """ParallelPool.scan of a CPU-bound function with 1..N workers vs a sequential loop (D40)."""
    from functools import partial

    from proto_db import ParallelPool
    items = list(range(p["items"]))
    work = partial(_spin, spin=p["spin"])
    expected = [work(x) for x in items]
    result = BenchResult({"items": len(items), "spin": p["spin"], "workers": ctx.threads}, None, sum(expected))
    samples, seq = measure(lambda: [work(x) for x in items], ctx.repeat, ctx.warmup, expect=expected)
    result.check("sequential results", seq == expected, "")
    seq_median = result.add_timing("sequential loop", samples, per=len(items))["median"]
    speedups = {}
    for workers in ctx.threads:
        with ParallelPool(workers) as pool:
            samples, got = measure(lambda: pool.scan(items, work), ctx.repeat, ctx.warmup, expect=expected)
        result.check(f"{workers} worker(s): results equal the sequential run", got == expected, "")
        speedups[workers] = round(seq_median / result.add_timing(f"ParallelPool.scan, {workers} worker(s)", samples,
                                                                 per=len(items))["median"], 2)
    result.metrics["speedup vs sequential"] = speedups
    result.checksum = sum(seq)
    return result
