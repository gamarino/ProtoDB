"""Write paths: bulk insert, index update cost, commit throughput per durability mode,
persistent update cost (versions) and the canonical() re-walk.

Durability is matched explicitly when SQLite is the reference (rulings R37, R45): a
ProtoDB :class:`~proto_db.FileStorage` in ``fsync`` / ``flush`` / ``none`` mode is
compared with SQLite (WAL journal) at ``synchronous=FULL`` / ``NORMAL`` / ``OFF``
(:data:`benchmarks.support.SQLITE_SYNC`).  Every variant's final state is read back and
checked (counts and id sums), the ProtoDB ones through a newly opened space.
"""
import os
import random

from benchmarks.harness import BenchResult, benchmark, measure
from benchmarks.support import SQLITE_SYNC, records, sqlite_connect


def _sum_ids(coll) -> tuple:
    ids = [rec.id for rec in coll.elements()]
    return len(ids), sum(ids)


@benchmark("bulk_insert", quick={"n": 300, "repeat": 2}, full={"n": 10_000, "repeat": 3})
def bulk_insert(ctx, p):
    """Bulk insert of n records in one transaction (fsync), with and without indexes; dict and sqlite3."""
    from proto_db import EqualityIndex, FileStorage, ObjectSpace, RangeIndex
    rows = records(p["n"])
    expected = (len(rows), sum(r["id"] for r in rows))
    repeat = min(ctx.repeat, p["repeat"])
    result = BenchResult({"rows": len(rows), "durability": "fsync / synchronous=FULL", "transactions": 1},
                         None, expected)

    def proto(indexed):
        def setup():
            return ctx.tempdir("bulk")

        def run(path):
            space = ObjectSpace(FileStorage(path, durability="fsync"))
            db = space.new_database("bench")
            with db.new_transaction() as tr:
                coll = tr.new_dictionary()
                if indexed:
                    coll = coll.add_index(EqualityIndex("by_code", "code")).add_index(RangeIndex("by_score", "score"))
                for r in rows:
                    coll = coll.set_at(r["id"], tr.new_object(**r))
                tr.set_root_object("rows", coll)
            space.close()
            return path
        return setup, run

    def read_back(path, indexed):
        space = ObjectSpace(FileStorage(path, durability="none"))
        try:
            tr = space.open_database("bench").new_transaction()
            coll = tr.get_root_object("rows")
            got = _sum_ids(coll)
            if indexed:
                elements = list(coll.elements())
                coll.get_index("by_code").validate(elements)
                coll.get_index("by_score").validate(elements)
            tr.abort()
            return got
        finally:
            space.close()

    for label, indexed in (("protodb Dictionary, no index", False),
                           ("protodb Dictionary + EqualityIndex + RangeIndex", True)):
        setup, run = proto(indexed)
        samples, path = measure(run, repeat, ctx.warmup, setup=setup)
        got = read_back(path, indexed)
        result.check(f"{label} read back", got == expected, f"{got} != {expected}")
        result.add_timing(label, samples, per=len(rows))
        if not indexed:
            result.checksum = got

    def py_dict():
        d = {}
        for r in rows:
            d[r["id"]] = dict(r)
        return len(d), sum(d)

    samples, got = measure(py_dict, repeat, ctx.warmup)
    result.check("python dict", got == expected, f"{got} != {expected}")
    result.add_timing("python dict (memory only)", samples, per=len(rows))

    def sql(indexed):
        def setup():
            return os.path.join(ctx.tempdir("bulk-sql"), "db.sqlite")

        def run(path):
            conn = sqlite_connect(path, "fsync")
            conn.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, code TEXT, city TEXT, score INTEGER)")
            if indexed:
                conn.execute("CREATE INDEX r_code ON r(code)")
                conn.execute("CREATE INDEX r_score ON r(score)")
            conn.execute("BEGIN")
            conn.executemany("INSERT INTO r VALUES (?,?,?,?)",
                             [(r["id"], r["code"], r["city"], r["score"]) for r in rows])
            conn.execute("COMMIT")
            conn.close()
            return path
        return setup, run

    for label, indexed in (("sqlite3 table, primary key only", False), ("sqlite3 + 2 indexes", True)):
        setup, run = sql(indexed)
        samples, path = measure(run, repeat, ctx.warmup, setup=setup)
        conn = sqlite_connect(path, "none")
        got = tuple(conn.execute("SELECT COUNT(*), SUM(id) FROM r").fetchone())
        conn.close()
        result.check(f"{label} read back", got == expected, f"{got} != {expected}")
        result.add_timing(label, samples, per=len(rows))
    return result


@benchmark("index_update_cost", quick={"n": 300, "updates": 50}, full={"n": 10_000, "updates": 2_000})
def index_update_cost(ctx, p):
    """Marginal cost of each index on an update (carry item: ~80-95 us per index update)."""
    from proto_db import EqualityIndex, F, MemoryStorage, ObjectSpace, RangeIndex, from_collection
    rows = records(p["n"])
    rng = random.Random(21)
    keys = [rng.randrange(len(rows)) for _ in range(p["updates"])]
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("bench")
    with db.new_transaction() as tr:
        base = tr.new_dictionary()
        for r in rows:
            base = base.set_at(r["id"], tr.new_object(**r))
        tr.set_root_object("rows", base)
    tr = db.new_transaction()
    base = tr.get_root_object("rows")
    configs = {0: base,
               1: base.add_index(RangeIndex("by_score", "score")),
               2: base.add_index(RangeIndex("by_score", "score")).add_index(EqualityIndex("by_code", "code")),
               3: base.add_index(RangeIndex("by_score", "score")).add_index(EqualityIndex("by_code", "code"))
                      .add_index(EqualityIndex("by_city", "city"))}
    last = {}
    for k in keys:
        last[k] = k
    expected = sorted(last)
    result = BenchResult({"rows": len(rows), "updates_per_sample": len(keys), "index_counts": sorted(configs)},
                         None, expected)
    medians = {}
    new_objects = [tr.new_object(id=k, code=f"new-{k}", city="moved", score=-1 - i) for i, k in enumerate(keys)]
    for count, coll in configs.items():
        def run(coll=coll):
            c = coll
            for k, obj in zip(keys, new_objects):
                c = c.set_at(k, obj)
            return c
        samples, final = measure(run, ctx.repeat, ctx.warmup)
        moved = sorted(rec.id for rec in final.elements() if rec.city == "moved")
        result.check(f"{count} index(es): updated keys", moved == expected, "updated set differs")
        result.check(f"{count} index(es): count unchanged", final.count == len(rows), str(final.count))
        if count >= 1:
            via_index = sorted(rec.id for rec in from_collection(final).where(F.score < 0).to_list())
            result.check(f"{count} index(es): RangeIndex sees the updates", via_index == expected,
                         "index result differs")
            final.get_index("by_score").validate(list(final.elements()))
        entry = result.add_timing(f"set_at (replace) with {count} index(es)", samples, per=len(keys))
        medians[count] = entry["per_op_us"]
        if count == 0:
            result.checksum = moved
    result.metrics["marginal_us_per_index"] = {f"{c - 1}->{c}": round(medians[c] - medians[c - 1], 1)
                                               for c in sorted(medians) if c > 0}
    tr.abort()
    space.close()
    return result


@benchmark("commit_durability", quick={"commits": {"fsync": 10, "flush": 10, "none": 20}},
           full={"commits": {"fsync": 300, "flush": 300, "none": 2_000}})
def commit_durability(ctx, p):
    """Small read-modify-write commits per durability mode vs sqlite3 at the matching synchronous level."""
    from proto_db import FileStorage, ObjectSpace
    result = BenchResult({"commit": "read counter n, n+1, set_at(i, i) on a Dictionary root",
                          "sqlite": "BEGIN; UPDATE n=n+1; INSERT (i, i); COMMIT (WAL)",
                          "commits_per_sample": p["commits"]}, None, None)
    checksum = {}
    for mode, k in p["commits"].items():
        def setup():
            path = ctx.tempdir(f"commit-{mode}")
            space = ObjectSpace(FileStorage(path, durability=mode))
            db = space.new_database("bench")
            db.run(lambda tr: (tr.set_root_object("n", 0), tr.set_root_object("log", tr.new_dictionary())))
            return path, space, db

        def run(state, k=k):
            path, space, db = state
            for i in range(k):
                def fn(tr, i=i):
                    tr.set_root_object("n", tr.get_root_object("n") + 1)
                    tr.set_root_object("log", tr.get_root_object("log").set_at(i, i))
                db.run(fn)
            space.close()
            return path

        samples, path = measure(run, ctx.repeat, ctx.warmup, setup=setup)
        space = ObjectSpace(FileStorage(path, durability="none"))
        tr = space.open_database("bench").new_transaction()
        got = (tr.get_root_object("n"), tr.get_root_object("log").count, sum(tr.get_root_object("log").keys()))
        tr.abort()
        space.close()
        want = (k, k, k * (k - 1) // 2)
        result.check(f"protodb {mode} final state", got == want, f"{got} != {want}")
        checksum[mode] = got
        result.add_timing(f"protodb FileStorage durability={mode}", samples, per=k)
        result.add_rate(f"protodb durability={mode} commits/s", [k / s for s in samples], "commits/s")

        def sql_setup():
            path = os.path.join(ctx.tempdir(f"commit-sql-{mode}"), "db.sqlite")
            conn = sqlite_connect(path, mode)
            conn.execute("CREATE TABLE c (n INTEGER)")
            conn.execute("INSERT INTO c VALUES (0)")
            conn.execute("CREATE TABLE log (i INTEGER PRIMARY KEY, v INTEGER)")
            return path, conn

        def sql_run(state, k=k):
            path, conn = state
            for i in range(k):
                conn.execute("BEGIN")
                conn.execute("UPDATE c SET n = n + 1")
                conn.execute("INSERT INTO log VALUES (?, ?)", (i, i))
                conn.execute("COMMIT")
            conn.close()
            return path

        samples, path = measure(sql_run, ctx.repeat, ctx.warmup, setup=sql_setup)
        conn = sqlite_connect(path, "none")
        got = (conn.execute("SELECT n FROM c").fetchone()[0],) + tuple(
            conn.execute("SELECT COUNT(*), SUM(i) FROM log").fetchone())
        conn.close()
        result.check(f"sqlite3 synchronous={SQLITE_SYNC[mode]} final state", got == want, f"{got} != {want}")
        result.add_timing(f"sqlite3 synchronous={SQLITE_SYNC[mode]}", samples, per=k)
        result.add_rate(f"sqlite3 synchronous={SQLITE_SYNC[mode]} commits/s", [k / s for s in samples], "commits/s")
    result.checksum = result.expected = checksum
    result.notes.append("flush keeps the root write's fsyncs (D19) while SQLite NORMAL in WAL mode syncs "
                        "nothing at commit: ProtoDB's flush is the stronger of the two")
    return result


@benchmark("versions", quick={"sizes": [100, 1_000], "updates": 50, "copy_updates": 5, "commits": 5},
           full={"sizes": [1_000, 10_000, 100_000], "updates": 2_000, "copy_updates": 50, "commits": 200})
def versions(ctx, p):
    """Persistent update cost: one new version per update (path copy) vs a full dict copy, by size."""
    from proto_db import FileStorage, MemoryStorage, ObjectSpace
    result = BenchResult({"sizes": p["sizes"], "updates_per_sample": p["updates"],
                          "dict_copy_updates_per_sample": p["copy_updates"], "committed_updates": p["commits"]},
                         None, None)
    checksum, expected = {}, {}
    result.expected = expected
    rng = random.Random(31)
    for n in p["sizes"]:
        space = ObjectSpace(MemoryStorage())
        db = space.new_database("bench")
        with db.new_transaction() as tr:
            d = tr.new_dictionary()
            for i in range(n):
                d = d.set_at(i, i)
            tr.set_root_object("d", d)
        tr = db.new_transaction()
        v0 = tr.get_root_object("d")
        keys = [rng.randrange(n) for _ in range(p["updates"])]

        def chain():
            versions_ = [v0]
            for j, k in enumerate(keys):
                versions_.append(versions_[-1].set_at(k, -1 - j))
            return versions_

        samples, chain_ = measure(chain, ctx.repeat, ctx.warmup)
        old_intact = all(v0.get_at(k) == k for k in keys)
        last = {k: -1 - j for j, k in enumerate(keys)}
        new_ok = all(chain_[-1].get_at(k) == v for k, v in last.items())
        each_ok = all(chain_[j + 1].get_at(k) == -1 - j for j, k in enumerate(keys))
        result.check(f"n={n}: every old version intact", old_intact, "v0 changed")
        result.check(f"n={n}: every new version holds its update", new_ok and each_ok, "update missing")
        result.add_timing(f"protodb set_at -> new version, n={n}", samples, per=len(keys))
        checksum[n] = sum(chain_[-1].get_at(k) for k in last)
        expected[n] = sum(last.values())
        tr.abort()
        space.close()

        plain = {i: i for i in range(n)}
        ckeys = keys[:p["copy_updates"]]

        def copies():
            vs = [plain]
            for j, k in enumerate(ckeys):
                nxt = dict(vs[-1])
                nxt[k] = -1 - j
                vs.append(nxt)
            return vs

        samples, vs = measure(copies, ctx.repeat, ctx.warmup)
        result.check(f"n={n}: dict copies keep old versions", all(plain[k] == k for k in ckeys), "changed")
        result.add_timing(f"python dict copy + update, n={n}", samples, per=len(ckeys))

        path = ctx.tempdir("versions")
        space = ObjectSpace(FileStorage(path, durability="none"))
        db = space.new_database("bench")
        with db.new_transaction() as tr2:
            d = tr2.new_dictionary()
            for i in range(n):
                d = d.set_at(i, i)
            tr2.set_root_object("d", d)
        ckeys2 = [rng.randrange(n) for _ in range(p["commits"])]

        def commits():
            for j, k in enumerate(ckeys2):
                db.run(lambda t, k=k, j=j: t.set_root_object("d", t.get_root_object("d").set_at(k, -1 - j)))

        samples, _ = measure(commits, 1, 0)
        hist = db.history()
        result.check(f"n={n}: history holds every committed version",
                     len(hist) == len(ckeys2) + 2, f"{len(hist)} != {len(ckeys2) + 2}")
        result.add_timing(f"protodb committed update (durability=none), n={n}", samples, per=len(ckeys2))
        space.close()
    result.checksum = checksum
    return result


@benchmark("canonical_rewalk", quick={"n": 2_000}, full={"n": 220_000})
def canonical_rewalk(ctx, p):
    """canonical() on a list vs on an already frozen list (carry item: the frozen input is re-walked)."""
    from proto_db import FrozenList, canonical
    data = list(range(p["n"]))
    result = BenchResult({"elements": p["n"]}, None, (p["n"], sum(data)))
    samples, frozen = measure(lambda: canonical(data), ctx.repeat, ctx.warmup)
    result.check("list becomes a FrozenList", type(frozen) is FrozenList and list(frozen) == data, "")
    result.add_timing("canonical(list) -> FrozenList", samples, per=p["n"])
    samples, again = measure(lambda: canonical(frozen), ctx.repeat, ctx.warmup)
    result.check("frozen input returned as the same object", again is frozen, "a copy was made")
    result.add_timing("canonical(FrozenList) re-walk", samples, per=p["n"])
    result.checksum = (len(again), sum(again))
    return result
