"""Point lookups, range queries and top-k: indexed vs scan vs Python dict/list vs sqlite3.

Every variant answers the same questions over the same :func:`~benchmarks.support.records`
data set and is checked against a model computed from the plain record list: the sum of
the matching ids for lookups, the count and id sum per range, the exact ordered id list
for top-k.  ProtoDB data lives in a committed :class:`~proto_db.Dictionary` (key ``id``)
read through a fresh transaction; ``code`` carries an :class:`~proto_db.EqualityIndex`,
``score`` (a unique permutation) a :class:`~proto_db.RangeIndex` and ``city`` an
EqualityIndex.  An unindexed copy of the dictionary gives the scan variants.  SQLite holds
the same rows in a file database (WAL) with a primary key on ``id`` and indexes on
``code``, ``score`` and ``city`` (and an unindexed copy for its scans); every query reads
the rows it returns.
"""
import atexit
import bisect
import heapq
import os
import random

from benchmarks.harness import BenchResult, benchmark, measure
from benchmarks.support import records, sqlite_connect

_DATA = {}


class _Data:
    """One committed ProtoDB data set and its SQLite and Python twins (built once per size)."""

    def __init__(self, n: int, workdir: str):
        from proto_db import EqualityIndex, MemoryStorage, ObjectSpace, RangeIndex
        self.n = n
        self.rows = records(n)
        self.space = ObjectSpace(MemoryStorage())
        db = self.space.new_database("bench")
        with db.new_transaction() as tr:
            plain = tr.new_dictionary()
            for r in self.rows:
                plain = plain.set_at(r["id"], tr.new_object(**r))
            indexed = plain.add_index(EqualityIndex("by_code", "code")) \
                .add_index(RangeIndex("by_score", "score")).add_index(EqualityIndex("by_city", "city"))
            tr.set_root_object("plain", plain)
            tr.set_root_object("indexed", indexed)
        self.tr = db.new_transaction()
        self.plain = self.tr.get_root_object("plain")
        self.indexed = self.tr.get_root_object("indexed")
        path = os.path.join(workdir, f"queries-{n}-{os.getpid()}.sqlite")
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(path + suffix):
                os.remove(path + suffix)
        self.sqlite_path = path
        self.sql = sqlite_connect(path, "none")
        self.sql.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, code TEXT, city TEXT, score INTEGER)")
        self.sql.execute("CREATE TABLE s (id INTEGER, code TEXT, city TEXT, score INTEGER)")
        self.sql.execute("BEGIN")
        rows = [(r["id"], r["code"], r["city"], r["score"]) for r in self.rows]
        self.sql.executemany("INSERT INTO r VALUES (?,?,?,?)", rows)
        self.sql.executemany("INSERT INTO s VALUES (?,?,?,?)", rows)
        self.sql.execute("COMMIT")
        self.sql.execute("CREATE INDEX r_code ON r(code)")
        self.sql.execute("CREATE INDEX r_score ON r(score)")
        self.sql.execute("CREATE INDEX r_city ON r(city)")
        self.sql.execute("ANALYZE")

    def close(self) -> None:
        self.tr.abort()
        self.space.close()
        self.sql.close()
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(self.sqlite_path + suffix):
                os.remove(self.sqlite_path + suffix)


def _data(ctx, n: int) -> _Data:
    """The shared data set of size ``n`` (built on first use, kept for the other query workloads)."""
    data = _DATA.get(n)
    if data is None:
        for old in list(_DATA.values()):
            old.close()
        _DATA.clear()
        data = _DATA[n] = _Data(n, ctx.workdir)
    return data


@atexit.register
def _close_all() -> None:
    """Close the shared data sets (their SQLite files live in the work directory)."""
    for data in list(_DATA.values()):
        data.close()
    _DATA.clear()


def _plan_root(queryable) -> str:
    return queryable.explain()["plan"]


@benchmark("point_lookup", quick={"n": 300, "lookups": 50, "scan_lookups": 3},
           full={"n": 20_000, "lookups": 2_000, "scan_lookups": 10})
def point_lookup(ctx, p):
    """Point lookups: primary key, secondary EqualityIndex, unindexed scan; dict/list and sqlite3."""
    from proto_db import F, from_collection
    d = _data(ctx, p["n"])
    rng = random.Random(11)
    ids = [rng.randrange(d.n) for _ in range(p["lookups"])]
    codes = [d.rows[i]["code"] for i in ids]
    scan_ids = ids[:p["scan_lookups"]]
    scan_codes = codes[:p["scan_lookups"]]
    expected, expected_scan = sum(ids), sum(scan_ids)
    by_id = {r["id"]: r for r in d.rows}
    by_code = {r["code"]: r for r in d.rows}
    as_list = list(d.rows)
    plan = _plan_root(from_collection(d.indexed).where(F.code == codes[0]))
    result = BenchResult({"rows": d.n, "lookups": len(ids), "scan_lookups": len(scan_ids),
                          "indexed_plan": plan}, None, expected)

    def pk():
        return sum(d.indexed.get_at(i).id for i in ids)

    def eq_index():
        return sum(rec.id for c in codes for rec in from_collection(d.indexed).where(F.code == c).to_list())

    def scan():
        return sum(rec.id for c in scan_codes for rec in from_collection(d.plain).where(F.code == c).to_list())

    def py_dict():
        return sum(by_id[i]["id"] for i in ids)

    def py_dict_secondary():
        return sum(by_code[c]["id"] for c in codes)

    def py_list_scan():
        return sum(r["id"] for c in scan_codes for r in as_list if r["code"] == c)

    def sql_pk():
        cur = d.sql.cursor()
        return sum(cur.execute("SELECT id, code, city, score FROM r WHERE id=?", (i,)).fetchone()[0] for i in ids)

    def sql_index():
        cur = d.sql.cursor()
        return sum(row[0] for c in codes for row in
                   cur.execute("SELECT id, code, city, score FROM r WHERE code=?", (c,)).fetchall())

    def sql_scan():
        cur = d.sql.cursor()
        return sum(row[0] for c in scan_codes for row in
                   cur.execute("SELECT id, code, city, score FROM s WHERE code=?", (c,)).fetchall())

    variants = [("protodb get_at(id)", pk, len(ids), expected),
                ("protodb EqualityIndex(code)", eq_index, len(ids), expected),
                ("protodb scan(code), no index", scan, len(scan_ids), expected_scan),
                ("python dict[id]", py_dict, len(ids), expected),
                ("python dict as code index", py_dict_secondary, len(ids), expected),
                ("python list scan(code)", py_list_scan, len(scan_ids), expected_scan),
                ("sqlite3 primary key", sql_pk, len(ids), expected),
                ("sqlite3 index(code)", sql_index, len(ids), expected),
                ("sqlite3 scan(code), no index", sql_scan, len(scan_ids), expected_scan)]
    for label, fn, per, want in variants:
        samples, got = measure(fn, ctx.repeat, ctx.warmup)
        result.check(f"{label} id sum", got == want, f"{got} != {want}")
        result.add_timing(label, samples, per=per)
    result.check("indexed plan uses the index", plan == "IndexLookup", plan)
    result.checksum = pk()
    return result


@benchmark("range_query", quick={"n": 300, "queries": 5, "scan_queries": 2, "width": 0.05},
           full={"n": 20_000, "queries": 50, "scan_queries": 5, "width": 0.01})
def range_query(ctx, p):
    """Range queries (1 % selectivity): RangeIndex vs scan vs sorted list/bisect vs sqlite3."""
    from proto_db import F, from_collection
    d = _data(ctx, p["n"])
    rng = random.Random(12)
    width = max(1, int(d.n * p["width"]))
    lows = [rng.randrange(d.n - width) for _ in range(p["queries"])]
    scan_lows = lows[:p["scan_queries"]]
    by_score = sorted(d.rows, key=lambda r: r["score"])
    keys = [r["score"] for r in by_score]

    def model(lo_list):
        rows = [r for lo in lo_list for r in by_score[lo:lo + width]]
        return len(rows), sum(r["id"] for r in rows)

    expected, expected_scan = model(lows), model(scan_lows)
    plan = _plan_root(from_collection(d.indexed).where(F.score.between(lows[0], lows[0] + width - 1)))
    result = BenchResult({"rows": d.n, "queries": len(lows), "scan_queries": len(scan_lows),
                          "rows_per_query": width, "indexed_plan": plan}, None, expected)

    def proto(coll, lo_list):
        def run():
            got = [rec.id for lo in lo_list
                   for rec in from_collection(coll).where(F.score.between(lo, lo + width - 1)).to_list()]
            return len(got), sum(got)
        return run

    def py_scan():
        got = [r["id"] for lo in scan_lows for r in d.rows if lo <= r["score"] <= lo + width - 1]
        return len(got), sum(got)

    def py_bisect():
        got = []
        for lo in lows:
            a, b = bisect.bisect_left(keys, lo), bisect.bisect_right(keys, lo + width - 1)
            got.extend(r["id"] for r in by_score[a:b])
        return len(got), sum(got)

    def sql(table, lo_list):
        def run():
            cur = d.sql.cursor()
            got = [row[0] for lo in lo_list for row in cur.execute(
                f"SELECT id, code, city, score FROM {table} WHERE score BETWEEN ? AND ?",
                (lo, lo + width - 1)).fetchall()]
            return len(got), sum(got)
        return run

    variants = [("protodb RangeIndex(score)", proto(d.indexed, lows), len(lows), expected),
                ("protodb scan, no index", proto(d.plain, scan_lows), len(scan_lows), expected_scan),
                ("python list scan", py_scan, len(scan_lows), expected_scan),
                ("python sorted list + bisect", py_bisect, len(lows), expected),
                ("sqlite3 index(score)", sql("r", lows), len(lows), expected),
                ("sqlite3 scan, no index", sql("s", scan_lows), len(scan_lows), expected_scan)]
    for label, fn, per, want in variants:
        samples, got = measure(fn, ctx.repeat, ctx.warmup)
        result.check(f"{label} count and id sum", got == want, f"{got} != {want}")
        result.add_timing(label, samples, per=per)
    result.check("indexed plan uses the range index", plan == "IndexRange", plan)
    result.checksum = proto(d.indexed, lows)()
    return result


@benchmark("topk", quick={"n": 300, "queries": 3, "k": 5},
           full={"n": 20_000, "queries": 20, "k": 10})
def topk(ctx, p):
    """Top-k by an indexed key (ruling R28 carry item: no early termination yet) vs heapq and sqlite3."""
    from proto_db import F, from_collection
    d = _data(ctx, p["n"])
    k = p["k"]
    rng = random.Random(13)
    lows = [rng.randrange(d.n // 2) for _ in range(p["queries"])]
    cities = [f"city{rng.randrange(100):03d}" for _ in range(p["queries"])]
    by_score = sorted(d.rows, key=lambda r: r["score"])
    exp_wide = [[r["id"] for r in by_score if r["score"] >= lo][:k] for lo in lows]
    exp_city = [[r["id"] for r in by_score if r["city"] == c][:k] for c in cities]
    wide_q = from_collection(d.indexed).where(F.score >= lows[0]).order_by(F.score).take(k)
    city_q = from_collection(d.indexed).where(F.city == cities[0]).order_by(F.score).take(k)

    def nodes(plan):
        out = [plan["plan"]]
        for child in plan.get("children", ()):
            out += nodes(child)
        return out

    result = BenchResult({"rows": d.n, "queries": len(lows), "k": k,
                          "plan_wide": " <- ".join(nodes(wide_q.explain())),
                          "plan_selective": " <- ".join(nodes(city_q.explain()))}, None, exp_wide)

    def proto_wide():
        return [[rec.id for rec in from_collection(d.indexed).where(F.score >= lo).order_by(F.score).take(k)]
                for lo in lows]

    def proto_city():
        return [[rec.id for rec in from_collection(d.indexed).where(F.city == c).order_by(F.score).take(k)]
                for c in cities]

    def heap_wide():
        return [[r["id"] for r in heapq.nsmallest(k, (r for r in d.rows if r["score"] >= lo),
                                                 key=lambda r: r["score"])] for lo in lows]

    def heap_city():
        return [[r["id"] for r in heapq.nsmallest(k, (r for r in d.rows if r["city"] == c),
                                                 key=lambda r: r["score"])] for c in cities]

    def sql_wide():
        cur = d.sql.cursor()
        return [[row[0] for row in cur.execute(
            "SELECT id, code, city, score FROM r WHERE score >= ? ORDER BY score LIMIT ?", (lo, k))]
            for lo in lows]

    def sql_city():
        cur = d.sql.cursor()
        return [[row[0] for row in cur.execute(
            "SELECT id, code, city, score FROM r WHERE city = ? ORDER BY score LIMIT ?", (c, k))]
            for c in cities]

    variants = [("protodb score>=x order_by(score) take k", proto_wide, exp_wide),
                ("python heapq.nsmallest (score>=x)", heap_wide, exp_wide),
                ("sqlite3 score>=x ORDER BY LIMIT", sql_wide, exp_wide),
                ("protodb city==c order_by(score) take k", proto_city, exp_city),
                ("python heapq.nsmallest (city==c)", heap_city, exp_city),
                ("sqlite3 city=c ORDER BY LIMIT", sql_city, exp_city)]
    for label, fn, want in variants:
        samples, got = measure(fn, ctx.repeat, ctx.warmup)
        result.check(f"{label} ordered ids", got == want, "result differs from the model")
        result.add_timing(label, samples, per=len(lows))
    result.notes.append("R28: the ordered index scan feeds a Filter and a Limit; the measured cost shows "
                        "whether the scan stops after k rows (early termination) or not")
    result.checksum = proto_wide()
    return result
