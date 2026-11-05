"""Query result cache and deep pages (Task 8.4b, D56; rulings R76, R77): ProtoDB vs sqlite3.

One data set per size N: rows ``{id, k, g}`` with ``id`` = 0..N-1, ``k`` a unique permutation of
0..N-1 (seeded) and ``g = id % 100``.  ProtoDB keeps them in a committed ``List`` with a
``RangeIndex`` on ``k``, in a :class:`~proto_db.FileStorage` in the work directory; SQLite keeps
the same rows in a file database (WAL) with an index on ``k``.  Both are built once per size
and reused by later runs (each checked for its row count and an id checksum before use).
Every ProtoDB request opens a new transaction (as a server request would); every SQLite
request runs on one open connection.  Every request returns full rows, and every result is
verified: the ordered ids of a page against the permutation's inverse, the row count and id
sum of a query against a Python model.

``deep_page``: page size 50 at row offsets 0, 10^4, 10^5 (and 9 x 10^5 at N = 10^6), ordered
by ``k``, with and without a range predicate (``k >= N/2``).
  * protodb pager.page (by rank): ``tx.pager(...).page(o // 50)``, O(log n + 50) (D56);
  * protodb islice (no rank): the ordered index scan read row by row up to the offset, the
    cost a plan without rank access pays;
  * sqlite3 OFFSET: ``ORDER BY k LIMIT 50 OFFSET o`` (index on ``k``);
  * sqlite3 keyset, next page: ``WHERE k > last ORDER BY k LIMIT 50`` with the previous
    page's last key known (no random jump);
  * sqlite3 keyset, jump to offset o: o/50 chained keyset fetches from the start.

``query_cache``: a range plus a residual filter (``lo <= k < lo + 2·10^4 and g < 50``, about
10^4 rows) and an ordered filtered scan (``g == 7`` ordered by ``k``, N/100 rows), each:
executed without the cache, through the cache on a miss (cache cleared before the request),
through the cache on a hit, and by sqlite3 (which has no result cache: every request runs).
"""
import atexit
import os
import random
import shutil
from itertools import islice

from benchmarks.harness import BenchResult, benchmark, measure
from benchmarks.support import sqlite_connect

PAGE = 50
#: Bumped when the on-disk form of the data sets changes, so old copies are rebuilt.
_DATA_VERSION = "8.4b-1"
_OPEN = {}


def _rows(n: int):
    rng = random.Random(84)
    perm = list(range(n))
    rng.shuffle(perm)
    return perm


class _Data:
    """The ProtoDB and SQLite copies of the data set of size ``n`` (built once, reused)."""

    def __init__(self, n: int, workdir: str):
        from proto_db import FileStorage, ObjectSpace, RangeIndex
        self.n = n
        self.perm = _rows(n)
        self.inverse = [0] * n
        for i, k in enumerate(self.perm):
            self.inverse[k] = i
        base = os.path.join(workdir, f"pagination-{_DATA_VERSION}-{n}")
        os.makedirs(base, exist_ok=True)
        marker = os.path.join(base, "complete")
        fresh = not os.path.exists(marker)
        self.path = os.path.join(base, "protodb")
        self.sqlite_path = os.path.join(base, "sqlite.db")
        if fresh:
            for name in os.listdir(base):
                full = os.path.join(base, name)
                if os.path.isdir(full):
                    shutil.rmtree(full)
                else:
                    os.remove(full)
        self.space = ObjectSpace(FileStorage(self.path))
        if fresh:
            db = self.space.new_database("bench")
            rows = [{"id": i, "k": self.perm[i], "g": i % 100} for i in range(n)]
            db.run(lambda tx: tx.set_root_object(
                "t", tx.new_list().extend(rows).add_index(RangeIndex("k", "k"))))
            del rows
            sql = sqlite_connect(self.sqlite_path, "none")
            sql.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, g INTEGER)")
            sql.execute("BEGIN")
            sql.executemany("INSERT INTO t VALUES (?,?,?)", ((i, self.perm[i], i % 100) for i in range(n)))
            sql.execute("COMMIT")
            sql.execute("CREATE INDEX t_k ON t(k)")
            sql.execute("ANALYZE")
            sql.close()
            with open(marker, "w") as f:
                f.write("ok\n")
        self.db = self.space.open_database("bench")
        self.sql = sqlite_connect(self.sqlite_path, "none")
        # Integrity of a reused copy: row counts and an id checksum on both sides.
        want = n * (n - 1) // 2
        with self.db.new_transaction() as tx:
            t = tx.get_root_object("t")
            index = t.get_index("k")
            if t.count != n or index.total_count != n or index.plan(_ge(0)).exact_rows() != n:
                raise RuntimeError(f"pagination data set at {base} is not the expected one; delete it")
        got = self.sql.execute("SELECT count(*), sum(id) FROM t").fetchone()
        if got != (n, want):
            raise RuntimeError(f"pagination SQLite data set at {base} is not the expected one; delete it")

    def page_ids(self, start: int, low: int = 0) -> list:
        """The expected ids of the page at row offset ``start`` of ``k >= low`` ordered by ``k``."""
        return [self.inverse[k] for k in range(low + start, min(low + start + PAGE, self.n))]

    def close(self) -> None:
        self.sql.close()
        self.space.close()


def _ge(v):
    from proto_db import Term
    return Term("k", ">=", v)


def _data(ctx, n: int) -> _Data:
    data = _OPEN.get(n)
    if data is None:
        data = _OPEN[n] = _Data(n, ctx.workdir)
    return data


@atexit.register
def _close_all() -> None:
    for data in list(_OPEN.values()):
        data.close()
    _OPEN.clear()


def _ids(rows) -> list:
    return [r["id"] for r in rows]


@benchmark("deep_page", quick={"n": 2_000, "offsets": [0, 500, 1_500], "requests": 5, "jump_max": 1_000},
           full={"n": 1_000_000, "offsets": [0, 10_000, 100_000, 900_000], "requests": 20,
                 "jump_max": 100_000})
def deep_page(ctx, p):
    """Page of 50 rows at deep offsets: ProtoDB pager by rank vs islice vs sqlite3 OFFSET and keyset."""
    from proto_db import plan_query
    d = _data(ctx, p["n"])
    n, requests = d.n, p["requests"]
    half = n // 2
    result = BenchResult({"rows": n, "page_size": PAGE, "offsets": p["offsets"], "requests_per_sample": requests},
                         None, None)
    with d.db.new_transaction() as tx:
        probe = tx.pager("t", order_by="k", page_size=PAGE)
        ranged = tx.pager("t", where=_ge(half), order_by="k", page_size=PAGE)
        result.check("pager over the ordered index is served by rank", probe.by_rank, probe.explain())
        result.check("pager over k >= N/2 ordered by k is served by rank", ranged.by_rank, ranged.explain())
        result.work["plan_ordered"] = _shape(probe.explain()["plan"])
        result.work["plan_ranged"] = _shape(ranged.explain()["plan"])
    expected, measured = [], []
    for offset in p["offsets"]:
        assert offset % PAGE == 0
        page_no = offset // PAGE
        want = d.page_ids(offset)
        want_ranged = d.page_ids(offset, half) if offset < n - half else []
        expect = [want] * requests
        expect_ranged = [want_ranged] * requests

        def pager_page(offset=offset, page_no=page_no):
            out = []
            for _ in range(requests):
                with d.db.new_transaction() as tx:
                    out.append(_ids(tx.pager("t", order_by="k", page_size=PAGE).page(page_no)))
            return out

        def pager_ranged(offset=offset, page_no=page_no):
            out = []
            for _ in range(requests):
                with d.db.new_transaction() as tx:
                    out.append(_ids(tx.pager("t", where=_ge(half), order_by="k", page_size=PAGE).page(page_no)))
            return out

        def proto_islice(offset=offset):
            out = []
            for _ in range(requests):
                with d.db.new_transaction() as tx:
                    plan = plan_query(tx.get_root_object("t"), order_by="k")
                    out.append(_ids(islice(plan.execute(), offset, offset + PAGE)))
            return out

        def sql_offset(offset=offset):
            cur = d.sql.cursor()
            return [[row[0] for row in cur.execute("SELECT id, k, g FROM t ORDER BY k LIMIT ? OFFSET ?",
                                                   (PAGE, offset))] for _ in range(requests)]

        def sql_offset_ranged(offset=offset):
            cur = d.sql.cursor()
            return [[row[0] for row in cur.execute(
                "SELECT id, k, g FROM t WHERE k >= ? ORDER BY k LIMIT ? OFFSET ?", (half, PAGE, offset))]
                for _ in range(requests)]

        def sql_keyset_next(offset=offset):
            cur = d.sql.cursor()
            last = offset - 1         # the previous page's last key, known to the client
            return [[row[0] for row in cur.execute("SELECT id, k, g FROM t WHERE k > ? ORDER BY k LIMIT ?",
                                                   (last, PAGE))] for _ in range(requests)]

        variants = [(f"protodb pager.page by rank @{offset}", pager_page, expect),
                    (f"protodb islice ordered scan (no rank) @{offset}", proto_islice, expect),
                    (f"sqlite3 OFFSET @{offset}", sql_offset, expect),
                    (f"sqlite3 keyset next page @{offset}", sql_keyset_next, expect)]
        if offset < n - half:
            variants += [(f"protodb pager k>=N/2 by rank @{offset}", pager_ranged, expect_ranged),
                         (f"sqlite3 k>=N/2 OFFSET @{offset}", sql_offset_ranged, expect_ranged)]
        if 0 < offset <= p["jump_max"]:
            def sql_keyset_jump(offset=offset):
                cur = d.sql.cursor()
                out = []
                for _ in range(max(1, requests // 10)):
                    last, rows = -1, []
                    for _ in range(offset // PAGE + 1):
                        rows = cur.execute("SELECT id, k, g FROM t WHERE k > ? ORDER BY k LIMIT ?",
                                           (last, PAGE)).fetchall()
                        last = rows[-1][1]
                    out.append([row[0] for row in rows])
                return out
            jump_requests = max(1, requests // 10)
            variants.append((f"sqlite3 keyset jump (chained) @{offset}", sql_keyset_jump, [want] * jump_requests))
        for label, fn, want_out in variants:
            samples, got = measure(fn, ctx.repeat, ctx.warmup, expect=want_out)
            result.check(f"{label} page ids", got == want_out, "page differs from the model")
            result.add_timing(label, samples, per=len(want_out))
        expected.append(want)
        measured.append(pager_page()[0])
    result.expected, result.checksum = expected, measured
    return result


@benchmark("query_cache", quick={"n": 2_000, "span": 400},
           full={"sizes": [100_000, 1_000_000], "span": 20_000})
def query_cache(ctx, p):
    """A repeated query: ProtoDB without cache, cache miss, cache hit vs sqlite3 re-execution."""
    from proto_db import F, from_collection
    sizes = p.get("sizes", [p.get("n")])
    result = BenchResult({"sizes": sizes, "span": p["span"]}, None, None)
    expected, measured = [], []
    for n in sizes:
        d = _data(ctx, n)
        span = min(p["span"], n // 5)
        lo = n // 3
        model_a = [i for i in range(n) if lo <= d.perm[i] < lo + span and i % 100 < 50]
        model_b = sorted((i for i in range(n) if i % 100 == 7), key=lambda i: d.perm[i])
        want_a = (len(model_a), sum(model_a))
        want_b = (len(model_b), sum(model_b), model_b[:5])

        def q_a(tx):
            return from_collection(tx.get_root_object("t")).where((F.k >= lo) & (F.k < lo + span) & (F.g < 50))

        def q_b(tx):
            return from_collection(tx.get_root_object("t")).where(F.g == 7).order_by(F.k)

        def summary_a(rows):
            ids = _ids(rows)
            return len(ids), sum(ids)

        def summary_b(rows):
            ids = _ids(rows)
            return len(ids), sum(ids), ids[:5]

        cache = d.space.query_cache
        with d.db.new_transaction() as tx:
            plan_a = q_a(tx).explain()
            plan_b = q_b(tx).explain()
            result.check(f"N={n}: query A is cacheable", cache.explain(q_a(tx))["cache"].get("key") is not None,
                         cache.explain(q_a(tx))["cache"])
        result.work[f"plan_A@{n}"] = _shape(plan_a)
        result.work[f"plan_B@{n}"] = _shape(plan_b)

        for name, build, summarize, want, sql_text, sql_args, sql_summary in (
                ("A range+filter", q_a, summary_a, want_a,
                 "SELECT id, k, g FROM t WHERE k >= ? AND k < ? AND g < 50", (lo, lo + span),
                 lambda rows: (len(rows), sum(r[0] for r in rows))),
                ("B filter ordered by k", q_b, summary_b, want_b,
                 "SELECT id, k, g FROM t WHERE g = 7 ORDER BY k", (),
                 lambda rows: (len(rows), sum(r[0] for r in rows), [r[0] for r in rows[:5]]))):

            def no_cache(build=build, summarize=summarize):
                with d.db.new_transaction() as tx:
                    return summarize(list(build(tx)))

            def miss(_cleared, build=build, summarize=summarize):
                with d.db.new_transaction() as tx:
                    return summarize(cache.result(build(tx)))

            def hit(build=build, summarize=summarize):
                with d.db.new_transaction() as tx:
                    return summarize(cache.result(build(tx)))

            def sql(sql_text=sql_text, sql_args=sql_args, sql_summary=sql_summary):
                return sql_summary(d.sql.execute(sql_text, sql_args).fetchall())

            hits_before = cache.stats()["hits"]
            for label, fn, setup in ((f"N={n} {name}: protodb no cache", no_cache, None),
                                     (f"N={n} {name}: protodb cache miss", miss, cache.clear),
                                     (f"N={n} {name}: protodb cache hit", hit, None),
                                     (f"N={n} {name}: sqlite3 (re-executes)", sql, None)):
                samples, got = measure(fn, ctx.repeat, ctx.warmup, setup, expect=want)
                result.check(f"{label} rows", got == want, f"{got!r} != {want!r}")
                result.add_timing(label, samples, per=1)
            result.check(f"N={n} {name}: the hits were hits", cache.stats()["hits"] >= hits_before + ctx.repeat,
                         cache.stats())
            expected.append(want)
            measured.append(hit())
    result.metrics["cache_stats"] = {n: _data(ctx, n).space.query_cache_stats() for n in sizes}
    result.expected, result.checksum = expected, measured
    return result


def _shape(info) -> str:
    out = [info["plan"]]
    for child in info.get("children", ()):
        out.append(_shape(child))
    return " <- ".join(out)
