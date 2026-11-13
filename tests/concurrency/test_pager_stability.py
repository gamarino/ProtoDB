"""Pagers and cached queries under concurrent writers (Task 8.4b, D56; R77), GIL on and off.

Writers keep committing insertions before, inside and after the pages being read, and
removals, while readers page through snapshots (by rank and materialized, ascending and
descending, resuming from a token half-way) and ask the shared result cache for queries.
Every reader must see exactly the rows of its own snapshot: no duplicate, no skip, and
every cached result equal to a fresh execution on the same snapshot.
"""
import random
import threading
import time

from proto_db.indexing import RangeIndex
from proto_db.query.linq import Agg, F, from_collection
from proto_db.query.terms import Term
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import exact_token

RUN_KW = dict(retries=500, backoff=(0.0005, 0.005))
#: Commits after which the writers stop although a reader may still run (CI-314).  Every row the
#: writers add makes each reader's next query longer, so on a slow runner unbounded writers grow
#: the readers' work faster than the readers finish it.  A local run commits about 130 times.
MAX_COMMITS = 1500


def _plain(value):
    """Frozen containers as plain ones (cached results are frozen by design, D25); exact scalars."""
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    return value


def _rows(seq):
    return [exact_token(_plain(r)) for r in seq]


def test_pagers_and_cached_queries_stay_on_their_snapshot_under_writers():
    space = ObjectSpace(MemoryStorage(), query_cache_bytes=8 * 1024 * 1024)
    db = space.new_database("db")
    db.run(lambda tx: tx.set_root_object("items", tx.new_list().add_index(RangeIndex("k", "k"))
                                         .extend([{"k": 10 * i, "v": i} for i in range(400)])))
    stop = threading.Event()
    errors = []
    commits = [0]

    def writer(seed):
        rnd = random.Random(seed)
        n = 0
        while not stop.is_set() and commits[0] < MAX_COMMITS:
            n += 1

            def change(tx, n=n):
                items = tx.get_root_object("items")
                choice = rnd.random()
                if choice < 0.3:
                    items = items.append_last({"k": -n, "v": -n})                  # before every page
                elif choice < 0.6:
                    items = items.append_last({"k": 10 * rnd.randrange(400) + 5, "v": n})  # inside
                elif choice < 0.8:
                    items = items.append_last({"k": 100_000 + n, "v": n})         # after every page
                elif items.count > 50:
                    items = items.remove_at(rnd.randrange(items.count))
                tx.set_root_object("items", items)
            try:
                db.run(change, **RUN_KW)
                commits[0] += 1
            except Exception as e:  # recorded and re-raised by the main thread
                errors.append(e)
                return

    def pager_reader(where, order, page_size):
        try:
            for _ in range(4):
                if stop.is_set():                       # the test ended (timeout): stop early
                    return
                with db.new_transaction() as tx:
                    items = tx.get_root_object("items")
                    query = from_collection(items)
                    if where is not None:
                        query = query.where(where)
                    query = query.order_by(F[order[0]], order[1])
                    expected = _rows(query)
                    pager = tx.pager("items", where=where, order_by=[order], page_size=page_size)
                    token = pager.token()
                seen = []
                half = pager.pages // 2
                for k in range(half):
                    seen.extend(pager.page(k))
                resumed = db.resume_pager(token)              # another handle, later, same pages
                for k in range(half, resumed.pages + 1):
                    seen.extend(resumed.page(k))
                assert _rows(seen) == expected, (where, order)
                assert pager.rows == resumed.rows == len(expected)
        except Exception as e:
            errors.append(e)

    def cache_reader(seed):
        rnd = random.Random(seed)
        try:
            for _ in range(30):
                if stop.is_set():                       # the test ended (timeout): stop early
                    return
                with db.new_transaction() as tx:
                    items = tx.get_root_object("items")
                    lo = rnd.randrange(0, 4000, 500)
                    for q in (from_collection(items).where(F.k >= lo).order_by(F.v),
                              from_collection(items).where((F.k < lo) | (F.v == 7)).select(["k"]),
                              from_collection(items).group_by(F.s, {"n": Agg.count()})):
                        assert _rows(space.query_cache.result(q)) == _rows(q)
        except Exception as e:
            errors.append(e)

    # Daemon threads, all stopped in ``finally`` (CI-314): when pytest-timeout ends the test by
    # raising in the main thread, threads left running would load every later test of the suite.
    writers = [threading.Thread(target=writer, args=(s,), daemon=True) for s in range(3)]
    readers = [threading.Thread(target=pager_reader, args=args, daemon=True) for args in (
        (None, ("k", False), 23),                       # ordered scan: by rank
        (Term("k", ">=", 1000), ("k", True), 17),       # range + order by the key: by rank
        (Term("v", "<", 300), ("k", False), 31),        # unindexed predicate: materialized
    )] + [threading.Thread(target=cache_reader, args=(s,), daemon=True) for s in range(3)]
    try:
        for t in writers + readers:
            t.start()
        deadline = time.monotonic() + 240               # inside the 300 s pytest-timeout budget
        for t in readers:
            t.join(timeout=max(0.0, deadline - time.monotonic()))
    finally:
        stop.set()
        deadline = time.monotonic() + 30                # the commit or query in flight
        for t in writers + readers:
            t.join(timeout=max(0.0, deadline - time.monotonic()))
    assert not any(t.is_alive() for t in writers + readers)
    if errors:
        raise errors[0]
    assert commits[0] > 20, commits
    assert space.query_cache_stats()["hits"] > 0
