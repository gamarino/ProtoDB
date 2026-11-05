"""The query result cache keyed by pointers (Task 8.4b, D56).

Keys come from the plan without executing it; equal keys give equal results; a commit
that changes what a query reads changes its key; uncacheable plans say why; the cache is
bounded by estimated bytes and holds atoms as references, never as loaded objects.
"""
import gc
import weakref

import pytest

from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoValidationError
from proto_db.indexing import RangeIndex
from proto_db.indexing.keyspec import register_extractor, unregister_extractor
from proto_db.query.cache import QUERY_SEMANTICS_VERSION, cache_key
from proto_db.query.linq import F, from_collection
from proto_db.query.planner import plan_query
from proto_db.query.expressions import And, PythonPredicate
from proto_db.query.plans import CollectionScanPlan, Filter, PythonFilter, ValuesScan
from proto_db.query.terms import Term
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import FrozenDict


def _space(**kw):
    space = ObjectSpace(MemoryStorage(), **kw)
    db = space.new_database("db")
    return space, db


def _seed(db, n=200, name="items"):
    def build(tx):
        items = tx.new_list().add_index(RangeIndex("k", "k"))
        items = items.extend([{"k": i % 50, "v": i} for i in range(n)])
        tx.set_root_object(name, items)
        tx.set_root_object("other", 0)
    db.run(build)


def test_equal_queries_on_one_version_share_a_key_and_an_entry():
    space, db = _space()
    _seed(db)
    with db.new_transaction() as t1, db.new_transaction() as t2:
        q1 = from_collection(t1.get_root_object("items")).where(F.k >= 40).order_by(F.v)
        q2 = from_collection(t2.get_root_object("items")).where(F.k >= 40).order_by(F.v)
        k1, k2 = cache_key(q1)[0], cache_key(q2)[0]
        assert k1 == k2 and k1[0] == QUERY_SEMANTICS_VERSION
        r1 = space.query_cache.result(q1)
        r2 = space.query_cache.result(q2)
        assert r2 is r1
        assert list(r1) == list(q1)
    stats = space.query_cache_stats()
    assert stats["hits"] == 1 and stats["misses"] == 1 and stats["entries"] == 1


def test_a_commit_to_the_collection_changes_the_key_and_another_root_does_not():
    space, db = _space()
    _seed(db)
    key = lambda tx: cache_key(from_collection(tx.get_root_object("items")).where(F.k == 3))[0]
    with db.new_transaction() as tx:
        before = key(tx)
    db.run(lambda tx: tx.set_root_object("other", 1))
    with db.new_transaction() as tx:
        assert key(tx) == before
    db.run(lambda tx: tx.set_root_object("items", tx.get_root_object("items").append_last({"k": 3, "v": -1})))
    with db.new_transaction() as tx:
        after = key(tx)
        assert after != before
        result = space.query_cache.result(from_collection(tx.get_root_object("items")).where(F.k == 3))
        assert {"k": 3, "v": -1} in list(result)


def test_exact_operands_give_distinct_keys():
    space, db = _space()
    _seed(db)
    with db.new_transaction() as tx:
        items = tx.get_root_object("items")
        keys = {cache_key(plan_query(items, Term("k", "==", v)))[0] for v in (1, 1.0, True)}
        assert len(keys) == 3


def test_uncacheable_plans_report_why_and_are_never_stored():
    space, db = _space()
    _seed(db)
    with db.new_transaction() as tx:
        items = tx.get_root_object("items")
        scan = CollectionScanPlan(items)
        cases = {
            "Python filter": PythonFilter(scan, lambda e: e["k"] > 3, "k > 3"),
            "ValuesScan": ValuesScan([1, 2]),
            "Python filter (": from_collection(items).where(lambda e: e["v"] % 2).plan(),
            "Python predicate": Filter(scan, And(Term("k", "==", 1), PythonPredicate(lambda e: True, "always"))),
            "callable": from_collection(items).order_by(lambda e: -e["v"]).plan(),
            "Python projection": from_collection(items).select(lambda e: e["v"]).plan(),
        }
        for words, plan in cases.items():
            key, reason = cache_key(plan)
            assert key is None and words in reason, (words, reason)
            result = space.query_cache.result(plan)
            assert result.key is None and list(result) == list(plan.execute())
            assert space.query_cache.explain(plan)["cache"] == {"uncacheable": reason}
        changed = items.append_last({"k": 1, "v": 1})
        key, reason = cache_key(from_collection(changed).where(F.k == 1))
        assert key is None and "not saved" in reason
    assert space.query_cache_stats()["entries"] == 0


def test_registered_extractor_generation_is_part_of_the_key():
    register_extractor("t84b_mod", lambda e: e["v"] % 7)
    try:
        space, db = _space()
        _seed(db)
        with db.new_transaction() as tx:
            items = tx.get_root_object("items")
            q = plan_query(items, Term("@t84b_mod", "==", 3))
            first, _ = cache_key(q)
            assert first is not None
            assert [e["v"] % 7 for e in space.query_cache.result(q)] == [3] * len(space.query_cache.result(q))
            unregister_extractor("t84b_mod")
            register_extractor("t84b_mod", lambda e: e["v"] % 5)
            q2 = plan_query(items, Term("@t84b_mod", "==", 3))
            second, _ = cache_key(q2)
            assert second != first
            assert all(e["v"] % 5 == 3 for e in space.query_cache.result(q2))
            assert cache_key(from_collection(items).order_by(F["@t84b_mod"]))[0] is not None
    finally:
        unregister_extractor("t84b_mod")
    with pytest.raises(ProtoValidationError):
        unregister_extractor("t84b_mod")


def test_results_are_frozen_and_equal_on_hit_and_miss():
    space, db = _space()
    _seed(db)
    with db.new_transaction() as tx:
        q = from_collection(tx.get_root_object("items")).where(F.k < 3).select(["k", "v"])
        miss = space.query_cache.result(q)
        hit = space.query_cache.result(q)
        assert hit is miss and miss.key is not None
        assert list(miss) == list(q)
        assert all(isinstance(row, FrozenDict) for row in miss)
        assert miss[1:3] == tuple(list(q)[1:3])


def test_atoms_are_held_by_reference_not_pinned():
    space, db = _space(atom_cache_entries=0)

    def build(tx):
        objs = tx.new_list().extend([tx.new_object(n=i) for i in range(30)])
        tx.set_root_object("objs", objs)
    db.run(build)
    with db.new_transaction() as tx:
        objs = tx.get_root_object("objs")
        result = space.query_cache.result(from_collection(objs).where(F.n >= 10))
        assert result.key is not None and len(result) == 20
        loaded = list(result)
        refs = [weakref.ref(o) for o in loaded]
        assert [o.n for o in loaded] == list(range(10, 30))
        assert all(isinstance(o, DBObject) for o in loaded)
    del loaded, objs, tx
    gc.collect()
    assert all(r() is None for r in refs), "the cached result kept loaded atoms alive"
    assert [o.n for o in result][:2] == [10, 11]


def test_the_byte_bound_holds_and_large_results_are_refused():
    space, db = _space(query_cache_bytes=200_000)
    _seed(db, n=2000)
    with db.new_transaction() as tx:
        items = tx.get_root_object("items")
        for k in range(50):
            space.query_cache.result(from_collection(items).where(F.k == k))
            stats = space.query_cache_stats()
            assert stats["bytes"] <= stats["capacity"]
        assert stats["entries"] < 50
        big = space.query_cache.result(from_collection(items))
        assert len(big) == 2000 and big.key is not None
        assert space.query_cache_stats()["refused"] == 1
        assert not space.query_cache.contains(big.key)


def test_explain_shows_the_key_and_whether_it_is_cached():
    space, db = _space()
    _seed(db)
    with db.new_transaction() as tx:
        q = from_collection(tx.get_root_object("items")).where(F.k == 4)
        assert space.query_cache.explain(q)["cache"]["cached"] is False
        space.query_cache.result(q)
        info = space.query_cache.explain(q)
        assert info["cache"]["cached"] is True and len(info["cache"]["key"]) == 32


def test_a_disabled_cache_still_answers():
    space, db = _space(query_cache_bytes=0)
    _seed(db)
    with db.new_transaction() as tx:
        q = from_collection(tx.get_root_object("items")).where(F.k == 4)
        assert list(space.query_cache.result(q)) == list(q)
    assert space.query_cache_stats()["entries"] == 0
