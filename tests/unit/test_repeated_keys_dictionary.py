"""Unit tests for proto_db.collections.dictionaries.RepeatedKeysDictionary (spec section 8.2, D26)."""
import random
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.collections.base import ConcurrentOptimized
from proto_db.collections.dictionaries import RepeatedKeysDictionary
from proto_db.collections.sets import CountedSet
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import order_key
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex


def make(pairs=(), indexes=()):
    d = RepeatedKeysDictionary()
    for name in indexes:
        d = d.add_index(RecordingIndex(name))
    for k, v in pairs:
        d = d.set_at(k, v)
    return d


def reload(coll, context=None):
    context = context or DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


def content(d):
    """``{order_key(key): {order_key(value): count}}``."""
    return {order_key(k): {order_key(v): n for v, n in cs.items()} for k, cs in d.items()}


def literal(start: dict, ops) -> dict:
    """Independent model: replay RKD ops one at a time on nested counters (clamped at zero)."""
    model = {k: Counter(vs) for k, vs in start.items()}
    for op in ops:
        k = order_key(op[1])
        if op[0] == "remove_key":
            model.pop(k, None)
            continue
        v = order_key(op[2])
        bucket = model.setdefault(k, Counter())
        if op[0] == "add":
            bucket[v] += 1
        elif bucket[v] > 0:
            bucket[v] -= 1
        if not +bucket:
            model.pop(k)
        else:
            model[k] = +bucket
    return {k: dict(vs) for k, vs in model.items()}


class TestBasics:
    def test_empty(self):
        d = RepeatedKeysDictionary()
        assert d.count == 0 and d.total_count == 0 and list(d) == [] and not d.has("k")
        assert isinstance(d.get_at("k"), CountedSet) and d.get_at("k").count == 0

    def test_concurrent_optimized(self):
        assert isinstance(RepeatedKeysDictionary(), ConcurrentOptimized)

    def test_set_get(self):
        d = make([("k", "a"), ("k", "b"), ("k", "a"), ("j", "a")])
        assert d.has("k") and "j" in d and not d.has("z")
        assert d.get_at("k").count_of("a") == 2 and d.get_at("k").count_of("b") == 1
        assert d.count == 3 and d.total_count == 4 and len(d) == 3
        assert list(d.keys()) == ["j", "k"]  # key order
        # elements(): each distinct value per key once, in key order
        assert list(d.elements()) == ["a"] + list(d.get_at("k").elements())
        assert sorted(d.get_at("k").elements()) == ["a", "b"]

    def test_remove_at_drops_all_records(self):
        d = make([("k", "a"), ("k", "b"), ("k", "a"), ("j", "a")])
        r = d.remove_at("k")
        assert not r.has("k") and r.count == 1 and r.total_count == 1 and list(r) == ["a"]
        assert d.has("k")
        absent = d.remove_at("zz")
        assert absent is not d and absent.root is d.root and absent.count == d.count

    def test_remove_record_absent_is_noop(self):
        d = make([("k", "a")])
        for r in (d.remove_record("k", "b"), d.remove_record("z", "a")):
            assert r is not d and r.root is d.root and content(r) == content(d)

    def test_spec_aliases(self):
        d = RepeatedKeysDictionary().add("k", 1).add("k", 2)
        assert d.get("k").count == 2
        assert not d.remove_key("k").has("k")

    def test_get_at_returns_counted_set_without_log(self):
        cs = make([("k", 1)]).get_at("k")
        assert isinstance(cs, CountedSet) and cs._op_log == () and cs.index_names() == []

    def test_key_constraints(self):
        for bad in ([1], {"a": 1}, float("nan")):
            with pytest.raises(ProtoValidationError):
                RepeatedKeysDictionary().set_at(bad, 1)
        with pytest.raises(ProtoValidationError):
            RepeatedKeysDictionary().set_at("k", [1])  # values are CountedSet members: orderable

    def test_immutable(self):
        with pytest.raises(ProtoUserError):
            make([("k", 1)]).root = None

    def test_bool_int_keys_and_values(self):
        d = make([(1, 1), (True, True), (1.0, 1.0), (Decimal(1), True)])
        assert list(content(d)) == sorted([order_key(True), order_key(1)])
        assert d.get_at(1).count_of(1) == 2 and d.get_at(1).count_of(True) == 1
        assert d.get_at(True).count_of(True) == 1 and d.get_at(True).count_of(1) == 0


class TestRegressions:
    def test_remove_record_keeps_other_records(self):
        """0.1 bug class: removing one record must not drop the key's other records."""
        d = make([("k", "a"), ("k", "b"), ("k", "a")])
        r = d.remove_record("k", "a")
        assert r.has("k") and r.get_at("k").count_of("a") == 1 and r.get_at("k").count_of("b") == 1
        r = r.remove_record("k", "a")
        assert r.has("k") and r.get_at("k").count_of("a") == 0 and r.get_at("k").count_of("b") == 1
        r = r.remove_record("k", "b")
        assert not r.has("k") and r.count == 0 and r.total_count == 0

    def test_persistence_round_trip_and_atoms(self):
        context = DirectContext(MemoryStorage())
        obj = DBObject(context=context, name="x")
        d = RepeatedKeysDictionary(context=context).set_at("k", obj).set_at("k", obj).set_at("j", 5)
        loaded = reload(d, context)
        assert content(loaded) == content(d) and loaded.count == 2 and loaded.total_count == 3
        assert loaded.get_at("k").count_of(obj) == 2
        assert next(iter(loaded.get_at("k"))).name == "x"
        again = reload(loaded.set_at("k", obj).remove_record("j", 5), context)
        assert again.get_at("k").count_of(obj) == 3 and not again.has("j")


class TestIndexEvents:
    def test_events(self):
        d = make([("k", "a"), ("k", "b"), ("k", "b"), ("j", "a")], indexes=("i",))
        base = d.get_index("i")
        assert d.set_at("k", "c").get_index("i").events_since(base) == (("add", "c"),)
        assert d.set_at("z", "a").get_index("i").events_since(base) == (("add", "a"),)
        assert d.set_at("k", "a").indexes is d.indexes
        assert d.remove_record("k", "b").indexes is d.indexes
        assert d.remove_record("k", "a").get_index("i").events_since(base) == (("remove", "a"),)
        assert d.remove_at("k").get_index("i").events_since(base) == tuple(
            ("remove", v) for v in d.get_at("k"))
        assert d.remove_at("zz").indexes is d.indexes
        assert d.get_index("i").live() == Counter(order_key(v) for v in ["a", "b", "a"])


class TestRebase:
    def test_rebase_nested_deltas(self):
        snapshot = reload(make([("k", "a"), ("j", "x")], indexes=("i",)))
        theirs = snapshot.set_at("k", "a").set_at("n", 1).remove_at("j")
        mine = snapshot.set_at("k", "a").set_at("k", "b").remove_record("j", "x").set_at("m", 2)
        merged = mine.rebase_onto(theirs)
        assert content(merged) == literal(content(theirs), mine._op_log)
        assert merged.get_at("k").count_of("a") == 3 and merged.get_at("k").count_of("b") == 1
        assert not merged.has("j") and merged.has("n") and merged.has("m")
        assert merged.get_index("i").live() == Counter(order_key(v) for v in merged)

    def test_remove_key_then_add_in_mine(self):
        snapshot = reload(make([("k", "a"), ("k", "a")]))
        theirs = snapshot.set_at("k", "b")
        mine = snapshot.remove_at("k").set_at("k", "c")
        merged = mine.rebase_onto(theirs)
        assert content(merged) == {order_key("k"): {order_key("c"): 1}}

    def test_rebase_matches_serial_replay(self):
        rng = random.Random(11)

        def step(d):
            k, v, r = rng.randrange(3), rng.randrange(3), rng.random()
            if r < 0.5:
                return d.set_at(k, v)
            if r < 0.85:
                return d.remove_record(k, v)
            return d.remove_at(k)

        for _ in range(200):
            snapshot = make([(rng.randrange(3), rng.randrange(3)) for _ in range(rng.randrange(6))])
            snapshot = snapshot._with(_op_log=())
            theirs, mine = snapshot, snapshot
            for _ in range(rng.randrange(8)):
                theirs = step(theirs)
            for _ in range(rng.randrange(10)):
                mine = step(mine)
            merged = mine.rebase_onto(theirs)
            assert content(merged) == literal(content(theirs), mine._op_log)
            assert merged.total_count == sum(n for _, cs in merged.items() for _, n in cs.items())
            assert merged.count == sum(1 for _ in merged)


class TestBulk:
    def test_from_items(self):
        d = RepeatedKeysDictionary.from_items([("k", 1), ("j", 2), ("k", 1), (1, "a"), (1.0, "a")])
        assert content(d) == content(make([("k", 1), ("j", 2), ("k", 1), (1, "a"), (1, "a")]))
        assert d.total_count == 5 and d.count == 3 and d._op_log == ()
        avl.check_invariants(d.root)
        assert RepeatedKeysDictionary.from_items([]).count == 0


class TestFreeThreading:
    def test_concurrent_readers(self):
        n = 300
        base = reload(RepeatedKeysDictionary.from_items((i % n, i % 7) for i in range(4 * n)))
        expected = content(RepeatedKeysDictionary.from_items((i % n, i % 7) for i in range(4 * n)))
        errors, barrier = [], threading.Barrier(8)

        def reader(seed):
            barrier.wait()
            for _ in range(3):
                if content(base) != expected:
                    errors.append("content")
                for k in range(seed, n, 17):
                    if base.get_at(k).total_count != 4:
                        errors.append(k)
                derived = base.set_at(seed, "x").remove_at(seed + 1)
                if not derived.has(seed) or derived.has(seed + 1) or base.get_at(seed).has("x"):
                    errors.append(f"derive {seed}")

        threads = [threading.Thread(target=reader, args=(s,)) for s in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []


class TestReviewFixes:
    def test_remove_record_validates_record_on_absent_key(self):
        d = RepeatedKeysDictionary().set_at("k", 1)
        for bad in ([1], {"a": 1}, float("nan")):
            with pytest.raises(ProtoValidationError):
                d.remove_record("zz", bad)
            with pytest.raises(ProtoValidationError):
                d.remove_record("k", bad)
        assert d._op_log == (("add", "k", 1),)

    def test_replay_does_not_churn_surviving_records(self):
        """A record that survives the merge sees no remove/add pair (one step per record)."""
        snapshot = reload(make([("k", "a"), ("k", "b")], indexes=("i",)))
        theirs = snapshot.set_at("j", 1)
        mine = snapshot.remove_record("k", "a").set_at("k", "a").remove_record("k", "b").remove_record("k", "b")
        merged = mine.rebase_onto(theirs)
        assert merged.get_index("i").events_since(theirs.get_index("i")) == (("remove", "b"),)
        assert merged.get_at("k").count_of("a") == 1
