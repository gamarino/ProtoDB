"""Unit tests for proto_db.collections.dictionaries.Dictionary (spec section 8.2, D8, D9, D23)."""
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.collections.base import ConcurrentOptimized
from proto_db.collections.dictionaries import Dictionary
from proto_db.errors import ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import order_key
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex


def make(pairs=(), indexes=()):
    d = Dictionary()
    for name in indexes:
        d = d.add_index(RecordingIndex(name))
    for k, v in pairs:
        d = d.set_at(k, v)
    return d


def reload(coll, context=None):
    context = context or DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


class TestBasics:
    def test_empty(self):
        d = Dictionary()
        assert d.count == 0 and list(d.items()) == [] and list(d) == [] and not d.has("a")
        assert d.get_at("a") is None and d.get_at("a", 7) == 7

    def test_concurrent_optimized(self):
        assert isinstance(Dictionary(), ConcurrentOptimized)

    def test_set_get_has(self):
        d = make([("b", 2), ("a", 1), ("c", 3)])
        assert d.get_at("a") == 1 and d.has("b") and "c" in d and "z" not in d
        assert list(d.keys()) == ["a", "b", "c"]
        assert list(d.values()) == [1, 2, 3] == list(d.elements()) == list(d)
        assert list(d.items()) == [("a", 1), ("b", 2), ("c", 3)]
        assert d.count == 3 == len(d)

    def test_keys_in_value_order_across_types(self):
        d = make([("s", 0), (2, 0), (None, 0), (True, 0), (b"b", 0), ((1, 2), 0)])
        assert list(d.keys()) == [None, True, 2, "s", b"b", (1, 2)]

    def test_overwrite_keeps_count_and_first_key_object(self):
        d = make([(1, "a")]).set_at(1.0, "b")
        assert d.count == 1 and d.get_at(1) == "b"
        assert type(list(d.keys())[0]) is int

    def test_remove(self):
        d = make([("a", 1), ("b", 2)])
        r = d.remove_at("a")
        assert list(r.items()) == [("b", 2)] and r.count == 1
        assert list(d.items()) == [("a", 1), ("b", 2)]

    def test_remove_absent_key_never_deletes_another_entry(self):
        """0.1 regression: removing an absent key removed a neighbour."""
        d = make([(k, k) for k in range(0, 20, 2)])
        for absent in (-1, 1, 7, 19, 100, "x", None):
            r = d.remove_at(absent)
            assert list(r.items()) == list(d.items()) and r.count == d.count
            assert r is not d and r.root is d.root

    def test_unorderable_keys_rejected(self):
        for key in ([1], {"a": 1}, float("nan")):
            with pytest.raises(ProtoValidationError):
                Dictionary().set_at(key, 1)
        with pytest.raises(ProtoValidationError):
            Dictionary().set_at(object(), 1)
        with pytest.raises(ProtoValidationError):
            Dictionary().set_at("k", object())

    def test_unorderable_values_allowed(self):
        d = make([("a", [1, 2]), ("b", {"x": (1,)})])
        assert d.get_at("a") == [1, 2] and reload(d).get_at("b") == {"x": (1,)}

    def test_immutable(self):
        with pytest.raises(ProtoUserError):
            make([("a", 1)]).count = 3


class TestKeyRegressions:
    def test_large_int_keys_all_retrievable(self):
        """0.1 regression: int keys above 2**53 collided through float conversion."""
        keys = [2**60 + i for i in range(50)]
        d = make([(k, i) for i, k in enumerate(keys)])
        assert d.count == 50
        assert all(d.get_at(k) == i for i, k in enumerate(keys))
        loaded = reload(d)
        assert all(loaded.get_at(k) == i and loaded.has(k) for i, k in enumerate(keys))
        assert list(loaded.keys()) == keys

    def test_bool_int_float_keys(self):
        """1 and 1.0 (and Decimal 1) are one key; True is a distinct key."""
        d = make([(1, "int"), (True, "bool"), (1.0, "float")])
        assert d.count == 2
        assert d.get_at(1) == "float" and d.get_at(Decimal(1)) == "float" and d.get_at(True) == "bool"
        assert d.remove_at(1.0).get_at(True) == "bool"
        assert make([(False, 1)]).get_at(0) is None

    def test_has_and_get_on_reloaded_dictionary(self):
        """0.1 regression: has() on a lazily loaded dictionary answered before loading."""
        context = DirectContext(MemoryStorage())
        d = make([("a", 1), ("b", 2)])
        for probe in ("has", "get", "contains"):
            loaded = reload(d, context)  # a fresh, unloaded stub each time
            if probe == "has":
                assert loaded.has("a") and not loaded.has("z")
            elif probe == "get":
                assert loaded.get_at("b") == 2 and loaded.get_at("z") is None
            else:
                assert "a" in loaded and "z" not in loaded


class TestIndexEvents:
    def test_events(self):
        d = make([("a", 1), ("b", 2)], indexes=("i",))
        base = d.get_index("i")
        assert d.set_at("c", 3).get_index("i").events_since(base) == (("add", 3),)
        assert d.set_at("a", 10).get_index("i").events_since(base) == (("replace", 1, 10),)
        assert d.remove_at("b").get_index("i").events_since(base) == (("remove", 2),)
        assert d.remove_at("zz").indexes is d.indexes


class TestOpLogAndRebase:
    def test_every_call_is_logged(self):
        d = reload(make([("a", 1)]))
        d2 = d.set_at("b", 2).remove_at("a").remove_at("q").set_at("b", 2)
        assert d2._op_log == (("set", "b", 2), ("remove", "a"), ("remove", "q"), ("set", "b", 2))

    def test_rebase_per_key(self):
        snapshot = reload(make([("k", 1), ("j", 5)]))
        theirs = snapshot.set_at("k", 2).set_at("x", 9)
        mine = snapshot.set_at("k", 3).remove_at("j").set_at("y", 4)
        merged = mine.rebase_onto(theirs)
        assert dict(merged.items()) == {"k": 3, "x": 9, "y": 4}

    def test_blind_remove_wins_over_concurrent_insert(self):
        snapshot = reload(make([("a", 1)]))
        theirs = snapshot.set_at("j", 5)
        mine = snapshot.remove_at("j")  # a no-op on mine, logged as intent
        assert dict(mine.rebase_onto(theirs).items()) == {"a": 1}

    def test_rebase_keeps_indexes_consistent(self):
        snapshot = reload(make([("a", 1), ("b", 2)], indexes=("i",)))
        theirs = snapshot.set_at("b", 20).set_at("c", 3)
        mine = snapshot.set_at("b", 200).remove_at("a")
        merged = mine.rebase_onto(theirs)
        assert dict(merged.items()) == {"b": 200, "c": 3}
        assert merged.get_index("i").live() == Counter(order_key(v) for v in merged.values())


class TestBulk:
    def test_from_items(self):
        d = Dictionary.from_items([("b", 1), ("a", 2), ("b", 3), (1, 0), (1.0, 9)])
        assert list(d.items()) == [(1, 9), ("a", 2), ("b", 3)] and d.count == 3
        avl.check_invariants(d.root)
        with pytest.raises(ProtoValidationError):
            Dictionary.from_items([([1], 1)])
        assert Dictionary.from_items([]).count == 0

    def test_from_items_context(self):
        context = DirectContext(MemoryStorage())
        assert Dictionary.from_items([("a", 1)], context=context).context is context

    def test_large_bulk_dictionary(self):
        n = 100_000
        d = Dictionary.from_items((i, str(i)) for i in range(n))
        assert d.count == n and d.get_at(99_999) == "99999"
        avl.check_invariants(d.root)
        loaded = reload(d)
        assert loaded.get_at(31337) == "31337" and loaded.has(0) and not loaded.has(n)
        assert sum(1 for _ in loaded.items()) == n
        d2 = loaded.set_at(n, "x").remove_at(5)
        assert d2.count == n and d2.get_at(n) == "x" and not d2.has(5)


class TestFreeThreading:
    @pytest.mark.parametrize("lazy", [False, True])
    def test_concurrent_readers_and_writers(self, lazy):
        """8 readers see a consistent version while other threads derive versions from it."""
        n = 2_000
        base = Dictionary.from_items((i, i * 2) for i in range(n))
        if lazy:
            base = reload(base)
        expected_items = [(i, i * 2) for i in range(n)]
        errors, barrier = [], threading.Barrier(12)

        def reader():
            barrier.wait()
            for round_ in range(5):
                if list(base.items()) != expected_items:
                    errors.append("items differ")
                for i in range(round_, n, 37):
                    if base.get_at(i) != i * 2 or not base.has(i):
                        errors.append(f"key {i}")
                if base.count != n:
                    errors.append("count")

        def writer(seed):
            barrier.wait()
            d = base
            for i in range(300):
                d = d.set_at(n + seed * 1000 + i, i).remove_at((seed * 7 + i) % n)
            if d.count != n:
                errors.append(f"writer {seed} count {d.count}")

        threads = [threading.Thread(target=reader) for _ in range(8)]
        threads += [threading.Thread(target=writer, args=(s,)) for s in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert list(base.items()) == expected_items
