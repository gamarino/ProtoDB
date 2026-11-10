"""Unit tests for proto_db.collections.sets.Set (spec section 8.2, D24, D26)."""
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.collections.base import ConcurrentOptimized
from proto_db.collections.sets import Set
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import hash_key, order_key
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex


class CollidingSet(Set):
    """Four buckets only, so that collision handling is exercised on every operation."""

    __type_name__ = "tests.unit.CollidingSet"

    def _bucket_hash(self, element):
        return hash_key(element) % 4


both = pytest.mark.parametrize("cls", [Set, CollidingSet])


def make(items=(), indexes=(), cls=Set):
    s = cls()
    for name in indexes:
        s = s.add_index(RecordingIndex(name))
    for e in items:
        s = s.add(e)
    return s


def reload(coll, context=None):
    context = context or DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


def keys(coll):
    return sorted(order_key(e) for e in coll.elements())


class TestBasics:
    def test_empty(self):
        s = Set()
        assert s.count == 0 and len(s) == 0 and list(s) == [] and not s.has(1) and 1 not in s

    def test_concurrent_optimized(self):
        assert isinstance(Set(), ConcurrentOptimized)

    @both
    def test_add_has_remove(self, cls):
        s = make(range(40), cls=cls)
        assert s.count == 40 and keys(s) == keys(make(range(40)))
        assert all(s.has(i) and i in s for i in range(40)) and not s.has(40)
        r = s.remove(3).remove(17)
        assert r.count == 38 and not r.has(3) and not r.has(17) and r.has(4)
        assert s.count == 40 and s.has(3)

    @both
    def test_add_present_is_content_noop(self, cls):
        s = make(["a", "b"], cls=cls)
        t = s.add("a")
        assert t is not s and t.root is s.root and t.count == 2

    @both
    def test_remove_absent_is_content_noop(self, cls):
        s = make(range(0, 40, 2), cls=cls)
        for absent in (-1, 1, 39, "x", None, True):
            r = s.remove(absent)
            assert r is not s and r.root is s.root and r.count == s.count and keys(r) == keys(s)

    def test_collisions_share_buckets(self):
        s = make(range(20), cls=CollidingSet)
        assert s.root.count <= 4
        assert sum(len(bucket) for _, bucket in avl.key_iter(s.root)) == 20

    def test_element_constraints(self):
        for bad in ([1], {"a": 1}, float("nan")):
            with pytest.raises(ProtoValidationError):
                Set().add(bad)
            with pytest.raises(ProtoValidationError):
                Set().has(bad)
        with pytest.raises(ProtoValidationError):
            Set().add(object())

    def test_canonical_elements(self):
        s = Set().add(bytearray(b"ab")).add((1, bytearray(b"x")))
        stored = list(s.elements())
        assert b"ab" in stored and all(not isinstance(e, bytearray) for e in stored)
        tup = next(e for e in stored if isinstance(e, tuple))
        assert type(tup[1]) is bytes and s.has((1, b"x"))

    def test_immutable(self):
        with pytest.raises(ProtoUserError):
            make([1]).root = None


class TestRegressions:
    @both
    def test_add_never_mutates_receiver(self, cls):
        """0.1 bug: Set.add mutated the receiver."""
        s = make([1, 2], cls=cls)
        snapshot = (s.root, s.count, keys(s))
        s.add(3)
        s.remove(1)
        s.union(make([9]))
        s.intersection(make([1]))
        s.difference(make([1]))
        assert (s.root, s.count, keys(s)) == snapshot

    @both
    def test_bool_and_int_distinct_int_and_float_same(self, cls):
        s = make([1, True, 1.0, Decimal(1), 0, False, 0.0], cls=cls)
        assert s.count == 4
        assert s.has(True) and s.has(1) and s.has(1.0) and s.has(False) and s.has(0)
        assert s.remove(1.0).has(True) and not s.remove(1.0).has(1)
        assert not s.remove(True).has(True) and s.remove(True).has(1)
        # The first stored object is kept for equal elements.
        assert type(next(e for e in s if e == 1 and not isinstance(e, bool))) is int

    @both
    def test_large_int_elements(self, cls):
        items = [2**60 + i for i in range(30)] + [-(2**70)]
        loaded = reload(make(items, cls=cls))
        assert all(loaded.has(i) for i in items) and loaded.count == 31

    @both
    def test_has_on_reloaded_stub(self, cls):
        context = DirectContext(MemoryStorage())
        s = make(["a", "b"], cls=cls)
        assert reload(s, context).has("a") and not reload(s, context).has("z") and "b" in reload(s, context)

    def test_atom_element_identity_after_save(self):
        """An atom element is the same member (by oid) before and after save/load."""
        context = DirectContext(MemoryStorage())
        obj, other = DBObject(context=context), DBObject(context=context)
        s = Set(context=context).add(obj)
        loaded = reload(s, context)
        assert loaded.has(obj) and not loaded.has(other)
        (element,) = list(loaded.elements())
        assert element.oid == obj.oid and element == obj
        assert loaded.add(obj).count == 1  # the reloaded stub and the original are one member
        reloaded_twice = reload(loaded.add(other), context)
        assert reloaded_twice.count == 2 and reloaded_twice.has(element)


class TestAlgebra:
    @both
    def test_union_intersection_difference(self, cls):
        a, b = make(range(0, 20), cls=cls), make(range(10, 30), cls=cls)
        assert keys(a.union(b)) == keys(make(range(30)))
        assert keys(a.intersection(b)) == keys(make(range(10, 20)))
        assert keys(a.difference(b)) == keys(make(range(10)))
        assert a.union(b).count == 30 and a.intersection(b).count == 10 and a.difference(b).count == 10

    def test_accepts_iterables(self):
        a = make([1, 2, 3])
        assert keys(a.union([3, 4, 4])) == keys(make([1, 2, 3, 4]))
        assert keys(a.intersection([2.0, 3, 7])) == keys(make([2, 3]))
        assert keys(a.difference((1, True))) == keys(make([2, 3]))

    def test_index_events_incremental(self):
        a = make([1, 2, 3], indexes=("i",))
        base = a.get_index("i")
        b = make([3, 4])
        assert a.union(b).get_index("i").events_since(base) == tuple(("add", e) for e in b if not a.has(e))
        assert a.intersection(make([2])).get_index("i").events_since(base) == tuple(
            ("remove", e) for e in a if e != 2)
        assert a.difference(make([1, 9])).get_index("i").events_since(base) == (("remove", 1),)
        assert a.union(make([1])).indexes is a.indexes

    def test_algebra_logs_intent(self):
        a = make([1, 2, 3])._with(_op_log=())
        assert a.union([3, 4])._op_log == (("add", 3), ("add", 4))
        assert a.difference([1, 9])._op_log == (("remove", 1), ("remove", 9))
        assert a.intersection([2])._op_log == tuple(("remove", e) for e in a if e != 2)


class TestIndexEvents:
    @both
    def test_events(self, cls):
        s = make(["a", "b"], indexes=("i",), cls=cls)
        base = s.get_index("i")
        assert s.add("c").get_index("i").events_since(base) == (("add", "c"),)
        assert s.add("a").indexes is s.indexes
        assert s.remove("b").get_index("i").events_since(base) == (("remove", "b"),)
        assert s.remove("zz").indexes is s.indexes


class TestRebase:
    @both
    def test_rebase_per_element_last_intent(self, cls):
        snapshot = reload(make([1, 2, 3], indexes=("i",), cls=cls))
        theirs = snapshot.remove(1).add(7).remove(2)
        mine = snapshot.add(2).remove(3).add(8).remove(8).add(9).remove(77)
        merged = mine.rebase_onto(theirs)
        assert keys(merged) == keys(make([2, 7, 9]))
        assert merged.get_index("i").live() == Counter(order_key(e) for e in merged)


class TestBulk:
    @both
    def test_from_iterable(self, cls):
        s = cls.from_iterable([3, 1, 1.0, True, "a", 3])
        assert keys(s) == keys(make([1, 3, True, "a"])) and s.count == 4 and s._op_log == ()
        avl.check_invariants(s.root)
        assert cls.from_iterable([]).count == 0
        with pytest.raises(ProtoValidationError):
            cls.from_iterable([[1]])

    def test_large_bulk(self):
        n = 50_000
        loaded = reload(Set.from_iterable(range(n)))
        assert loaded.count == n and all(loaded.has(i) for i in range(0, n, 997)) and not loaded.has(n)
        assert sum(1 for _ in loaded) == n


class TestFreeThreading:
    @pytest.mark.parametrize("lazy", [False, True])
    def test_concurrent_readers(self, lazy):
        n = 1_500
        base = Set.from_iterable(f"e{i}" for i in range(n))
        if lazy:
            base = reload(base)
        expected = keys(Set.from_iterable(f"e{i}" for i in range(n)))
        errors, barrier = [], threading.Barrier(8)

        def reader(seed):
            barrier.wait()
            for _ in range(3):
                if keys(base) != expected or base.count != n:
                    errors.append("elements")
                for i in range(seed, n, 37):
                    if not base.has(f"e{i}"):
                        errors.append(i)
                derived = base.add(f"x{seed}").remove(f"e{seed}")
                if derived.count != n or base.has(f"x{seed}"):
                    errors.append(f"derive {seed}")

        threads = [threading.Thread(target=reader, args=(s,)) for s in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []


class TestReviewFixes:
    def test_algebra_returns_new_version_when_content_unchanged(self):
        a = make([1, 2])
        for result in (a.union([]), a.union([1]), a.intersection([1, 2]), a.difference([]), a.difference([9])):
            assert result is not a and result.root is a.root and result.indexes is a.indexes
