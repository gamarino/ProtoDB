"""content_equals(other): content comparison for every collection (ruling R12, D27)."""
import math
from decimal import Decimal

import pytest

from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.core.dbobject import DBObject
from proto_db.storage.memory import MemoryStorage
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex


def reload(coll, context=None):
    context = context or DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


def fold(coll, method, items):
    for item in items:
        coll = getattr(coll, method)(*item) if isinstance(item, tuple) and method != "add" else \
            getattr(coll, method)(item)
    return coll


class TestCommon:
    def test_different_types_are_never_equal(self):
        empties = [List(), Dictionary(), HashDictionary(), Set(), CountedSet(), RepeatedKeysDictionary()]
        for a in empties:
            for b in empties:
                assert a.content_equals(b) == (type(a) is type(b))
            assert not a.content_equals([]) and not a.content_equals(None)

    def test_indexes_are_not_content(self):
        a = Set.from_iterable([1, 2])
        assert a.add_index(RecordingIndex("i")).content_equals(a)

    def test_equality_survives_reload_and_identity_is_unchanged(self):
        a = Dictionary().set_at("k", [1, {"x": 2}])
        b = Dictionary().set_at("k", [1, {"x": 2}])
        assert a.content_equals(b) and reload(a).content_equals(b) and a != b


class TestList:
    def test_order_matters(self):
        assert List.from_iterable([1, "a", [2]]).content_equals(List().append_last(1.0).append_last("a").append_last([2]))
        assert not List.from_iterable([1, 2]).content_equals(List.from_iterable([2, 1]))
        assert not List.from_iterable([1, 2]).content_equals(List.from_iterable([1, 2, 3]))
        assert not List.from_iterable([True]).content_equals(List.from_iterable([1]))

    def test_unorderable_and_nan_values(self):
        assert List.from_iterable([{"a": [1]}, math.nan]).content_equals(List.from_iterable([{"a": [1]}, math.nan]))
        assert not List.from_iterable([{"a": [1]}]).content_equals(List.from_iterable([{"a": [2]}]))
        assert not List.from_iterable([[1]]).content_equals(List.from_iterable([(1,)]))


@pytest.mark.parametrize("cls", [Dictionary, HashDictionary])
class TestDictionaries:
    def test_insertion_order_irrelevant(self, cls):
        pairs = [(i, i * 2) for i in range(30)] + [("s", [1])]
        assert cls.from_items(pairs).content_equals(fold(cls(), "set_at", reversed(pairs)))
        assert cls.from_items([(1, "a")]).content_equals(cls.from_items([(Decimal(1), "a")]))

    def test_different_content(self, cls):
        base = cls.from_items([(1, "a"), (2, "b")])
        assert not base.content_equals(cls.from_items([(1, "a"), (2, "c")]))
        assert not base.content_equals(cls.from_items([(1, "a"), (3, "b")]))
        assert not base.content_equals(cls.from_items([(1, "a")]))
        assert not cls.from_items([(1, "a")]).content_equals(cls.from_items([(True, "a")]))


class TestSets:
    def test_set(self):
        assert Set.from_iterable(range(40)).content_equals(fold(Set(), "add", reversed(range(40))))
        assert Set.from_iterable([1, "a"]).content_equals(Set.from_iterable(["a", 1.0]))
        assert not Set.from_iterable([1, 2]).content_equals(Set.from_iterable([1, 3]))
        assert not Set.from_iterable([1]).content_equals(Set.from_iterable([True]))

    def test_counted_set(self):
        assert CountedSet.from_iterable([1, 1, 2]).content_equals(CountedSet().add(2).add(1).add(1.0))
        assert not CountedSet.from_iterable([1, 1, 2]).content_equals(CountedSet.from_iterable([1, 2, 2]))
        assert not CountedSet.from_iterable([1, 1]).content_equals(CountedSet.from_iterable([1]))

    def test_atom_members(self):
        context = DirectContext(MemoryStorage())
        obj = DBObject(context=context)
        s = Set(context=context).add(obj)
        assert reload(s, context).content_equals(Set().add(obj)) and not s.content_equals(Set().add(DBObject()))


class TestRepeatedKeysDictionary:
    def test_rkd(self):
        pairs = [("k", 1), ("k", 2), ("k", 1), ("j", "x")]
        a = RepeatedKeysDictionary.from_items(pairs)
        assert a.content_equals(fold(RepeatedKeysDictionary(), "set_at", reversed(pairs)))
        assert reload(a).content_equals(a)
        assert not a.content_equals(a.remove_record("k", 1))
        assert not a.content_equals(a.remove_record("k", 1).set_at("k", 2))  # same count, other records
        assert not a.content_equals(a.remove_at("j").set_at("i", "x"))
