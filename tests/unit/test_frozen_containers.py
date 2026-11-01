"""Tests for frozen list/dict values (ruling R13, decision D25)."""
import copy
import pickle

import pytest

from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.lists import List
from proto_db.core.codec import decode_value, encode_value
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import FrozenDict, FrozenList, canonical, hash_key
from tests.support.persistence import DirectContext

LIST_MUTATORS = [
    lambda x: x.append(1), lambda x: x.extend([1]), lambda x: x.insert(0, 1), lambda x: x.pop(),
    lambda x: x.remove(1), lambda x: x.clear(), lambda x: x.sort(), lambda x: x.reverse(),
    lambda x: x.__setitem__(0, 9), lambda x: x.__delitem__(0), lambda x: x.__setitem__(slice(0, 1), [5]),
    lambda x: x.__iadd__([1]), lambda x: x.__imul__(2),
]
DICT_MUTATORS = [
    lambda x: x.update(a=1), lambda x: x.setdefault("z", 1), lambda x: x.popitem(),
    lambda x: x.pop("a"), lambda x: x.clear(), lambda x: x.__setitem__("a", 2),
    lambda x: x.__delitem__("a"), lambda x: x.__ior__({"b": 1}),
]


@pytest.mark.parametrize("op", LIST_MUTATORS)
def test_list_mutators_raise(op):
    fl = FrozenList([1, 2])
    with pytest.raises(ProtoValidationError, match="copy"):
        op(fl)
    assert fl == [1, 2]


@pytest.mark.parametrize("op", DICT_MUTATORS)
def test_dict_mutators_raise(op):
    fd = FrozenDict({"a": 1})
    with pytest.raises(ProtoValidationError, match="copy"):
        op(fd)
    assert fd == {"a": 1}


def test_reinit_is_noop():
    fl, fd = FrozenList([1]), FrozenDict(a=1)
    fl.__init__([7]), fd.__init__({"q": 1})
    assert fl == [1] and fd == {"a": 1}


def test_equality_hash_copy_pickle():
    assert FrozenList([1]) == [1] and [1] == FrozenList([1])
    assert FrozenDict(a=1) == {"a": 1}
    with pytest.raises(TypeError):
        hash(FrozenList())
    with pytest.raises(TypeError):
        hash(FrozenDict())
    c = FrozenList([1, FrozenList([2])]).copy()
    assert type(c) is list and type(c[1]) is FrozenList
    assert type(FrozenDict(a=1).copy()) is dict
    for v in (FrozenList([1, FrozenDict(a=FrozenList([2]))]), FrozenDict(a=FrozenList([1]))):
        for r in (copy.deepcopy(v), pickle.loads(pickle.dumps(v)), copy.copy(v)):
            assert r == v and type(r) is type(v)
    assert type(copy.deepcopy(FrozenList([FrozenList()]))[0]) is FrozenList


def test_canonical_nested_frozen_and_idempotent():
    v = canonical({"a": [1, {"b": [2, (3, [4])]}]})
    assert type(v) is FrozenDict and type(v["a"]) is FrozenList and type(v["a"][1]) is FrozenDict
    assert type(v["a"][1]["b"]) is FrozenList and type(v["a"][1]["b"][1][1]) is FrozenList
    assert canonical(v) is v
    plain = [1]
    assert canonical(plain) is not plain and type(canonical(plain)) is FrozenList
    assert hash_key((1, 2)) == hash_key((1, 2))


def test_codec_roundtrip_frozen():
    for value in ([1, {"a": [2]}], {"k": [1, 2], "t": ({"x": 1},)}):
        enc = encode_value(canonical(value), None)
        assert enc == encode_value(value, None)
        dec = decode_value(enc, None)
        assert dec == value
        assert type(dec) in (FrozenList, FrozenDict)
    dec = decode_value(encode_value([1, {"a": [2]}], None), None)
    assert type(dec[1]) is FrozenDict and type(dec[1]["a"]) is FrozenList


def reload(coll):
    ctx = DirectContext(MemoryStorage())
    return ctx.load(ctx.save(coll), type(coll).__type_name__)


@pytest.mark.parametrize("reloaded", [False, True])
def test_dictionary_value_cannot_be_mutated(reloaded):
    d = Dictionary().set_at("k", {"a": [1, 2]})
    if reloaded:
        d = reload(d)
    v = d.get_at("k")
    assert type(v) is FrozenDict
    with pytest.raises(ProtoValidationError):
        v["a"] = 1
    with pytest.raises(ProtoValidationError):
        v["a"].append(3)
    assert d.get_at("k") == {"a": [1, 2]}


@pytest.mark.parametrize("reloaded", [False, True])
def test_list_element_cannot_be_mutated(reloaded):
    lst = List.from_iterable([[1], {"a": 1}])
    if reloaded:
        lst = reload(lst)
    with pytest.raises(ProtoValidationError):
        lst.get_at(0).append(2)
    with pytest.raises(ProtoValidationError):
        lst.get_at(1)["b"] = 2
    assert lst.get_at(0) == [1] and lst.get_at(1) == {"a": 1}


@pytest.mark.parametrize("reloaded", [False, True])
def test_dbobject_field_cannot_be_mutated(reloaded):
    o = DBObject(tags=["a"], meta={"x": [1]})
    if reloaded:
        o = reload(o)
    with pytest.raises(ProtoValidationError):
        o.tags.append("b")
    with pytest.raises(ProtoValidationError):
        o.meta["x"].append(2)
    assert o.tags == ["a"] and o.meta == {"x": [1]}
