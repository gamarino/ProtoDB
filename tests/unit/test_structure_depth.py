"""Value nesting depth inside collections and indexes: refused at the call, never at commit (B12, D60).

``MAX_VALUE_DEPTH`` applies to every attribute value of every record, so a structure that stores
an application's value inside more containers (a hash bucket, a CountedSet entry, an index's
member key, a surrogate) can store only a shallower value (format spec, "Value nesting depth",
"Every attribute value, internal nodes included").  Catches: a mutator that accepts a value its
node cannot encode, so the whole transaction fails at commit under an internal attribute name
(``'key'``, ``'value'``); an effective limit that disagrees with what the encoder accepts (each
boundary below is committed through a FileStorage, so the encoder is the judge); an error that
does not name the collection, root, mutable cell or index; a transaction left unusable by the
refusal; an index added later over data it cannot store failing at commit.

Every limit is computed from the wrapping constants next to the code that builds the shapes
(``ELEMENT_WRAPPING``, ``ENTRY_WRAPPING``, ``MEMBER_KEY_WRAPPING``, ``STORE_KEY_WRAPPING``, the
surrogate's per-level costs), never written as a number here.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from itertools import count

import pytest

from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoValidationError
from proto_db.indexing import KeySpec
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.keyed import (MEMBER_KEY_WRAPPING, SURROGATE_LEAF, SURROGATE_LIST_LEVEL, SURROGATE_MAP_LEVEL,
                                     element_depth_limits, member_key)
from proto_db.indexing.range import RangeIndex
from proto_db.indexing.vector import STORE_KEY_WRAPPING, VectorIndex
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from proto_db import values
from proto_db.values import MAX_VALUE_DEPTH, canonical, value_depth

#: Scalars by the containers their wire form nests (``leaf_containers``): 0 to 3.
LEAVES = (1, Decimal("1.5"), date(2026, 1, 2), datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc))


def nest(n: int, leaf=1, kind: str = "list"):
    """``leaf`` inside ``n`` containers of ``kind``, built without recursion."""
    value = leaf
    for _ in range(n):
        value = [value] if kind == "list" else (value,) if kind == "tuple" else {"k": value}
    return value


def tuples_at(depth: int):
    """An orderable value (nested tuples) whose wire form nests exactly ``depth`` containers."""
    n, extra = divmod(depth, 2)
    value = nest(n, LEAVES[extra], "tuple")
    assert value_depth(value) == depth
    return value


def lists_with_member_key_at(depth: int, wrapping: int = MEMBER_KEY_WRAPPING, outer: tuple = ()):
    """An unorderable element (nested lists, inside the map levels ``outer`` names) whose member key,
    inside ``wrapping`` containers, nests exactly ``depth``: each list level costs
    ``SURROGATE_LIST_LEVEL`` in the surrogate, each map level ``SURROGATE_MAP_LEVEL``, the leaf
    ``SURROGATE_LEAF`` plus its own containers."""
    room = depth - wrapping - SURROGATE_LEAF - SURROGATE_MAP_LEVEL * len(outer)
    n, extra = divmod(room, SURROGATE_LIST_LEVEL)
    value = nest(n, LEAVES[extra])
    for fields in reversed(outer):
        value = dict(fields, deep=value)
    assert value_depth(member_key(canonical(value))) + wrapping - MEMBER_KEY_WRAPPING == depth
    return value


_DATABASES = count()


@pytest.fixture
def space(tmp_path):
    space = ObjectSpace(FileStorage(str(tmp_path)))
    yield space
    space.close()


def commit(space, name: str, value):
    """Store ``value`` as root ``name`` of a new database and commit it through the encoder;
    return what reads back."""
    db = space.new_database(f"{name}{next(_DATABASES)}")
    tr = db.new_transaction()
    tr.set_root_object(name, value)
    tr.commit()
    return db.new_transaction().get_root_object(name)


def refused(build, *patterns: str) -> str:
    """Run ``build()``, which must raise ``ProtoValidationError`` mentioning every pattern."""
    with pytest.raises(ProtoValidationError) as info:
        build()
    message = str(info.value)
    for pattern in patterns:
        assert pattern in message, (pattern, message)
    return message


# -- collections whose nodes wrap the value ---------------------------------------------------

def _set_add(value):
    return Set().add(value)


def _set_from(value):
    return Set.from_iterable([value])


def _counted_add(value):
    return CountedSet().add(value)


def _counted_from(value):
    return CountedSet.from_iterable([value, value])


def _hash_value(value):
    return HashDictionary().set_at("k", value)


def _hash_key(value):
    return HashDictionary().set_at(value, 1)


def _hash_from(value):
    return HashDictionary.from_items([("k", value)])


def _repeated_record(value):
    return RepeatedKeysDictionary().set_at("k", value)


def _repeated_from(value):
    return RepeatedKeysDictionary.from_items([("k", value)])


COLLECTIONS = [
    ("Set.add", _set_add, Set.ELEMENT_WRAPPING, "Set element"),
    ("Set.from_iterable", _set_from, Set.ELEMENT_WRAPPING, "Set element"),
    ("CountedSet.add", _counted_add, CountedSet.ELEMENT_WRAPPING, "CountedSet element"),
    ("CountedSet.from_iterable", _counted_from, CountedSet.ELEMENT_WRAPPING, "CountedSet element"),
    ("HashDictionary value", _hash_value, HashDictionary.ENTRY_WRAPPING, "HashDictionary value"),
    ("HashDictionary key", _hash_key, HashDictionary.ENTRY_WRAPPING, "HashDictionary key"),
    ("HashDictionary.from_items", _hash_from, HashDictionary.ENTRY_WRAPPING, "HashDictionary value"),
    ("RepeatedKeysDictionary record", _repeated_record, CountedSet.ELEMENT_WRAPPING, "RepeatedKeysDictionary record"),
    ("RepeatedKeysDictionary.from_items", _repeated_from, CountedSet.ELEMENT_WRAPPING,
     "RepeatedKeysDictionary record"),
]


@pytest.mark.parametrize("name, build, wrapping, holder", COLLECTIONS, ids=[c[0] for c in COLLECTIONS])
def test_a_collection_refuses_at_the_call_what_its_buckets_cannot_store(space, name, build, wrapping, holder):
    limit = MAX_VALUE_DEPTH - wrapping
    built = build(tuples_at(limit))
    node_value = built.root.value
    if isinstance(node_value, CountedSet):          # a RepeatedKeysDictionary: key -> CountedSet
        node_value = node_value.root.value
    assert value_depth(node_value) == MAX_VALUE_DEPTH  # the limit is tight: the bucket is at the limit
    stored = commit(space, "c", built)
    assert stored.count == 1
    message = refused(lambda: build(tuples_at(limit + 1)), holder, f"at most {limit}", f"adds {wrapping}",
                      "MAX_VALUE_DEPTH")
    assert f"{limit + 1} MessagePack containers" in message


def test_lists_and_dictionaries_add_no_containers(space):
    """The structures that store a value as a node attribute directly take the whole limit."""
    deepest, over = nest(MAX_VALUE_DEPTH), nest(MAX_VALUE_DEPTH + 1)
    assert commit(space, "l", List().append_last(deepest)).count == 1
    assert commit(space, "d", Dictionary().set_at(tuples_at(MAX_VALUE_DEPTH), deepest)).count == 1
    refused(lambda: List().append_last(over), "List element: ", "MAX_VALUE_DEPTH")
    refused(lambda: List.from_iterable([over]), "List element: ")
    refused(lambda: List().extend([1, over]), "List element: ")
    refused(lambda: List().append_last(1).set_at(0, over), "List element: ")
    refused(lambda: Dictionary().set_at("k", over), "Dictionary value: ")
    refused(lambda: Dictionary().set_at(tuples_at(MAX_VALUE_DEPTH + 1), 1), "Dictionary key: ")
    refused(lambda: Dictionary.from_items([("k", over)]), "Dictionary value: ")
    refused(lambda: RepeatedKeysDictionary().set_at(tuples_at(MAX_VALUE_DEPTH + 1), 1),
            "RepeatedKeysDictionary key: ")


def test_roots_and_mutable_cells_are_named(space):
    over = nest(MAX_VALUE_DEPTH + 1)
    tr = space.new_database("d").new_transaction()
    refused(lambda: tr.set_root_object("r", over), "Root 'r': ", "MAX_VALUE_DEPTH")
    refused(lambda: tr.new_mutable("m", over), "Mutable 'm': ", "MAX_VALUE_DEPTH")
    tr.new_mutable("m", 1)
    refused(lambda: tr.set_mutable("m", over), "Mutable 'm': ")
    tr.set_root_object("r", nest(MAX_VALUE_DEPTH))
    tr.commit()


# -- indexes: the member key wraps the element --------------------------------------------------

def _indexed(index):
    return List().add_index(index)


def test_an_orderable_element_of_an_index_may_nest_the_limit_less_the_member_key(space):
    limit = element_depth_limits()["orderable"]
    assert limit == MAX_VALUE_DEPTH - MEMBER_KEY_WRAPPING
    assert value_depth(member_key(canonical(tuples_at(limit)))) == MAX_VALUE_DEPTH
    stored = commit(space, "l", _indexed(EqualityIndex("e", KeySpec.element())).append_last(tuples_at(limit)))
    assert stored.count == 1
    message = refused(lambda: _indexed(EqualityIndex("e", KeySpec.element())).append_last(tuples_at(limit + 1)),
                      "Index 'e' (EqualityIndex on", f"at most {limit}", "MAX_VALUE_DEPTH")
    assert "member key would nest" in message


@pytest.mark.parametrize("kind", ["lists", "maps"])
def test_an_unorderable_element_nests_through_its_surrogate(space, kind):
    """The reviewer's case: an index over unorderable elements stores about 254 nested lists, or
    169 nested maps (format spec table), not 1022."""
    limits = element_depth_limits()
    n = limits[kind]
    shape = "list" if kind == "lists" else "map"
    per_level = SURROGATE_LIST_LEVEL if kind == "lists" else SURROGATE_MAP_LEVEL
    assert n == (MAX_VALUE_DEPTH - MEMBER_KEY_WRAPPING - SURROGATE_LEAF) // per_level
    stored = commit(space, "l", _indexed(EqualityIndex("e", KeySpec.element())).append_last(nest(n, 1, shape)))
    assert stored.count == 1
    refused(lambda: _indexed(EqualityIndex("e", KeySpec.element())).append_last(nest(n + 1, 1, shape)),
            "Index 'e'", f"at most {limits['lists']} nested lists or {limits['maps']} nested maps")


def test_the_member_key_limit_is_exact(space):
    """At every leaf width: a member key of exactly MAX_VALUE_DEPTH commits, one more is refused."""
    for extra in range(SURROGATE_LIST_LEVEL):
        depth = MAX_VALUE_DEPTH - extra
        assert commit(space, "l", _indexed(EqualityIndex("e", KeySpec.element()))
                      .append_last(lists_with_member_key_at(depth))).count == 1
    for extra in range(SURROGATE_LIST_LEVEL):
        over = lists_with_member_key_at(MAX_VALUE_DEPTH + 1 + extra)
        refused(lambda: _indexed(EqualityIndex("e", KeySpec.element())).append_last(over),
                f"would nest {MAX_VALUE_DEPTH + 1 + extra} MessagePack containers")


def test_a_field_index_over_plain_maps_stores_the_maps_through_their_surrogates(space):
    """The pattern of ``test_query_cache``: a List of dicts with a RangeIndex on one field; the
    dict elements are unorderable, so a deep second field counts through the surrogate."""
    at = lists_with_member_key_at(MAX_VALUE_DEPTH, outer=({"k": 1},))
    over = lists_with_member_key_at(MAX_VALUE_DEPTH + 1, outer=({"k": 1},))
    assert commit(space, "l", _indexed(RangeIndex("k", "k")).append_last(at)).count == 1
    refused(lambda: _indexed(RangeIndex("k", "k")).extend([{"k": 0, "v": 1}, over]),
            "Index 'k' (RangeIndex on k)", "member key would nest")


def test_a_vector_index_store_key_adds_its_own_tuple(space):
    """A member with a valid vector is stored under ``(list number, member key)``; an invalid one
    in the ``invalid`` group, under its member key alone."""
    vector = {"v": (1.0, 0.0)}
    wrapping = MEMBER_KEY_WRAPPING + STORE_KEY_WRAPPING
    at = lists_with_member_key_at(MAX_VALUE_DEPTH, wrapping, outer=(vector,))
    over = lists_with_member_key_at(MAX_VALUE_DEPTH + 1, wrapping, outer=(vector,))
    assert commit(space, "v", _indexed(VectorIndex("near", "v", 2, backend="exact")).append_last(at)).count == 1
    refused(lambda: _indexed(VectorIndex("near", "v", 2, backend="exact")).append_last(over),
            "Index 'near' (VectorIndex on v)", f"at most {element_depth_limits(wrapping)['lists']} nested lists")
    invalid_at = lists_with_member_key_at(MAX_VALUE_DEPTH, outer=({"v": None},))
    invalid_over = lists_with_member_key_at(MAX_VALUE_DEPTH + 1, outer=({"v": None},))
    assert commit(space, "w", _indexed(VectorIndex("near", "v", 2, backend="exact"))
                  .append_last(invalid_at)).count == 1
    refused(lambda: _indexed(VectorIndex("near", "v", 2, backend="exact")).append_last(invalid_over),
            "Index 'near'")


def test_an_index_added_over_existing_data_refuses_the_same_way(space):
    """``add_index`` bulk-builds the index from the elements already held: an element its member
    keys cannot store is refused there, with the same error, and the transaction goes on."""
    n = element_depth_limits()["lists"]
    db = space.new_database("d")
    tr = db.new_transaction()
    tr.set_root_object("l", List().extend([1, nest(n + 1)]))
    tr.commit()
    tr = db.new_transaction()
    held = tr.get_root_object("l")
    for index in (EqualityIndex("e", KeySpec.element()), VectorIndex("near", KeySpec.element(), 2,
                                                                       backend="exact")):
        refused(lambda: held.add_index(index), f"Index {index.name!r}", "member key would nest",
                f"at most {n} nested lists")
    tr.set_root_object("l", held.remove_at(1).add_index(EqualityIndex("e", KeySpec.element())))
    tr.set_root_object("other", DBObject(x=1))
    tr.commit()
    tr = db.new_transaction()
    assert tr.get_root_object("l").count == 1 and tr.get_root_object("other").x == 1


def test_a_refusal_leaves_the_transaction_usable(space):
    """The reviewer's reproduction: the refusal comes at ``append_last``, nothing has been written
    into the transaction, and its other work commits."""
    n = element_depth_limits()["lists"]
    db = space.new_database("d")
    tr = db.new_transaction()
    tr.set_root_object("l", _indexed(EqualityIndex("e", KeySpec.element())))
    tr.commit()
    tr = db.new_transaction()
    held = tr.get_root_object("l")
    refused(lambda: held.append_last(nest(n + 1)), "Index 'e'")
    refused(lambda: Set().add(tuples_at(MAX_VALUE_DEPTH)), "Set element")
    tr.set_root_object("l", held.append_last(nest(n)))
    tr.set_root_object("other", DBObject(x=1))
    tr.commit()
    tr = db.new_transaction()
    assert tr.get_root_object("l").count == 1 and tr.get_root_object("other").x == 1


# -- the msgpack floor ------------------------------------------------------------------------------

def test_an_older_msgpack_is_refused_with_a_clear_error():
    """msgpack before 1.2.0 packs at most 511 containers: ProtoBase refuses to run on it rather
    than accept values it then cannot commit (a bare ``ValueError``)."""
    values.check_msgpack(values.MSGPACK_MINIMUM)
    values.check_msgpack((1, 2, 4))
    for old in ((1, 1, 2), (1, 0, 8), (0, 6, 2)):
        with pytest.raises(ProtoNotSupportedError, match=r"msgpack 1\.2\.0 or later.*MAX_VALUE_DEPTH"):
            values.check_msgpack(old)
