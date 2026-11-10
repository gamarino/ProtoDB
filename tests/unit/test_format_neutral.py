"""The storage format is language-neutral by construction (ruling R129, decision D58).

Catches: an exact token, order key or hash that depends on how a Python object was built (time
zone object identity, subclass names, ``repr``) or on the reader's time-zone database; an index
removal that silently takes "the first equal member" (review C1); a ``@map`` whose keys collapse
silently (review I5); a non-canonical text form that decodes to a value whose bytes differ; a
built-in atom missing a required field that surfaces lazily as ``AttributeError`` (review I6); an
index written with format-3 member tokens that is changed in place instead of rebuilt.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from unittest import mock
from zoneinfo import ZoneInfo

import msgpack
import pytest

from proto_db.collections.lists import List
from proto_db.core import codec
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoValidationError
from proto_db.indexing import EqualityIndex, RangeIndex, VectorIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.query.terms import EQ, Term
from proto_db.storage import format as fmt
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import FrozenDict, canonical, exact_token, hash_key, order_key
from tests.support.persistence import DirectContext


def _wire(value) -> bytes:
    return msgpack.packb(codec.encode_value(value, lambda a: a.atom_pointer), use_bin_type=True)


def _roundtrip(value):
    return codec.decode_value(msgpack.unpackb(_wire(value), raw=False), lambda p, t: None)


def _decode(data):
    return codec.decode_value(msgpack.unpackb(msgpack.packb(data, use_bin_type=True), raw=False),
                              lambda p, t: None)


#: The application's UTC (named explicitly) and the same instant at -03:00 (review C1).
UTC_NAMED = datetime(2026, 1, 1, tzinfo=timezone(timedelta(0), "UTC"))
MINUS_3 = datetime(2025, 12, 31, 21, tzinfo=timezone(timedelta(hours=-3)))


# -- exact tokens: a function of the canonical bytes (C1) -------------------------------------

class _Int(int):
    pass


@pytest.mark.parametrize("built, decoded_like", [
    (UTC_NAMED, datetime(2026, 1, 1, tzinfo=timezone.utc)),
    (datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1), "UTC+01:00")),
     datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1)))),
    (_Int(5), 5),
    ((_Int(1), "a"), (1, "a")),
])
def test_values_with_the_same_canonical_bytes_have_the_same_token(built, decoded_like):
    assert _wire(built) == _wire(decoded_like)
    assert exact_token(built) == exact_token(decoded_like) == exact_token(_roundtrip(built))


@pytest.mark.parametrize("value", [None, True, 1, 1.0, -0.0, Decimal("1.00"), "a", b"a", date(2026, 1, 1),
                                   time(12, 0, 0, 5), UTC_NAMED, timedelta(1, 2, 3), uuid.UUID(int=7),
                                   (1, "a"), [1, 2], {"k": 1}, float("nan")])
def test_the_token_of_a_plain_value_is_its_canonical_encoding(value):
    assert exact_token(canonical(value)) == _wire(canonical(value))


def test_values_equal_as_keys_but_not_identical_have_distinct_tokens():
    tokens = {exact_token(v) for v in (1, 1.0, Decimal("1"), Decimal("1.00"), 0.0, -0.0, UTC_NAMED, MINUS_3)}
    assert len(tokens) == 8


def test_every_atom_has_the_atom_token_and_nested_atoms_are_placeholders():
    a, b = DBObject(x=1), DBObject(x=2)
    assert exact_token(a) == exact_token(b) == b"\x81\xa4@ref\xc0"
    assert exact_token((a, 1)) == exact_token((b, 1)) == msgpack.packb({"@tup": [{"@ref": None}, 1]})
    # With an atom_token callback the atoms are told apart (read sets, the query cache).
    assert exact_token((a, 1), lambda x: id(x)) != exact_token((b, 1), lambda x: id(x))


# -- removal: the exact member, never "the first equal member" (C1) ---------------------------

def test_removing_a_decoded_value_removes_that_member_not_an_equal_one():
    index = EqualityIndex("e", KeySpec.element()).empty().build([UTC_NAMED, MINUS_3])
    after = index.with_removed(_roundtrip(UTC_NAMED))
    rows = list(after.plan(Term(None, EQ, UTC_NAMED)).execute())
    assert [r.isoformat() for r in rows] == [MINUS_3.isoformat()]
    after.validate([MINUS_3])


@pytest.mark.parametrize("make", [
    lambda: EqualityIndex("e", KeySpec.element()),
    lambda: RangeIndex("e", KeySpec.element()),
])
def test_removing_an_equal_but_different_value_is_refused(make):
    index = make().empty().build([1, Decimal("1.00")])
    with pytest.raises(ProtoValidationError, match="does not hold"):
        index.with_removed(1.0)
    assert list(index.with_removed(Decimal("1.00")).plan(Term(None, EQ, 1)).execute()) == [1]


def test_vector_removal_is_exact_too():
    index = VectorIndex("v", KeySpec.element(), dim=2, metric="l2").empty().build([(1, 0), (1.0, 0)])
    with pytest.raises(ProtoValidationError, match="does not hold"):
        index.with_removed((Decimal(1), 0))
    index.with_removed((1.0, 0)).validate([(1, 0)])


def test_validate_reports_a_member_under_another_token():
    index = EqualityIndex("e", KeySpec.element()).empty().build([1])
    # The member of 1 stored as if it were 1.0: same group, same equality part, wrong token.
    from proto_db.collections import avl
    from proto_db.collections.avl import CountedKeyNode
    members = avl.key_put(None, ((0, 1), exact_token(1.0)), (1, 1), node_class=CountedKeyNode)[0]
    groups = avl.key_put(None, 1, (members, 1), node_class=CountedKeyNode)[0]
    broken = index._with(groups=groups)
    with pytest.raises(ProtoCorruptionError, match="wrong member key"):
        broken.validate([1])


def test_the_reviewers_end_to_end_repro(tmp_path):
    path = str(tmp_path / "db")
    space = ObjectSpace(FileStorage(path))
    db = space.new_database("d")
    with db.new_transaction() as tr:
        tr.set_root_object("l", tr.new_list().add_index(EqualityIndex("e", KeySpec.element()))
                           .append_last(UTC_NAMED).append_last(MINUS_3))
    space.close()
    space = ObjectSpace(FileStorage(path))
    db = space.open_database("d")
    with db.new_transaction() as tr:
        tr.set_root_object("l", tr.get_root_object("l").remove_at(0))
    space.close()
    space = ObjectSpace(FileStorage(path))
    try:
        tr = space.open_database("d").new_transaction()
        lst = tr.get_root_object("l")
        scan = [x.isoformat() for x in lst.elements()]
        found = [x.isoformat() for x in lst.get_index("e").plan(Term(None, EQ, UTC_NAMED)).execute()]
        assert scan == found == [MINUS_3.isoformat()]
        lst.get_index("e").validate(list(lst.elements()))
    finally:
        space.close()


# -- zone datetimes: keyed by the UTC instant in the bytes, never by the tz database (I9) --------

NY = ZoneInfo("America/New_York")


def test_a_zone_datetime_is_written_as_its_utc_instant():
    value = datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=NY)          # the second 01:30: 06:30 UTC
    assert codec.encode_value(value, None) == {"@dt": [2026, 11, 1, 6, 30, 0, 0, 0, ["instant", "America/New_York"]]}
    back = _roundtrip(value)
    assert back == value and back.tzinfo is NY and (back.hour, back.fold) == (1, 1)
    assert order_key(back) == order_key(value) and hash_key(back) == hash_key(value)


def test_order_and_hash_of_a_zone_datetime_do_not_depend_on_the_readers_tz_database():
    value = datetime(2030, 6, 1, 12, tzinfo=NY)
    data = _wire(value)
    real = ZoneInfo

    def other_rules(key):          # a reader whose tz database gives New York other rules
        return real("Asia/Tokyo") if key == "America/New_York" else real(key)
    with mock.patch.object(codec, "ZoneInfo", other_rules):
        back = codec.decode_value(msgpack.unpackb(data, raw=False), None)
    assert order_key(back) == order_key(value) and hash_key(back) == hash_key(value)
    assert codec.encode_value(back, None)["@dt"][:8] == codec.encode_value(value, None)["@dt"][:8]


def test_the_format_3_zone_form_is_still_read_as_a_wall_time():
    back = _decode({"@dt": [2026, 11, 1, 1, 30, 0, 0, 1, ["zone", "America/New_York"]]})
    assert back == datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=NY) and back.fold == 1
    assert codec.encode_value(back, None)["@dt"][-1] == ["instant", "America/New_York"]


@pytest.mark.parametrize("body", [
    [2026, 1, 1, 0, 0, 0, 0, 1, ["instant", "America/New_York"]],      # fold must be 0
    [2026, 1, 1, 0, 0, 0, 0, 0, ["instant", "No/Such_Zone"]],
])
def test_malformed_instant_forms_are_refused(body):
    with pytest.raises(ProtoCorruptionError):
        _decode({"@dt": body})


def test_a_zone_datetime_whose_instant_is_out_of_range_cannot_be_written():
    with pytest.raises(ProtoValidationError):
        codec.encode_value(datetime(1, 1, 1, tzinfo=ZoneInfo("Asia/Tokyo")), None)


def test_a_zone_time_orders_and_hashes_as_its_wall_clock():
    # Normative rule (I1): a time has no date, so a zone gives it no offset: rank 7, as naive.
    assert order_key(time(12, tzinfo=NY)) == order_key(time(12)) == (7, 12 * 3600 * 10 ** 6)
    assert hash_key(time(12, tzinfo=NY)) == hash_key(time(12))
    assert exact_token(time(12, tzinfo=NY)) != exact_token(time(12))


# -- @map keys: distinct exactly when their canonical values are distinct (I5) --------------------

@pytest.mark.parametrize("pairs", [
    [[1, "a"], [True, "b"]],
    [[1, "a"], [1.0, "b"]],
    [[1, "a"], [{"@dec": "1"}, "b"]],
    [[0.0, "a"], [-0.0, "b"]],
    [[{"@tup": [1]}, "a"], [{"@tup": [True]}, "b"]],
    [[{"@dt": [2026, 1, 1, 0, 0, 0, 0, 0, ["fixed", 0, "UTC"]]}, 1],
     [{"@dt": [2025, 12, 31, 21, 0, 0, 0, 0, ["fixed", -10800000000, "UTC-03:00"]]}, 2]],
    [["k", 1], ["k", 2]],
    [[float("nan"), 1], [float("nan"), 2]],
])
def test_a_map_whose_keys_conflict_is_refused_never_collapsed(pairs):
    with pytest.raises(ProtoCorruptionError, match="map key"):
        _decode({"@map": pairs})


@pytest.mark.parametrize("key", [[1], {"@map": []}, {"@tup": [[1]]}, {"@dec": "sNaN"}])
def test_list_map_and_signaling_nan_keys_are_refused(key):
    with pytest.raises(ProtoCorruptionError, match="map key"):
        _decode({"@map": [[key, 1]]})


def test_distinct_nan_payloads_and_bool_keys_without_numbers_are_kept():
    nan2 = bytes.fromhex("7ff8000000000002")
    value = _decode({"@map": [[float("nan"), 1], [msgpack.unpackb(b"\xcb" + nan2), 2], [True, 3], ["1", 4]]})
    assert len(value) == 4


def test_canonical_refuses_a_dict_whose_keys_conflict_in_the_format():
    nan = float("nan")
    with pytest.raises(ProtoValidationError, match="map key"):
        canonical({nan: 1, float("nan"): 2})
    ny = datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=NY)          # an ambiguous time: Python's
    utc = ny.astimezone(timezone.utc)                              # == says these differ
    with pytest.raises(ProtoValidationError, match="map key"):
        canonical({ny: 1, utc: 2})
    assert canonical({1: "a", "1": "b", b"1": "c"}) == {1: "a", "1": "b", b"1": "c"}


# -- canonical text forms: one spelling per value ----------------------------------------------

@pytest.mark.parametrize("data", [
    {"@dec": "1.0E+1"}, {"@dec": "1e3"}, {"@dec": "+1"}, {"@dec": " 1"}, {"@dec": "nan"}, {"@dec": "Inf"},
    {"@dec": "1_000"},
    {"@int": "0x10000000000000000"}, {"@int": "10000000000000000 "}, {"@int": "FFFFFFFFFFFFFFFFF"},
    {"@int": "+10000000000000000"}, {"@int": "ff"}, {"@int": "-0"},
    {"@td": [0, 86400, 0]}, {"@td": [0, 0, 1000000]}, {"@td": [0, -1, 0]},
])
def test_non_canonical_text_forms_are_refused(data):
    with pytest.raises(ProtoCorruptionError):
        _decode(data)


@pytest.mark.parametrize("text", ["1.20", "-1.5E+30", "-0", "NaN", "-NaN123", "sNaN", "-Infinity", "1E-7",
                                  "0E+2", "123456789012345678901234567890"])
def test_canonical_decimal_texts_are_read(text):
    assert str(_decode({"@dec": text})) == text


# -- required fields: absence is corruption at load (I6) ----------------------------------------

def test_a_builtin_atom_missing_a_required_field_is_corrupt_at_load():
    storage = MemoryStorage()
    ctx = DirectContext(storage)
    ptr = storage.push_atom({"t": 5, "oid": uuid.uuid4().bytes, "s": {"root": None}})     # List: no count
    with pytest.raises(ProtoCorruptionError, match="count"):
        ctx.load(ptr, "proto_db.collections.List").count


def test_optional_fields_may_be_absent():
    storage = MemoryStorage()
    ctx = DirectContext(storage)
    ptr = ctx.save(List.from_iterable([1, 2]))
    assert ctx.load(ptr, "proto_db.collections.List").count == 2


# -- format 4, and indexes written with format-3 tokens -------------------------------------------

def test_the_format_number_is_4_and_formats_2_to_4_read():
    assert fmt.FORMAT_VERSION == 4
    assert fmt.READABLE_FORMAT_VERSIONS == frozenset({2, 3, 4})


def _legacy(index):
    """``index`` as a format-3 writer left it: member tokens of the old Python scheme, no marker."""
    from proto_db.collections import avl
    from proto_db.collections.avl import CountedKeyNode
    groups = None
    for key, (members, total) in avl.key_iter(index.groups):
        old = None
        for (eq, _tok), (element, n) in avl.key_iter(members):
            old = avl.key_put(old, (eq, ("i", element)), (element, n), node_class=CountedKeyNode)[0]
        groups = avl.key_put(groups, key, (old, total), node_class=CountedKeyNode)[0]
    legacy = index._with(groups=groups)
    object.__delattr__(legacy, "member_tokens")
    return legacy


def test_an_index_with_format_3_tokens_is_rebuilt_at_its_first_change():
    lst = List.from_iterable([1, 2, 2]).add_index(EqualityIndex("e", KeySpec.element()))
    legacy = _legacy(lst.get_index("e"))
    assert not legacy.has_current_tokens()
    old = lst._with(indexes=lst.indexes.without("e").with_index(legacy))
    new = old.remove_at(1)
    index = new.get_index("e")
    assert index.has_current_tokens()
    index.validate(list(new.elements()))
    assert list(index.plan(Term(None, EQ, 2)).execute()) == [2]
