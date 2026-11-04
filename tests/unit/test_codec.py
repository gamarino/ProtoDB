"""Unit tests for proto_db.core.codec: exact round-trip of every canonical type (D3)."""
import os
import zoneinfo
import uuid
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo

import msgpack
import pytest

from proto_db.core import codec
from proto_db.core.atom import Atom, AtomPointer
from proto_db.errors import ProtoCorruptionError, ProtoValidationError
from proto_db.values import MISSING
from tests.support.exact import assert_exact


class CodecLeaf(Atom):
    """Plain atom used as a nested reference."""

    def __init__(self, label):
        super().__init__()
        self.label = label


class _Refs:
    """save_child / load_ref pair backed by a dict, with call recording."""

    def __init__(self):
        self.segment = uuid.uuid4()
        self.by_ptr = {}
        self.saved = []
        self.loaded = []

    def save_child(self, atom):
        self.saved.append(atom)
        ptr = AtomPointer(self.segment, len(self.by_ptr))
        self.by_ptr[ptr] = atom
        return ptr

    def load_ref(self, ptr, type_name):
        self.loaded.append((ptr, type_name))
        return self.by_ptr[ptr]


def roundtrip(value, refs=None):
    refs = refs or _Refs()
    encoded = codec.encode_state({"v": value}, refs.save_child)
    wire = msgpack.unpackb(msgpack.packb(encoded, use_bin_type=True), raw=False)
    return codec.decode_state(wire, refs.load_ref)["v"]


CASES = [
    None, True, False, 0, -1, 2**63 - 1, -2**63, 2**64 - 1, 2**64, -2**63 - 1, 10**40, -10**40,
    0.0, -0.0, 1.5, float("inf"), float("-inf"),
    Decimal("1.10"), Decimal("-0"), Decimal("1E+5"), Decimal("NaN"), Decimal("-Infinity"),
    Decimal("3.14159265358979323846264338327950288"),
    "", "hello", "ñandú € \U0001F600", b"", b"\x00\xff",
    date(2026, 11, 1), date(1, 1, 1), date(9999, 12, 31),
    time(0, 0), time(23, 59, 59, 999999), time(12, 30, tzinfo=timezone.utc),
    time(1, 2, 3, 4, tzinfo=timezone(timedelta(hours=-3), "ART")), time(1, 2, fold=1),
    datetime(2026, 11, 1, 10, 20, 30, 123456),
    datetime(2026, 11, 1, 10, 20, tzinfo=timezone.utc),
    datetime(2026, 11, 1, 10, 20, tzinfo=timezone(timedelta(hours=5, minutes=30))),
    datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=ZoneInfo("America/New_York")),
    datetime(2026, 3, 1, tzinfo=timezone(timedelta(seconds=1, microseconds=7))),
    timedelta(days=2, seconds=5), timedelta(0), timedelta(days=-1, microseconds=1),
    timedelta.max, timedelta.min,
    uuid.UUID(int=0), uuid.uuid4(),
    MISSING,
    (), (1, "a", (2.5, None)), [], [1, [2, [3]]], {}, {"a": 1},
    {1: "int key", (1, 2): "tuple key", "s": [1, (2, {3: b"x"})], None: True},
    [{"k": (date(2020, 1, 1), Decimal("0.0"))}],
]


@pytest.mark.parametrize("value", CASES, ids=lambda v: f"{type(v).__name__}:{v!r}"[:60])
def test_roundtrip_exact(value):
    assert_exact(roundtrip(value), value)


def test_named_cases_from_brief():
    for value in (timedelta(days=2, seconds=5), date(2026, 1, 2),
                  datetime(2026, 1, 2, 3, 4, tzinfo=timezone(timedelta(hours=2))),
                  Decimal("1.10")):
        assert_exact(roundtrip(value), value)
    assert str(roundtrip(Decimal("1.10"))) == "1.10"


def test_bytes_up_to_one_mib():
    blob = bytes(range(256)) * 4096
    assert len(blob) == 1024 * 1024
    assert_exact(roundtrip(blob), blob)


@pytest.mark.parametrize("value", [10**5000, -10**5000, 2**70000 + 1, -(2**70000) - 1],
                         ids=["1e5000", "-1e5000", "2^70000+1", "-2^70000-1"])
def test_huge_int_roundtrip_beyond_str_digit_limit(value):
    assert_exact(roundtrip(value), value)


def test_huge_int_wire_form_is_hex():
    encoded = codec.encode_state({"v": -2**64 - 1}, _Refs().save_child)
    assert encoded["v"] == {"@int": "-10000000000000001"}


def test_bytearray_comes_back_as_bytes():
    assert_exact(roundtrip(bytearray(b"ab")), b"ab")


def test_strings_come_back_as_str():
    out = roundtrip(["x", ("y",), {"z": "w"}])
    assert type(out[0]) is str and type(out[1][0]) is str
    assert all(type(k) is str and type(v) is str for k, v in out[2].items())


def test_float_nan_roundtrips():
    out = roundtrip(float("nan"))
    assert type(out) is float and out != out


def test_utc_comes_back_as_singleton():
    assert roundtrip(datetime(2026, 1, 1, tzinfo=timezone.utc)).tzinfo is timezone.utc


def test_nested_atom_refs():
    refs = _Refs()
    a, b = CodecLeaf("a"), CodecLeaf("b")
    value = {"one": a, "many": [b, (a, {"deep": b})]}
    out = roundtrip(value, refs)
    assert out["one"] is a and out["many"][0] is b
    assert out["many"][1][0] is a and out["many"][1][1]["deep"] is b
    assert refs.saved == [a, b, a, b]
    assert {t for _, t in refs.loaded} == {CodecLeaf.__type_name__}


def test_ref_wire_shape():
    """Format 3 (D51): the segment as its 16 bytes, the type as its name (a user type)."""
    refs = _Refs()
    leaf = CodecLeaf("x")
    encoded = codec.encode_state({"r": leaf}, refs.save_child)
    segment, offset, type_name = encoded["r"]["@ref"]
    assert segment == refs.segment.bytes and offset == 0 and type_name == CodecLeaf.__type_name__


def test_ref_of_a_builtin_type_uses_its_alias():
    from proto_db.collections.avl import KeyNode
    from proto_db.core.atom import BUILTIN_TYPE_ALIASES, type_name_of, type_tag
    refs = _Refs()
    encoded = codec.encode_state({"r": KeyNode(1, 2)}, refs.save_child)
    tag = encoded["r"]["@ref"][2]
    assert type(tag) is int and BUILTIN_TYPE_ALIASES[tag] == KeyNode.__type_name__
    assert type_name_of(type_tag("my.Type")) == "my.Type"
    with pytest.raises(ProtoCorruptionError):
        type_name_of(len(BUILTIN_TYPE_ALIASES))


def test_a_format_2_ref_still_decodes():
    """Format 2 wrote ``[segment_hex, offset, type_name]``; it is still read."""
    refs = _Refs()
    leaf = CodecLeaf("x")
    ptr = refs.save_child(leaf)
    out = codec.decode_state({"r": {"@ref": [ptr.segment.hex, ptr.offset, CodecLeaf.__type_name__]}}, refs.load_ref)
    assert out["r"] is leaf and refs.loaded[-1] == (ptr, CodecLeaf.__type_name__)


def test_encoded_output_is_plain_msgpack_types():
    encoded = codec.encode_state({"v": CASES[:-1]}, _Refs().save_child)

    def walk(x):
        assert x is None or type(x) in (bool, int, float, str, bytes, list, dict), type(x)
        if isinstance(x, list):
            for y in x:
                walk(y)
        elif isinstance(x, dict):
            for k, y in x.items():
                assert type(k) is str
                walk(y)
    walk(encoded)


def test_unknown_tag_raises_corruption():
    with pytest.raises(ProtoCorruptionError):
        codec.decode_state({"v": {"@nope": 1}}, _Refs().load_ref)


@pytest.mark.parametrize("bad", [
    {"v": {"@dec": 1, "@uuid": b""}},
    {"v": {"plain": 1}},
    {"v": {"@uuid": b"short"}},
    {"v": {"@date": [2026, 13, 1]}},
    {"v": {"@td": "x"}},
    {"v": {"@ref": ["zz", 0, "T"]}},
    {"v": {"@ref": [b"short", 0, "T"]}},
    {"v": {"@ref": [b"0123456789abcdef", 0, 999]}},
    {"v": {"@ref": [b"0123456789abcdef", 0, 1.5]}},
    {"v": {"@dt": [2026, 1, 1, 0, 0, 0, 0, 0, ["bogus"]]}},
    {"v": {"@map": [[1]]}},
    {"v": {"@int": "xyz"}},
    {"v": {"@int": 12}},
    {"v": {"@dec": "not a number"}},
    {"v": object()},
    {1: 1},
    [],
])
def test_malformed_data_raises_corruption(bad):
    with pytest.raises(ProtoCorruptionError):
        codec.decode_state(bad, _Refs().load_ref)


class _CustomTz(tzinfo):
    """A tzinfo implementation the codec cannot persist."""

    def utcoffset(self, dt):
        return timedelta(hours=1)

    def tzname(self, dt):
        return "CUSTOM"

    def dst(self, dt):
        return timedelta(0)


@pytest.mark.parametrize("bad", [object(), {1, 2}, frozenset(), 1j,
                                 datetime(2026, 1, 1, tzinfo=_CustomTz()),
                                 time(1, tzinfo=_CustomTz())])
def test_unsupported_values_raise_validation(bad):
    with pytest.raises(ProtoValidationError):
        codec.encode_state({"v": bad}, _Refs().save_child)


def test_keyless_zoneinfo_raises_validation():
    path = next((os.path.join(d, "UTC") for d in zoneinfo.TZPATH
                 if os.path.isfile(os.path.join(d, "UTC"))), None)
    if path is None:
        pytest.skip("no system tz database file to build a keyless ZoneInfo from")
    with open(path, "rb") as fh:
        keyless = ZoneInfo.from_file(fh)
    with pytest.raises(ProtoValidationError):
        codec.encode_state({"v": datetime(2026, 1, 1, tzinfo=keyless)}, _Refs().save_child)


def test_non_str_state_key_raises_validation():
    with pytest.raises(ProtoValidationError):
        codec.encode_state({1: "x"}, _Refs().save_child)


def test_builtin_type_aliases_are_append_only():
    """Format 3 writes these indexes into every segment: an alias must keep naming its type
    (append new built-in types at the end, never reorder or remove)."""
    from proto_db.core.atom import BUILTIN_ATOM_MODULES, BUILTIN_TYPE_ALIASES
    pinned = ("proto_db.avl.KeyNode", "proto_db.avl.PosNode", "proto_db.avl.ValueBox",
              "proto_db.collections.IndexSet", "proto_db.collections.IndexedCollection",
              "proto_db.collections.List", "proto_db.collections.Dictionary",
              "proto_db.collections.RepeatedKeysDictionary", "proto_db.collections.HashDictionary",
              "proto_db.collections.sets._HashBuckets", "proto_db.collections.Set",
              "proto_db.collections.CountedSet", "proto_db.indexing.Index",
              "proto_db.indexing.keyed.KeyGroupIndex", "proto_db.indexing.EqualityIndex",
              "proto_db.indexing.RangeIndex", "proto_db.indexing.VectorIndex",
              "proto_db.transactions.SpaceRoot", "proto_db.transactions.DatabaseRoot",
              "proto_db.core.dbobject.DBObject", "proto_db.avl._Node")
    assert BUILTIN_TYPE_ALIASES[:len(pinned)] == pinned
    assert len(set(BUILTIN_TYPE_ALIASES)) == len(BUILTIN_TYPE_ALIASES)
    assert set(BUILTIN_ATOM_MODULES) <= set(BUILTIN_TYPE_ALIASES)


def test_subclasses_of_builtin_types_encode_as_their_base_type():
    """The encoder's exact-type fast paths and its per-class memo (Task 8.3i) must give every
    value the same wire form as the full isinstance chain: a subclass of a built-in type is
    written as its base type, an atom as a reference, whatever order the classes are met in."""
    import enum

    class Color(enum.IntEnum):
        RED = 3

    class Name(str):
        pass

    class Raw(bytes):
        pass

    refs = _Refs()
    leaf = CodecLeaf("x")
    for _ in range(2):            # the second pass goes through the memo
        encoded = codec.encode_state({"i": Color.RED, "s": Name("n"), "b": Raw(b"r"), "t": True,
                                      "a": leaf, "m": MISSING, "big": 2**70}, refs.save_child)
        assert type(encoded["i"]) is int and encoded["i"] == 3
        assert type(encoded["s"]) is str and encoded["s"] == "n"
        assert type(encoded["b"]) is bytes and encoded["b"] == b"r"
        assert encoded["t"] is True and encoded["m"] == {"@missing": 0}
        assert set(encoded["a"]) == {"@ref"} and encoded["big"] == {"@int": format(2**70, "x")}
