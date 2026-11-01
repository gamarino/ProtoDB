"""Unit tests for proto_db.indexing.keyspec (spec section 8.3, Task 4.1).

Catches: wrong key extraction (absent attributes or mapping keys not mapped to
MISSING, explicit None confused with absence, dotted paths not descending through
records and mappings), key specs that cannot be persisted as plain data or that
compare by identity, invalid paths accepted, and named extractors that are
silently ignored, silently redefined or fail late without naming the extractor.
"""
import dataclasses

import pytest

from proto_db.collections.lists import List
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoValidationError
from proto_db.indexing import KeySpec, register_extractor
from proto_db.storage.memory import MemoryStorage
from proto_db.values import MISSING, FrozenDict, FrozenList
from tests.support.persistence import DirectContext


def test_path_reads_record_attributes_and_absent_is_missing():
    spec = KeySpec.path("city")
    assert spec.extract(DBObject(city="Rome")) == "Rome"
    assert spec.extract(DBObject(city=None)) is None
    assert spec.extract(DBObject(zip=1)) is MISSING


def test_dotted_path_through_records_and_mappings():
    spec = KeySpec.path("address.zip")
    assert spec.extract(DBObject(address=DBObject(zip=1000))) == 1000
    assert spec.extract(DBObject(address={"zip": 2000})) == 2000  # stored as FrozenDict
    assert spec.extract({"address": {"zip": 3000}}) == 3000  # plain mappings too
    assert spec.extract(FrozenDict({"address": FrozenDict({"zip": None})})) is None
    assert spec.extract(DBObject(address={"street": "x"})) is MISSING
    assert spec.extract(DBObject(address=None)) is MISSING
    assert spec.extract(DBObject(address=5)) is MISSING
    assert spec.extract(DBObject()) is MISSING
    assert spec.extract(7) is MISSING
    assert spec.extract(None) is MISSING


def test_private_and_reserved_names_are_never_read():
    assert KeySpec.path("context").extract(DBObject(a=1)) is MISSING
    assert KeySpec.path("oid").extract(DBObject(a=1)) is MISSING
    assert KeySpec.path("_loaded").extract(DBObject(a=1)) is MISSING
    assert KeySpec.path("_x").extract({"_x": 1}) == 1  # mapping keys are data, not attributes


def test_path_reads_persisted_attributes_of_other_atoms_only():
    lst = List().append_last(1).append_last(2)
    assert KeySpec.path("count").extract(lst) == 2
    assert KeySpec.path("elements").extract(lst) is MISSING  # a method, not persisted state


def test_path_extraction_loads_lazy_stubs():
    context = DirectContext(MemoryStorage())
    rec = DBObject(address=DBObject(zip=42), city="Oslo")
    stub = context.load(context.save(rec), DBObject.__type_name__)
    assert KeySpec.path("address.zip").extract(stub) == 42
    assert KeySpec.path("city").extract(stub) == "Oslo"
    assert KeySpec.path("zip").extract(stub) is MISSING


def test_element_returns_the_element_itself():
    rec = DBObject(a=1)
    spec = KeySpec.element()
    assert spec.extract(rec) is rec
    assert spec.extract(5) == 5
    assert spec.extract(MISSING) is MISSING


@pytest.mark.parametrize("bad", ["", "a..b", ".a", "a.", "@x", "a. b", " a", 3, None, b"a"])
def test_invalid_paths_are_rejected(bad):
    with pytest.raises(ProtoValidationError):
        KeySpec.path(bad)


def _city(element):
    return KeySpec.path("city").extract(element)


def _as_list(element):
    return [element, element]


def _bad_value(element):
    return object()


# Registered once at import: a name keeps its function for the life of the process.
register_extractor("tests.keyspec.upper_city", _city)
register_extractor("tests.keyspec.as_list", _as_list)
register_extractor("tests.keyspec.bad_value", _bad_value)


def test_named_extractor_registry():
    spec = KeySpec.extractor("tests.keyspec.upper_city")
    assert spec.extract(DBObject(city="x")) == "x"
    assert spec.extract(DBObject()) is MISSING


def test_extractor_results_are_canonical():
    value = KeySpec.extractor("tests.keyspec.as_list").extract(1)
    assert type(value) is FrozenList and value == [1, 1]
    with pytest.raises(ProtoValidationError):
        KeySpec.extractor("tests.keyspec.bad_value").extract(1)


def test_unregistered_extractor_fails_at_use_with_a_clear_message():
    spec = KeySpec.extractor("tests.keyspec.never_registered")  # constructing it is fine
    with pytest.raises(ProtoValidationError, match="tests.keyspec.never_registered"):
        spec.extract(DBObject())


def _first(element):
    return 1


def test_extractor_names_cannot_be_redefined():
    register_extractor("tests.keyspec.stable", _first)
    register_extractor("tests.keyspec.stable", _first)  # same function: idempotent
    with pytest.raises(ProtoValidationError):
        register_extractor("tests.keyspec.stable", lambda e: 2)
    assert KeySpec.extractor("tests.keyspec.stable").extract(None) == 1


@pytest.mark.parametrize("name, fn", [("", len), (None, len), (3, len), ("ok.name", "not callable"),
                                      ("has space", len)])
def test_register_extractor_validates(name, fn):
    with pytest.raises(ProtoValidationError):
        register_extractor(name, fn)


def test_extractor_name_validated_on_construction():
    for bad in ("", None, 3, "has space"):
        with pytest.raises(ProtoValidationError):
            KeySpec.extractor(bad)


def test_keyspec_is_a_frozen_value():
    a, b = KeySpec.path("a.b"), KeySpec.path("a.b")
    assert a == b and hash(a) == hash(b)
    assert a != KeySpec.path("a") and KeySpec.element() == KeySpec.element()
    assert KeySpec.extractor("x") != KeySpec.path("x")
    with pytest.raises(dataclasses.FrozenInstanceError):
        a.arg = "c"


def test_direct_construction_is_validated():
    assert KeySpec("path", "a") == KeySpec.path("a")
    for kind, arg in (("bogus", "a"), ("element", "a"), ("path", None), ("extractor", "")):
        with pytest.raises(ProtoValidationError):
            KeySpec(kind, arg)


@pytest.mark.parametrize("spec", [KeySpec.path("a.b"), KeySpec.element(), KeySpec.extractor("e.f")])
def test_plain_data_round_trip(spec):
    data = spec.to_data()
    assert isinstance(data, tuple) and all(x is None or isinstance(x, str) for x in data)
    assert KeySpec.from_data(data) == spec
    assert KeySpec.from_data(list(data)) == spec  # msgpack may hand back a list


@pytest.mark.parametrize("data", [None, "path", ("path",), ("path", "a", "b"), ("bogus", "a"), ("path", "")])
def test_malformed_data_is_corruption(data):
    with pytest.raises(ProtoCorruptionError):
        KeySpec.from_data(data)


def test_term_paths():
    assert KeySpec.path("a.b").term_path == "a.b"
    assert KeySpec.element().term_path is None
    assert KeySpec.extractor("f").term_path == "@f"
    for spec in (KeySpec.path("a.b"), KeySpec.element(), KeySpec.extractor("f")):
        assert KeySpec.for_term_path(spec.term_path) == spec
    with pytest.raises(ProtoValidationError):
        KeySpec.for_term_path(3)


def test_describe():
    assert KeySpec.path("a.b").describe() == "a.b"
    assert KeySpec.element().describe() == "<element>"
    assert KeySpec.extractor("f").describe() == "@f"
