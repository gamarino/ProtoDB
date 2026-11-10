"""Stable, language-neutral atom type names (ruling R83, decision D57).

Catches: a new atom class persisted under the Python ``module.QualifiedName`` default (a name
another language cannot produce and a refactor silently changes); a declared name that is
not a plain dotted name; data written under an old name becoming unreadable after a rename;
an alias resolving to a second identity for one stored atom; the built-in names drifting.
"""
import uuid

import pytest

from proto_db.core.atom import (BUILTIN_TYPE_ALIASES, Atom, AtomPointer, type_name_of, type_tag)
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoUserError, ProtoValidationError
from tests.support.dict_storage import DictStorage
from tests.support.persistence import DirectContext


class Undeclared(Atom):
    """No ``__type_name__`` in its body: readable under its legacy name, never written."""

    def __init__(self, value, *, context=None):
        super().__init__(context=context)
        self.value = value


class Declared(Atom):
    __type_name__ = "tests.names.Declared"

    def __init__(self, value, *, context=None):
        super().__init__(context=context)
        self.value = value


class Renamed(DBObject):
    __type_name__ = "tests.names.Renamed"
    __type_aliases__ = ("tests.unit.test_type_names.OldRenamed", "tests.names.Older")


def _write_raw(storage, tag, state, oid=None):
    """Push an atom payload with the given type tag, as an older writer would have."""
    return storage.push_atom({"t": tag, "oid": (oid or uuid.uuid4()).bytes, "s": state})


def test_undeclared_class_keeps_its_legacy_name_for_reading():
    assert Undeclared.__type_name__ == f"{__name__}.Undeclared"
    assert Atom._class_for(Undeclared.__type_name__) is Undeclared


def test_undeclared_class_cannot_be_saved():
    ctx = DirectContext(DictStorage())
    with pytest.raises(ProtoValidationError, match="__type_name__"):
        ctx.save(Undeclared(1))


def test_undeclared_child_fails_before_the_parent_is_written():
    storage = DictStorage()
    ctx = DirectContext(storage)
    parent = Declared(Undeclared(1))
    with pytest.raises(ProtoValidationError, match="Undeclared"):
        ctx.save(parent)
    assert parent.atom_pointer is None


def test_data_written_under_a_legacy_default_name_still_loads():
    storage = DictStorage()
    ptr = _write_raw(storage, Undeclared.__type_name__, {"value": 7})
    stub = DirectContext(storage).load(ptr, type_name_of(Undeclared.__type_name__))
    assert type(stub) is Undeclared and stub.value == 7


def test_declared_class_saves_and_loads_under_its_name():
    storage = DictStorage()
    ctx = DirectContext(storage)
    ptr = ctx.save(Declared("x"))
    assert storage.get_atom(ptr)["t"] == "tests.names.Declared"
    assert ctx.load(ptr, "tests.names.Declared").value == "x"


@pytest.mark.parametrize("name", ["", "Plain", "a..b", ".a.b", "a.b.", "a.1b", "a.b-c", "a b.c", "a.<locals>.C",
                                  "ñ.b", 5])
def test_declared_name_must_be_a_dotted_identifier_name(name):
    with pytest.raises(ProtoUserError):
        type("Bad", (Atom,), {"__type_name__": name})


def test_aliases_read_as_the_declared_name():
    storage = DictStorage()
    ctx = DirectContext(storage)
    old = _write_raw(storage, "tests.unit.test_type_names.OldRenamed", {"a": 1})
    older = _write_raw(storage, "tests.names.Older", {"a": 2})
    assert type_name_of("tests.names.Older") == "tests.names.Renamed"
    assert Atom._class_for("tests.names.Older") is Renamed
    for ptr, value in ((old, 1), (older, 2)):
        stub = ctx.load(ptr, type_name_of(storage.get_atom(ptr)["t"]))
        assert type(stub) is Renamed and stub.a == value
    new = stub.with_(a=3)
    ptr = ctx.save(new)
    assert storage.get_atom(ptr)["t"] == "tests.names.Renamed"


def test_alias_stub_loads_when_asked_by_the_old_name():
    storage = DictStorage()
    ptr = _write_raw(storage, "tests.names.Older", {"a": 5})
    stub = DirectContext(storage).load(ptr, "tests.names.Older")
    assert type(stub) is Renamed and stub.a == 5


def test_alias_clashing_with_a_registered_name_raises():
    with pytest.raises(ProtoUserError):
        type("Clash", (Atom,), {"__type_name__": "tests.names.Clash", "__type_aliases__": ("tests.names.Declared",)})
    with pytest.raises(ProtoUserError):
        type("Clash2", (Atom,), {"__type_name__": "tests.names.Older"})


@pytest.mark.parametrize("aliases", ["tests.names.X", ("",), (5,), ("tests.names.Self",)])
def test_malformed_aliases_raise(aliases):
    with pytest.raises(ProtoUserError):
        type("BadAliases", (Atom,), {"__type_name__": "tests.names.Self", "__type_aliases__": aliases})


def test_aliases_are_not_inherited():
    class Child(Renamed):
        __type_name__ = "tests.names.RenamedChild"
    assert Atom._class_for("tests.names.Older") is Renamed
    assert type_name_of("tests.names.Older") == "tests.names.Renamed"


def test_every_builtin_atom_type_declares_its_name_in_its_own_body():
    for name in BUILTIN_TYPE_ALIASES:
        cls = Atom._class_for(name)
        assert cls.__dict__.get("__type_name__") == name, cls


def test_builtin_type_aliases_are_pinned():
    """Append-only (D51): the format specification and the conformance vectors publish this table."""
    assert BUILTIN_TYPE_ALIASES == (
        "proto_db.avl.KeyNode", "proto_db.avl.PosNode", "proto_db.avl.ValueBox",
        "proto_db.collections.IndexSet", "proto_db.collections.IndexedCollection",
        "proto_db.collections.List", "proto_db.collections.Dictionary",
        "proto_db.collections.RepeatedKeysDictionary", "proto_db.collections.HashDictionary",
        "proto_db.collections.sets._HashBuckets", "proto_db.collections.Set", "proto_db.collections.CountedSet",
        "proto_db.indexing.Index", "proto_db.indexing.keyed.KeyGroupIndex", "proto_db.indexing.EqualityIndex",
        "proto_db.indexing.RangeIndex", "proto_db.indexing.VectorIndex", "proto_db.transactions.SpaceRoot",
        "proto_db.transactions.DatabaseRoot", "proto_db.core.dbobject.DBObject", "proto_db.avl._Node",
        "proto_db.avl.CountedKeyNode")
    assert type_tag("proto_db.avl.CountedKeyNode") == 21
    assert type_tag("tests.names.Declared") == "tests.names.Declared"


def test_pointer_reference_to_an_aliased_type_resolves_to_one_identity():
    """A reference written with the old name and one with the new name give one shared atom."""
    from proto_db.core import codec
    storage = DictStorage()
    ctx = DirectContext(storage)
    ptr = _write_raw(storage, "tests.names.Older", {"a": 9})
    seen = []
    codec.decode_value({"@ref": [ptr.segment.bytes, ptr.offset, "tests.names.Older"]},
                       lambda p, t: seen.append(t) or ctx.load(p, t))
    assert seen == ["tests.names.Renamed"]
    assert isinstance(ptr, AtomPointer)
