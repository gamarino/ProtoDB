"""A database shared with implementations in other languages (Task 10.P, requests A3 and A4).

Catches: a record of a type this process has no class for refused as corruption although another
implementation wrote it legitimately (A3); such a record rewritten, changed or silently treated as
another type; an accessor rule that differs between the spec and the Python implementation (A4).
"""
from __future__ import annotations

import uuid

import msgpack
import pytest

from proto_db.collections.dictionaries import Dictionary
from proto_db.core import codec
from proto_db.core.atom import OpaqueAtom
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.persistence import DirectContext

ORDER = "polyglot.st.Order"


def _push_order(storage, **fields):
    """An atom record of the type ``polyglot.st.Order``, which no Python class implements."""
    state = {"number": 7, "lines": ["a", "b"], **fields}
    return storage.push_atom({"t": ORDER, "oid": uuid.UUID(int=99).bytes,
                              "s": codec.encode_state(state, lambda a: a.atom_pointer)})


# -- A3: unknown type names are preserved, never refused --------------------------------------

def test_a_record_of_an_unknown_type_loads_as_an_opaque_atom():
    storage = MemoryStorage()
    ptr = _push_order(storage)
    atom = DirectContext(storage).load(ptr, ORDER)
    assert isinstance(atom, OpaqueAtom)
    assert type(atom).__type_name__ == ORDER
    assert atom.number == 7 and list(atom.lines) == ["a", "b"]
    assert atom.oid == uuid.UUID(int=99)


def test_an_opaque_atom_reencodes_to_the_bytes_it_was_read_from():
    storage = MemoryStorage()
    ptr = _push_order(storage, total=None, tags={"k": (1, "x")})
    atom = DirectContext(storage).load(ptr, ORDER)
    payload = storage.get_atom(ptr)
    again = codec.encode_state(atom._state(), lambda a: a.atom_pointer)
    assert msgpack.packb(again, use_bin_type=True) == msgpack.packb(payload["s"], use_bin_type=True)


def test_behaviour_of_an_opaque_atom_is_an_explicit_error_naming_the_type():
    storage = MemoryStorage()
    atom = DirectContext(storage).load(_push_order(storage), ORDER)
    with pytest.raises(ProtoNotSupportedError, match=ORDER):
        atom.close()                                   # a method only the Smalltalk class has
    with pytest.raises(ProtoNotSupportedError, match=ORDER):
        atom.number = 8
    with pytest.raises(ProtoNotSupportedError, match=ORDER):
        type(atom)()


def test_an_opaque_atom_used_as_a_collection_is_an_explicit_error():
    storage = MemoryStorage()
    ptr = _push_order(storage)
    with pytest.raises(ProtoNotSupportedError, match=ORDER):
        Dictionary._stub(DirectContext(storage), ptr, ORDER)


@pytest.mark.parametrize("name", ["proto_db.collections.NoSuchType", "nodots", "1bad.Name"])
def test_unknown_names_in_the_reserved_namespace_or_outside_the_grammar_are_corruption(name):
    storage = MemoryStorage()
    ptr = storage.push_atom({"t": name, "oid": uuid.uuid4().bytes, "s": {}})
    with pytest.raises(ProtoCorruptionError):
        DirectContext(storage).load(ptr, name)


def test_a_class_registered_later_does_not_clash_with_its_opaque_stand_in():
    storage = MemoryStorage()
    DirectContext(storage).load(_push_order(storage), ORDER).number     # creates the stand-in class

    class Late(DBObject):
        __type_name__ = "polyglot.st.LateOrder"
    other = storage.push_atom({"t": "polyglot.st.LateOrder", "oid": uuid.uuid4().bytes, "s": {"x": 1}})
    assert type(DirectContext(storage).load(other, "polyglot.st.LateOrder")) is Late


def test_an_opaque_atom_survives_a_rewrite_of_what_references_it(tmp_path):
    path = str(tmp_path / "db")
    space = ObjectSpace(FileStorage(path))
    db = space.new_database("d")
    with db.new_transaction() as tr:
        tr.set_root_object("holder", DBObject(name="h"))
    space.close()
    # Another implementation adds an Order and makes the holder reference it.
    space = ObjectSpace(FileStorage(path))
    db = space.open_database("d")
    with db.new_transaction() as tr:
        holder = tr.get_root_object("holder")
        ptr = _push_order(space.storage)
        space.storage.flush()
        order = tr.load(ptr, ORDER)
        tr.set_root_object("holder", holder.with_(order=order))
    space.close()
    # This implementation changes the holder; the Order is kept as written.
    space = ObjectSpace(FileStorage(path))
    db = space.open_database("d")
    with db.new_transaction() as tr:
        holder = tr.get_root_object("holder")
        before = holder.order.atom_pointer
        tr.set_root_object("holder", holder.with_(name="changed"))
    space.close()
    space = ObjectSpace(FileStorage(path))
    try:
        holder = space.open_database("d").new_transaction().get_root_object("holder")
        assert holder.name == "changed"
        assert holder.order.atom_pointer == before and holder.order.number == 7
        assert type(holder.order).__type_name__ == ORDER
    finally:
        space.close()


# -- A4: accessors -------------------------------------------------------------------------------

class Invoice(DBObject):
    __type_name__ = "tests.polyglot.Invoice"

    @property
    def total(self):                      # an accessor in other languages' sense
        return self.__dict__["total"]


def test_python_refuses_a_class_attribute_named_like_a_persisted_field_even_an_accessor():
    # In Python a class attribute hides the stored field (a property is a data descriptor): the
    # field is its own accessor, so the class must define nothing of that name.
    with pytest.raises(ProtoValidationError, match="accessor"):
        Invoice(total=3)
    storage = MemoryStorage()
    ptr = storage.push_atom({"t": Invoice.__type_name__, "oid": uuid.uuid4().bytes, "s": {"n": 1, "total": 3}})
    with pytest.raises(ProtoCorruptionError, match="accessor"):
        DirectContext(storage).load(ptr, Invoice.__type_name__).n


# -- A1: names reserved for additive extensions --------------------------------------------------

@pytest.mark.parametrize("name", ["__currentModules", "__currentSymbols", "__x"])
def test_root_names_starting_with_two_underscores_are_reserved(tmp_path, name):
    space = ObjectSpace(FileStorage(str(tmp_path / "db")))
    try:
        db = space.new_database("d")
        with pytest.raises(ProtoValidationError, match="reserved"):
            with db.new_transaction() as tr:
                tr.set_root_object(name, 1)
        with db.new_transaction() as tr:
            tr.set_root_object("_one_underscore", 1)            # not reserved
    finally:
        space.close()
