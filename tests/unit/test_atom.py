"""Unit tests for proto_db.core.atom and proto_db.core.dbobject."""
import dataclasses
import threading
import uuid

import pytest

from proto_db.core.atom import Atom, AtomPointer, PersistenceContext, register_atom_class
from proto_db.core.dbobject import DBObject
from proto_db.errors import (ProtoCorruptionError, ProtoUserError, ProtoValidationError)
from tests.support.dict_storage import DictStorage
from tests.support.persistence import DirectContext


class Leaf(Atom):
    """Atom with one persisted attribute."""

    def __init__(self, value, *, context=None):
        super().__init__(context=context)
        self.value = value


class Node(Atom):
    """Atom with children."""

    def __init__(self, left, right, *, context=None):
        super().__init__(context=context)
        self.left = left
        self.right = right
        self._cache = "runtime only"


class Renamed(Atom):
    """Atom with an explicit stable type name."""

    __type_name__ = "tests.custom.Renamed"


def ctx():
    return DirectContext(DictStorage())


# --- AtomPointer -----------------------------------------------------------------

def test_atom_pointer_frozen_and_hashable():
    seg = uuid.uuid4()
    p = AtomPointer(seg, 7)
    assert p == AtomPointer(seg, 7) and hash(p) == hash(AtomPointer(seg, 7))
    assert {p: 1}[AtomPointer(seg, 7)] == 1
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.offset = 3


@pytest.mark.parametrize("seg, off", [("abc", 0), (uuid.uuid4(), -1), (uuid.uuid4(), 1.0),
                                      (uuid.uuid4(), True)])
def test_atom_pointer_validates(seg, off):
    with pytest.raises(ProtoValidationError):
        AtomPointer(seg, off)


# --- registry ------------------------------------------------------------------

def test_default_type_name_and_registry():
    assert Leaf.__type_name__ == f"{__name__}.Leaf"
    assert Renamed.__type_name__ == "tests.custom.Renamed"
    assert Atom._class_for(Leaf.__type_name__) is Leaf


def test_subclass_does_not_inherit_parent_type_name():
    class Child(Leaf):
        __type_name__ = "tests.custom.ChildOfLeaf"
    assert Child.__type_name__ != Leaf.__type_name__


def test_duplicate_type_name_raises():
    with pytest.raises(ProtoUserError):
        class Clash(Atom):
            __type_name__ = "tests.custom.Renamed"


def test_register_same_class_twice_is_idempotent():
    register_atom_class(Leaf)
    assert Atom._class_for(Leaf.__type_name__) is Leaf


def test_unknown_type_name_raises_corruption():
    with pytest.raises(ProtoCorruptionError):
        Atom._class_for("no.such.Type")


# --- identity ----------------------------------------------------------------------

def test_oid_is_uuid4_and_distinct():
    a, b = Leaf(1), Leaf(1)
    assert isinstance(a.oid, uuid.UUID) and a.oid.version == 4
    assert a.oid != b.oid and a != b
    assert a.atom_pointer is None and a.context is None


def test_equality_and_hash_by_oid():
    a = Leaf(1)
    clone = Leaf(2)
    object.__setattr__(clone, "oid", a.oid)
    assert a == clone and hash(a) == hash(clone)
    assert a != "x"


def test_state_holds_public_attributes_only():
    leaf = Leaf(1)
    n = Node(leaf, None)
    assert n._state() == {"left": leaf, "right": None}


def test_persistence_context_protocol():
    assert isinstance(ctx(), PersistenceContext)
    assert not isinstance(object(), PersistenceContext)


def test_a_user_context_may_have_an_attribute_named_saved():
    """Review 8.3i M1: the save hook of the decoded-atom cache is private (``_atom_saved``); a
    user PersistenceContext with its own ``saved`` attribute (here a counter) saves normally."""
    class CountingContext(DirectContext):
        def __init__(self, storage):
            super().__init__(storage)
            self.saved = 0

        def save(self, atom):
            self.saved += 1
            return super().save(atom)

    c = CountingContext(DictStorage())
    root = Node(Leaf(1), Leaf(2), context=c)
    ptr = root._save()
    assert ptr == root.atom_pointer and c.saved == 2           # the two children, through c.save


def test_construction_rejects_a_context_that_is_not_one_every_time():
    """The context check is remembered per class (Task 8.3i: it cost most of a node's
    construction), but an object that is not a context is still refused, and so is an instance of
    an accepted class that lacks ``storage``."""
    for _ in range(3):
        assert Leaf(1, context=ctx()).context is not None
        for bad in ("x", object(), 42):
            with pytest.raises(ProtoValidationError):
                Leaf(1, context=bad)
    broken = ctx()
    del broken.storage
    with pytest.raises(ProtoValidationError):
        Leaf(1, context=broken)
    assert Leaf(1, context=None).context is None


# --- save ------------------------------------------------------------------------

def test_save_children_first_and_once():
    c = ctx()
    shared = Leaf("shared")
    root = Node(Node(shared, Leaf("x")), shared, context=c)
    ptr = root._save()
    assert root.atom_pointer == ptr
    payloads = c.storage.payloads_in_order()
    assert len(payloads) == 4  # shared counted once
    assert payloads[-1]["oid"] == root.oid.bytes
    assert payloads[0]["oid"] == shared.oid.bytes
    assert all(set(p) == {"t", "oid", "s"} for p in payloads)
    assert payloads[-1]["t"] == Node.__type_name__
    assert root._save() == ptr and c.storage.push_count == 4
    assert shared.context is c  # adopted by the saving context


def test_save_without_context_raises():
    with pytest.raises(ProtoUserError):
        Leaf(1)._save()


def test_concurrent_save_produces_one_pointer():
    c = ctx()
    for _ in range(20):
        atom = Node(Leaf(1), Leaf(2), context=c)
        before = c.storage.push_count
        barrier = threading.Barrier(16)
        results = []

        def worker():
            barrier.wait()
            results.append(atom._save())

        threads = [threading.Thread(target=worker) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(set(results)) == 1
        assert c.storage.push_count - before == 3


# --- lazy load -------------------------------------------------------------------

def _persist_tree(c):
    leaf = Leaf({"k": [1, 2]})
    root = Node(leaf, Leaf(None), context=c)
    return root, root._save()


def test_stub_loads_on_first_public_access():
    c = ctx()
    root, ptr = _persist_tree(c)
    gets = c.storage.get_count
    stub = c.load(ptr, Node.__type_name__)
    assert type(stub) is Node
    assert stub.atom_pointer == ptr and stub.context is c
    assert c.storage.get_count == gets
    left = stub.left
    assert c.storage.get_count == gets + 1
    assert type(left) is Leaf and c.storage.get_count == gets + 1  # child is a stub too
    assert left.value == {"k": [1, 2]}
    assert c.storage.get_count == gets + 2
    assert stub.right.value is None
    assert stub == root and left == root.left


def test_underscore_and_pointer_access_do_not_load():
    c = ctx()
    _, ptr = _persist_tree(c)
    stub = c.load(ptr, Node.__type_name__)
    gets = c.storage.get_count
    _ = stub.atom_pointer, stub.context, repr(stub)
    with pytest.raises(AttributeError):
        _ = stub._not_there
    assert c.storage.get_count == gets


def test_oid_access_loads_stub():
    c = ctx()
    root, ptr = _persist_tree(c)
    stub = c.load(ptr, Node.__type_name__)
    gets = c.storage.get_count
    assert stub.oid == root.oid
    assert c.storage.get_count == gets + 1


def test_runtime_attributes_are_not_persisted():
    c = ctx()
    _, ptr = _persist_tree(c)
    stub = c.load(ptr, Node.__type_name__)
    with pytest.raises(AttributeError):
        _ = stub._cache
    with pytest.raises(AttributeError):
        _ = stub.missing_attribute


def test_saving_a_stub_does_not_load_it():
    c = ctx()
    _, ptr = _persist_tree(c)
    stub = c.load(ptr, Node.__type_name__)
    gets = c.storage.get_count
    parent = Node(stub, None, context=c)
    parent._save()
    assert c.storage.get_count == gets


def test_load_type_mismatch_raises_corruption():
    c = ctx()
    _, ptr = _persist_tree(c)
    stub = c.load(ptr, Leaf.__type_name__)
    with pytest.raises(ProtoCorruptionError):
        _ = stub.value


def test_load_malformed_payload_raises_corruption():
    c = ctx()
    ptr = c.storage.push_atom({"t": Leaf.__type_name__, "oid": b"short", "s": {}})
    with pytest.raises(ProtoCorruptionError):
        _ = c.load(ptr, Leaf.__type_name__).value


def test_concurrent_lazy_load_never_torn():
    c = ctx()
    width = 40
    obj = DBObject(context=c, **{f"f{i}": i for i in range(width)})
    ptr = obj._save()
    for _ in range(150):
        stub = c.load(ptr, DBObject.__type_name__)
        gets = c.storage.get_count
        barrier = threading.Barrier(16)
        errors = []

        def worker(offset):
            barrier.wait()
            for j in range(width):
                i = (j + offset) % width
                v = getattr(stub, f"f{i}")
                if v != i:
                    errors.append((i, v))
            if stub.oid != obj.oid or len(stub._state()) != width:
                errors.append("state")

        threads = [threading.Thread(target=worker, args=(k,)) for k in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert c.storage.get_count - gets == 1


def test_getattr_after_concurrent_load_finds_attribute():
    # Deterministic replay of the losing interleaving: the normal lookup failed while
    # the stub was unloaded, then another thread finished the load before __getattr__
    # checked the flag.  __getattr__ must look the attribute up again.
    c = ctx()
    obj = DBObject(context=c, f=1)
    stub = c.load(obj._save(), DBObject.__type_name__)
    stub._load()
    assert Atom.__getattr__(stub, "f") == 1
    assert Atom.__getattr__(stub, "oid") == obj.oid


# --- DBObject ----------------------------------------------------------------------

def test_dbobject_absent_reads_none_and_has_defined_attr():
    o = DBObject(name="ann", note=None)
    assert o.name == "ann" and o.note is None and o.age is None
    assert o.has_defined_attr("name") and o.has_defined_attr("note")
    assert not o.has_defined_attr("age")
    assert not o.has_defined_attr("oid") and not o.has_defined_attr("_x")
    assert o._state() == {"name": "ann", "note": None}


def test_dbobject_is_immutable():
    o = DBObject(name="ann")
    with pytest.raises(ProtoUserError):
        o.name = "bob"
    with pytest.raises(ProtoUserError):
        o.age = 3
    with pytest.raises(ProtoUserError):
        del o.name
    with pytest.raises(ProtoUserError):
        o.oid = uuid.uuid4()
    assert o.name == "ann"


def test_dbobject_with_returns_new_version_with_new_oid():
    c = ctx()
    o = DBObject(context=c, name="ann", age=3)
    o2 = o.with_(age=4, city="x", name=None)
    assert type(o2) is DBObject and o2.oid != o.oid and o2 != o
    assert (o.name, o.age, o.city) == ("ann", 3, None)
    assert (o2.name, o2.age, o2.city) == (None, 4, "x")
    assert o2.has_defined_attr("name")
    assert o2.context is c and o2.atom_pointer is None


def test_dbobject_with_on_subclass_keeps_class():
    o = Person(name="z")
    assert type(o.with_(name="y")) is Person


class Person(DBObject):
    """DBObject subclass used to check with_ keeps the class."""


@pytest.mark.parametrize("name", ["oid", "context", "atom_pointer", "_private"])
def test_dbobject_rejects_reserved_field_names(name):
    with pytest.raises(ProtoValidationError):
        DBObject(**{name: 1})
    with pytest.raises(ProtoValidationError):
        DBObject(a=1).with_(**{name: 1})


def test_dbobject_rejects_unsupported_values():
    with pytest.raises(ProtoValidationError):
        DBObject(x=object())


def test_dbobject_canonicalizes_values():
    o = DBObject(b=bytearray(b"x"))
    assert type(o.b) is bytes


def test_dbobject_persist_and_lazy_reload():
    c = ctx()
    child = DBObject(n=1)
    o = DBObject(context=c, child=child, items=[child, (2, "x")], gone=None)
    ptr = o._save()
    stub = c.load(ptr, DBObject.__type_name__)
    assert stub.has_defined_attr("gone") and stub.gone is None
    assert not stub.has_defined_attr("absent") and stub.absent is None
    assert stub.child == child and stub.child.n == 1
    assert stub.items[0] == child and stub.items[1] == (2, "x")
    assert stub == o and hash(stub) == hash(o)
    with pytest.raises(ProtoUserError):
        stub.child = None


class Account(DBObject):
    """DBObject subclass with a read-only property and a method."""

    @property
    def label(self):
        return f"#{self.number}"

    def close(self):
        return self.with_(closed=True)


@pytest.mark.parametrize("name", ["with_", "has_defined_attr"])
def test_dbobject_rejects_names_shadowing_class_attributes(name):
    with pytest.raises(ProtoValidationError):
        DBObject(**{name: 1})
    with pytest.raises(ProtoValidationError):
        DBObject(a=1).with_(**{name: 1})


@pytest.mark.parametrize("name", ["label", "close"])
def test_subclass_rejects_names_shadowing_its_properties_and_methods(name):
    with pytest.raises(ProtoValidationError):
        Account(**{name: 1})
    assert DBObject(**{name: 1}).has_defined_attr(name)  # fine on the base class
    assert Account(number=7).label == "#7"


@pytest.mark.parametrize("name", ["label", "with_"])
def test_load_of_shadowing_name_raises_corruption(name):
    c = ctx()
    state = {"number": 1, name: 2}
    ptr = c.storage.push_atom({"t": Account.__type_name__, "oid": uuid.uuid4().bytes, "s": state})
    stub = c.load(ptr, Account.__type_name__)
    with pytest.raises(ProtoCorruptionError):
        _ = stub.number


class Shadowed(Atom):
    """Plain atom whose persisted attribute shadows a class-level default."""

    height = 0

    def __init__(self, height, *, context=None):
        super().__init__(context=context)
        self.height = height


def test_save_rejects_instance_attribute_shadowing_class_attribute():
    with pytest.raises(ProtoValidationError):
        Shadowed(3, context=ctx())._save()


# --- lazy identity (D21) -------------------------------------------------------------

def test_oid_is_stable_once_observed():
    a = Leaf(1)
    first = a.oid
    assert a.oid is first and a.oid == first
    assert hash(a) == hash(first)


def test_hash_and_eq_assign_a_stable_oid():
    a, b = Leaf(1), Leaf(1)
    h = hash(a)
    assert "oid" in a.__dict__ and hash(a) == h == hash(a.oid)
    assert a != b
    assert "oid" in b.__dict__
    assert a == a


def test_concurrent_first_oid_access_yields_one_oid():
    for _ in range(50):
        atom = Leaf(1)
        barrier = threading.Barrier(16)
        seen = []

        def worker():
            barrier.wait()
            seen.append(atom.oid)

        threads = [threading.Thread(target=worker) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(seen) == 16 and len(set(seen)) == 1 and atom.oid == seen[0]


def test_saved_payload_carries_observed_oid():
    c = ctx()
    a = Leaf(1, context=c)
    observed = a.oid
    a._save()
    assert c.storage.payloads_in_order()[-1]["oid"] == observed.bytes
    assert a.oid == observed


def test_save_assigns_oid_of_unobserved_atom():
    c = ctx()
    child = Leaf("c")
    root = Node(child, None, context=c)
    assert "oid" not in root.__dict__ and "oid" not in child.__dict__
    ptr = root._save()
    payloads = c.storage.payloads_in_order()
    assert payloads[0]["oid"] == child.oid.bytes
    assert payloads[-1]["oid"] == root.oid.bytes
    stub = c.load(ptr, Node.__type_name__)
    assert stub == root and hash(stub) == hash(root) and stub.left == child


def test_fresh_atoms_never_equal():
    atoms = [Leaf(1) for _ in range(1000)]
    assert len(set(atoms)) == 1000
    assert len({a.oid for a in atoms}) == 1000


def test_dbobject_versions_get_lazy_distinct_oids():
    o = DBObject(a=1)
    o2 = o.with_(a=2)
    assert "oid" not in o2.__dict__
    assert o2 != o and o2.oid != o.oid
