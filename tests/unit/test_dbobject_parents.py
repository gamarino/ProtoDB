"""DBObject parents, persisted as the envelope key ``"p"`` (runtime registry, Task 3).

Catches: a record without parents whose bytes change (the base scheme must stay format 4 byte
for byte); parents lost or reordered by a save, a load, a cache twin or an unload; ``with_``
dropping the parents; a parent of a kind the spec does not allow (Q6) accepted; an unsaved parent
saved twice, or after its child; ``parents`` usable as a field name; an envelope with other keys
read instead of refused.
"""
from __future__ import annotations

import uuid

import msgpack
import pytest

from proto_db.collections.lists import List
from proto_db.core import codec
from proto_db.core.atom import SAVE_NESTING, type_tag
from proto_db.core.dbobject import DBObject, MutableObject
from proto_db.errors import ProtoCorruptionError, ProtoValidationError
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.fake_object_store import FakeObjectStore
from tests.support.persistence import DirectContext
from tests.support.tiered_cluster import TieredCluster

pytestmark = [pytest.mark.timeout(120)]

DBOBJECT = DBObject.__type_name__


class _Recording(MemoryStorage):
    """A memory storage that keeps every atom payload pushed, with its declared features."""

    def __init__(self):
        super().__init__()
        self.pushed = []

    def push_atom(self, payload, *, features=frozenset()):
        ptr = super().push_atom(payload, features=features)
        self.pushed.append((ptr, payload, frozenset(features)))
        return ptr


def _save_direct(obj, storage=None):
    storage = storage if storage is not None else _Recording()
    context = DirectContext(storage)
    obj._bind(context)
    return storage, context, obj._save()


# -- the base scheme is unchanged ---------------------------------------------------------------

@pytest.mark.parametrize("make", [lambda: DBObject(a=1, b="x"), lambda: DBObject(parents=(), a=1, b="x"),
                                  lambda: DBObject(parents=("acme.Base",), a=1, b="x").with_parents()])
def test_no_parents_keeps_v4_bytes(make):
    obj = make()
    storage, _, ptr = _save_direct(obj)
    (_, payload, features), = storage.pushed
    today = {"t": type_tag(DBOBJECT), "oid": obj.oid.bytes,
             "s": codec.encode_state({"a": 1, "b": "x"}, lambda a: a.atom_pointer)}
    assert list(payload) == ["t", "oid", "s"] and features == frozenset()
    assert msgpack.packb(payload, use_bin_type=True) == msgpack.packb(today, use_bin_type=True)
    assert storage._segment_versions[ptr.segment] == 4
    assert obj.parents == ()


# -- round trip ---------------------------------------------------------------------------------

def _memory(tmp_path, store):
    return MemoryStorage()


def _file(tmp_path, store):
    return FileStorage(str(tmp_path / "space"), durability="none")


def _cloud(tmp_path, store):
    return CloudStorage(store)


def _fill(tx):
    base = DBObject(kind="base")
    tx.set_root_object("base", base)
    tx.set_root_object("child", DBObject(parents=(base, "acme.sales.Order"), n=1))


def _check(tx):
    child = tx.get_root_object("child")
    base, name = child.parents
    assert isinstance(base, DBObject) and base.kind == "base" and name == "acme.sales.Order"
    assert base == tx.get_root_object("base") and base.atom_pointer == tx.get_root_object("base").atom_pointer
    assert child.n == 1 and not child.has_defined_attr("parents")
    assert child._state() == {"n": 1}


@pytest.mark.parametrize("open_storage", [_memory, _file, _cloud], ids=lambda f: f.__name__.strip("_"))
def test_parents_round_trip_through_every_storage(tmp_path, open_storage):
    store = FakeObjectStore()
    storage = open_storage(tmp_path, store)
    space = ObjectSpace(storage)
    db = space.new_database("db")
    db.run(_fill)
    check = _check
    db.run(check)                                   # through the cache: the saved atom's twin
    space._atoms._unload_all()
    db.run(check)                                   # loaded again from its record
    if open_storage is _memory:
        space.close()
        return
    space.close()
    space = ObjectSpace(open_storage(tmp_path, store))
    try:
        space.open_database("db").run(check)        # a fresh process view: the record itself
    finally:
        space.close()


def test_parents_round_trip_through_a_tiered_cluster_node(tmp_path):
    with TieredCluster(tmp_path, 1) as tc:
        node = tc.ids[0]
        tc.create_database(node)
        tc.commit(node, _fill)
        tc.db(node).run(_check)
        tc.spaces[node]._atoms._unload_all()
        tc.db(node).run(_check)
        tc.kill(node)
        tc.revive(node)
        tc.db(node).run(_check)                     # reopened on the node's local segments


def test_a_parents_record_declares_the_feature_and_encodes_atoms_as_refs():
    base = DBObject(kind="base")
    child = DBObject(parents=(base, "acme.sales.Order"), n=1)
    storage, _, ptr = _save_direct(child)
    (bptr, bpayload, bfeatures), (cptr, payload, features) = storage.pushed
    assert list(bpayload) == ["t", "oid", "s"] and bfeatures == frozenset()
    assert list(payload) == ["t", "oid", "s", "p"] and features == {"parents"}
    assert payload["p"] == {"@tup": [{"@ref": [bptr.segment.bytes, bptr.offset, type_tag(DBOBJECT)]},
                                     "acme.sales.Order"]}
    assert payload["s"] == {"n": 1}
    assert storage._segment_versions[cptr.segment] == 5


def test_parents_survive_a_load_through_a_direct_context():
    base = DBObject(kind="base")
    storage, context, ptr = _save_direct(DBObject(parents=("x.Y", base), n=1))
    loaded = context.load(ptr, DBOBJECT)
    assert loaded.parents[0] == "x.Y" and loaded.parents[1].kind == "base"
    assert loaded.parents[1] == base and loaded.n == 1


# -- with_ and with_parents ---------------------------------------------------------------------

def test_with_keeps_parents():
    base = DBObject(kind="base")
    child = DBObject(parents=(base, "acme.sales.Order"), n=1)
    newer = child.with_(n=2)
    assert newer.parents == child.parents and newer.parents[0] is base
    assert newer.n == 2 and newer != child


def test_with_parents_replaces_them():
    base = DBObject(kind="base")
    child = DBObject(parents=(base,), n=1)
    other = child.with_parents("x.Y", "x.Z")
    assert other.parents == ("x.Y", "x.Z") and other.n == 1 and other != child
    assert child.parents == (base,)
    assert other.with_parents().parents == ()
    assert type(other.parents) is tuple


def test_parents_accepts_a_list_and_keeps_order():
    a, b = DBObject(n=1), DBObject(n=2)
    assert DBObject(parents=[b, "q.R", a]).parents == (b, "q.R", a)


@pytest.mark.parametrize("bad", [1, None, MutableObject("cell"), "", 1.5, b"x.Y", ("x.Y",), List()],
                         ids=["int", "none", "mutable", "empty-str", "float", "bytes", "tuple", "list-atom"])
def test_invalid_parent_types_are_refused(bad):
    with pytest.raises(ProtoValidationError):
        DBObject(parents=(bad,))
    with pytest.raises(ProtoValidationError):
        DBObject(n=1).with_parents(bad)


@pytest.mark.parametrize("bad", ["x.Y", None, 1, {"a": 1}])
def test_parents_must_be_a_tuple_or_list(bad):
    with pytest.raises(ProtoValidationError):
        DBObject(parents=bad)


def test_a_surrogate_parent_name_is_refused():
    with pytest.raises(ProtoValidationError):
        DBObject(parents=("bad\ud800",))


def test_an_object_may_not_be_its_own_parent():
    parent = DBObject(n=1)
    child = DBObject(parents=(parent,))
    object.__setattr__(child, "oid", parent.oid)        # the same identity: its own parent
    with pytest.raises(ProtoValidationError, match="own parent"):
        _save_direct(child)


# -- saving an unsaved parent (Review Focus 1) ----------------------------------------------------

def test_unsaved_parent_is_saved_once_with_the_child():
    storage = _Recording()
    space = ObjectSpace(storage)
    db = space.new_database("db")

    def fill(tx):
        parent = DBObject(kind="parent")
        first = DBObject(parents=(parent, "acme.Base"), n=1)
        second = DBObject(parents=("acme.Other", parent), n=2, ref=parent)
        tx.set_root_object("pair", DBObject(first=first, second=second))
    before = len(storage.pushed)
    db.run(fill)
    written = storage.pushed[before:]
    parents = [(ptr, p) for ptr, p, _ in written if p["s"].get("kind") == "parent"]
    assert len(parents) == 1, "the unsaved parent is written exactly once"
    parent_ptr = parents[0][0]
    order = [ptr for ptr, _, _ in written]
    children = [ptr for ptr, p, _ in written if "p" in p]
    assert len(children) == 2 and all(order.index(parent_ptr) < order.index(c) for c in children)

    def check(tx):
        pair = tx.get_root_object("pair")
        assert pair.first.parents[0].atom_pointer == parent_ptr == pair.second.parents[1].atom_pointer
        assert pair.second.ref.atom_pointer == parent_ptr
        assert [p if isinstance(p, str) else p.kind for p in pair.first.parents] == ["parent", "acme.Base"]
        assert [p if isinstance(p, str) else p.kind for p in pair.second.parents] == ["acme.Other", "parent"]
    db.run(check)
    space._atoms._unload_all()
    db.run(check)
    space.close()


def test_a_chain_of_unsaved_parents_deeper_than_the_save_nesting_saves_in_one_commit():
    storage = _Recording()
    space = ObjectSpace(storage)
    db = space.new_database("db")
    depth = SAVE_NESTING * 3

    def fill(tx):
        obj = DBObject(level=0)
        for level in range(1, depth):
            obj = DBObject(parents=(obj,), level=level)
        tx.set_root_object("top", obj)
    db.run(fill)

    def check(tx):
        obj, level = tx.get_root_object("top"), depth - 1
        while obj.parents:
            assert obj.level == level
            obj, level = obj.parents[0], level - 1
        assert obj.level == 0 and level == 0
    db.run(check)
    levels = [p["s"]["level"] for _, p, _ in storage.pushed if "level" in p["s"]]
    assert sorted(levels) == list(range(depth)), "each version is written once"
    space.close()


# -- reserved name and envelope -----------------------------------------------------------------

def test_parents_is_a_reserved_attribute_name():
    obj = DBObject(n=1)
    for name in ("parents", "with_parents"):
        with pytest.raises(ProtoValidationError):
            obj.with_(**{name: 1})
        assert not obj.has_defined_attr(name)
    with pytest.raises(ProtoValidationError):
        DBObject._from_state({"parents": ("x.Y",)}, None)
    storage = MemoryStorage()
    ptr = storage.push_atom({"t": type_tag(DBOBJECT), "oid": uuid.uuid4().bytes,
                             "s": {"parents": {"@tup": ["x.Y"]}}})
    with pytest.raises(ProtoCorruptionError):
        DirectContext(storage).load(ptr, DBOBJECT).n


def _raw(payload, features=frozenset()):
    storage = MemoryStorage()
    ptr = storage.push_atom(payload, features=features)
    return DirectContext(storage), ptr


_OID = uuid.UUID(int=7).bytes
_T = type_tag(DBOBJECT)


@pytest.mark.parametrize("payload", [
    {"t": _T, "oid": _OID, "s": {}, "x": 1},
    {"t": _T, "oid": _OID, "s": {}, "p": {"@tup": ["x.Y"]}, "x": 1},
    {"t": _T, "oid": _OID, "p": {"@tup": ["x.Y"]}},
    {"t": _T, "oid": _OID},
], ids=["unknown-key", "parents-and-unknown", "no-state", "two-keys"])
def test_a_record_with_unknown_envelope_keys_is_corrupt(payload):
    context, ptr = _raw(payload, {"parents"} if "p" in payload else frozenset())
    with pytest.raises(ProtoCorruptionError):
        context.load(ptr, DBOBJECT).n


@pytest.mark.parametrize("p", [{"@tup": []}, ["x.Y"], {"@tup": [""]}, {"@tup": [1]}, {"@tup": [None]},
                               {"@tup": [{"@mut": "cell"}]}, "x.Y", {"@tup": [{"@tup": ["x.Y"]}]}],
                         ids=["empty", "list", "empty-str", "int", "nil", "mutable", "str", "nested"])
def test_a_malformed_parents_value_is_corrupt(p):
    context, ptr = _raw({"t": _T, "oid": _OID, "s": {}, "p": p}, {"parents"})
    with pytest.raises(ProtoCorruptionError):
        context.load(ptr, DBOBJECT).n


def test_a_parent_reference_to_a_non_dbobject_atom_is_corrupt():
    storage = MemoryStorage()
    context = DirectContext(storage)
    lst = List()
    lst._bind(context)
    lptr = lst._save()
    ref = {"@ref": [lptr.segment.bytes, lptr.offset, type_tag(List.__type_name__)]}
    ptr = storage.push_atom({"t": _T, "oid": _OID, "s": {}, "p": {"@tup": [ref]}}, features={"parents"})
    with pytest.raises(ProtoCorruptionError):
        context.load(ptr, DBOBJECT).n


def test_parents_on_a_built_in_type_other_than_dbobject_are_corrupt():
    storage = MemoryStorage()
    context = DirectContext(storage)
    lst = List()
    lst._bind(context)
    payload = storage.get_atom(lst._save())
    ptr = storage.push_atom(dict(payload, p={"@tup": ["x.Y"]}), features={"parents"})
    with pytest.raises(ProtoCorruptionError):
        list(context.load(ptr, List.__type_name__).elements())


def test_an_opaque_atom_with_parents_loads_and_keeps_them():
    storage = MemoryStorage()
    ptr = storage.push_atom({"t": "polyglot.st.Order", "oid": _OID, "s": {"n": 1}, "p": {"@tup": ["st.Base"]}},
                            features={"parents"})
    atom = DirectContext(storage).load(ptr, "polyglot.st.Order")
    assert atom.n == 1 and atom._probe("_parents") == ("st.Base",)
