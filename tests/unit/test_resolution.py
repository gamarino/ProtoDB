"""Resolution over object parents, without a registry (runtime registry, Task 4).

Catches: an inherited attribute found in the wrong order (not C3), a diamond node visited twice,
an inconsistent hierarchy resolved silently, ``o.x`` and ``o["x"]`` disagreeing, a miss that raises
instead of returning ``None`` (R150), ``in`` unable to tell an absent name from an explicit
``None``, a write that reaches a parent, resolution hiding a Python member, a linearization
recomputed on every lookup or cached under the wrong key, a cost that grows with history depth,
two stubs of one oid visited as two nodes (R159), a parent cycle that recurses forever (R160), an
opaque parent that stops the lookup (R161), a registry name that raises before the lookup reaches
it, and indexes that start seeing inherited fields.
"""
from __future__ import annotations

import uuid

import pytest

from proto_db import resolution
from proto_db.core.atom import type_tag
from proto_db.core.dbobject import DBObject
from proto_db.errors import (ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError,
                             ProtoValidationError)
from proto_db.indexing.keyspec import MISSING, KeySpec
from proto_db.resolution import linearize, resolve
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import FrozenDict
from tests.support.persistence import DirectContext

pytestmark = [pytest.mark.timeout(120)]

DBOBJECT = DBObject.__type_name__
_T = type_tag(DBOBJECT)


class _CountingLookups:
    """Counts the node probes resolution makes (the frames it walks), by node identity."""

    def __init__(self, monkeypatch):
        self.visited = []
        real = resolution._lookup

        def counting(node, name):
            self.visited.append(node)
            return real(node, name)
        monkeypatch.setattr(resolution, "_lookup", counting)


# -- order --------------------------------------------------------------------------------------

def _hierarchy():
    c = DBObject(c="C", shared="C")
    a = DBObject(parents=(c,), a="A")
    b = DBObject(parents=(c,), b="B", shared="B")
    o = DBObject(parents=(a, b), own="O")
    return o, a, b, c


def test_own_then_parents_in_c3_order():
    o, a, b, c = _hierarchy()
    lin = o._linearization()
    assert [id(n) for n in lin] == [id(o), id(a), id(b), id(c)]
    assert [id(n) for n in linearize(o)] == [id(n) for n in lin]
    assert resolve(o, "own") == ("O", 0)
    assert resolve(o, "a") == ("A", 1)
    assert resolve(o, "b") == ("B", 2)
    assert resolve(o, "c") == ("C", 3)
    assert o.shared == "B"                          # B comes before C (C3), although A is first
    assert resolve(o, "missing") == (None, None)
    assert linearize(DBObject(n=1))[0].n == 1 and len(linearize(DBObject(n=1))) == 1


def test_c3_order_matches_python_mro():
    """The classic C3 example (Python's own MRO): the same hierarchy as classes and as parents."""
    shape = {"A": (), "B": (), "C": (), "D": (), "E": (), "K1": ("A", "B", "C"), "K2": ("D", "B", "E"),
             "K3": ("D", "A"), "Z": ("K1", "K2", "K3")}
    classes, objects = {}, {}
    for name, bases in shape.items():
        classes[name] = type(name, tuple(classes[b] for b in bases) or (object,), {})
        objects[name] = DBObject(parents=tuple(objects[b] for b in bases), label=name)
    expected = [k.__name__ for k in classes["Z"].__mro__[:-1]]
    assert [n.label for n in objects["Z"]._linearization()] == expected


def test_diamond_visits_each_node_once(monkeypatch):
    o, a, b, c = _hierarchy()
    counter = _CountingLookups(monkeypatch)
    assert o.nowhere is None
    assert sorted(map(id, counter.visited)) == sorted(map(id, (a, b, c)))   # o's own probe is apart
    assert len(o._linearization()) == 4


def test_inconsistent_hierarchy_raises():
    a = DBObject(n="a")
    b = DBObject(n="b")
    x = DBObject(parents=(a, b), n="x")
    y = DBObject(parents=(b, a), n="y")
    z = DBObject(parents=(x, y), n="z")
    with pytest.raises(ProtoValidationError) as e:
        z.missing
    assert str(a.oid) in str(e.value) and str(b.oid) in str(e.value)
    with pytest.raises(ProtoValidationError):
        linearize(z)
    with pytest.raises(ProtoValidationError):
        z["missing"]
    assert z.n == "z" and z["n"] == "z"            # an own hit needs no linearization


# -- the two forms ------------------------------------------------------------------------------

def test_item_and_attribute_agree():
    o, a, b, c = _hierarchy()
    for name in ("own", "a", "b", "c", "shared", "missing"):
        assert o[name] == getattr(o, name)
    field = "".join(["sha", "red"])                 # a name computed at run time
    assert o[field] == o.shared == "B"
    plain = DBObject(n=1)
    assert plain["n"] == plain.n == 1


def test_miss_returns_none_in_both_forms():
    o, *_ = _hierarchy()
    plain = DBObject(n=1)
    for obj in (o, plain):
        assert obj.nothing_here is None
        assert obj["nothing_here"] is None
        assert resolve(obj, "nothing_here") == (None, None)


class _WithMethod(DBObject):
    __type_name__ = "tests.resolution.WithMethod"

    def greet(self):
        return f"hello {self.name}"


def test_in_tells_absent_from_explicit_none():
    parent = DBObject(inherited_none=None, inherited=1)
    o = DBObject(parents=(parent,), own_none=None)
    for name in ("own_none", "inherited_none"):
        assert getattr(o, name) is None and o[name] is None
        assert name in o
    assert "inherited" in o and "absent" not in o and o.absent is None
    m = _WithMethod(name="x")
    assert "greet" in m and m["greet"]() == "hello x" == m.greet()
    child = DBObject(parents=(m,))
    assert "greet" not in child                     # a node contributes its own attributes only
    assert child.name == "x"
    for odd in (1, None, "", "_private", "__class__", "oid", "with_", "has_defined_attr"):
        assert odd not in o


def test_writes_never_reach_a_parent():
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")

    def fill(tx):
        parent = DBObject(x=1, y=2)
        tx.set_root_object("parent", parent)
        child = DBObject(parents=(parent,))
        assert child.x == 1
        newer = child.with_(x=10)
        assert newer.x == 10 and parent.x == 1 and child.x == 1
        assert newer._own == {"x": 10} and child._own == {}
        with pytest.raises(ProtoUserError):
            child.x = 5
        with pytest.raises(ProtoUserError):
            child["x"] = 5
        with pytest.raises(ProtoUserError):
            del child["x"]
        tx.set_root_object("child", newer)
    db.run(fill)

    def check(tx):
        parent, child = tx.get_root_object("parent"), tx.get_root_object("child")
        assert parent._own == {"x": 1, "y": 2} and child._own == {"x": 10}
        assert child.x == 10 and child.y == 2 and parent.x == 1
    db.run(check)
    space._atoms._unload_all()
    db.run(check)
    space.close()


class _Labelled(DBObject):
    __type_name__ = "tests.resolution.Labelled"

    @property
    def label(self):
        return "from python"

    def describe(self):
        return "method"


def test_python_subclass_members_come_first(monkeypatch):
    parent = DBObject(label="from parent", describe="data", other="inherited")
    o = _Labelled(parents=(parent,))
    counter = _CountingLookups(monkeypatch)
    assert o.label == "from python" and o["label"] == "from python"
    assert o.describe() == "method" and o["describe"]() == "method"
    assert "label" in o and "describe" in o
    assert counter.visited == []                    # resolution never ran for a Python member
    assert o.other == "inherited" and o["other"] == "inherited"
    assert not o.has_defined_attr("other")          # own-only, as before


# -- cache --------------------------------------------------------------------------------------

def test_linearization_is_cached_by_pointer(monkeypatch):
    calls = []
    real = resolution._compute

    def counting(parents, registry):
        calls.append(parents)
        return real(parents, registry)
    monkeypatch.setattr(resolution, "_compute", counting)

    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")

    def fill(tx):
        base = DBObject(kind="base", tag="b")
        tx.set_root_object("one", DBObject(parents=(base, "acme.Base"), n=1))
        tx.set_root_object("two", DBObject(parents=(base, "acme.Base"), n=2))
    db.run(fill)

    def read(tx):
        one, two = tx.get_root_object("one"), tx.get_root_object("two")
        assert one.parents is not two.parents      # two records, two decoded tuples
        assert one.kind == "base" and two.kind == "base" and one.tag == "b"
    db.run(read)
    space._atoms._unload_all()
    db.run(read)
    assert len(calls) == 1                          # one computation for the parent pointers
    space.close()

    # Unsaved parents: cached on the transaction, for that tuple object.
    calls.clear()
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    tx = db.new_transaction()
    unsaved = DBObject(kind="unsaved")
    first = DBObject(context=tx, parents=(unsaved,), n=1)
    second = first.with_(n=2)                       # the same parents tuple object
    assert first.kind == "unsaved" and second.kind == "unsaved" and first.kind == "unsaved"
    assert len(calls) == 1
    other = DBObject(context=tx, parents=(unsaved,), n=3)   # another tuple object
    assert other.kind == "unsaved"
    assert len(calls) == 2
    tx.abort()
    space.close()


def test_without_the_atom_cache_saved_parents_are_cached_on_the_transaction(monkeypatch):
    calls = []
    real = resolution._compute
    monkeypatch.setattr(resolution, "_compute", lambda p, r: calls.append(p) or real(p, r))
    space = ObjectSpace(MemoryStorage(), atom_cache_entries=0)
    db = space.new_database("db")

    def fill(tx):
        base = DBObject(kind="base")
        tx.set_root_object("one", DBObject(parents=(base,), n=1))
        tx.set_root_object("two", DBObject(parents=(base,), n=2))
    db.run(fill)

    def read(tx):
        assert tx.get_root_object("one").kind == "base" and tx.get_root_object("two").kind == "base"
    db.run(read)
    assert len(calls) == 1
    db.run(read)                                    # a new transaction, a new cache
    assert len(calls) == 2
    space.close()


def test_cost_does_not_depend_on_history_depth(monkeypatch):
    """A structural count, not a timing: the frames walked are the same for parents with 100
    superseded versions and with 1,000 (anti-goal A4).  Every version keeps its predecessor as an
    attribute, and each version of ``base`` has the current version of ``kind`` as its parent, so
    a resolution that walked history (or old parents) would visit more frames with more depth."""
    def frames(depth):
        space = ObjectSpace(MemoryStorage())
        db = space.new_database("db")

        def start(tx):
            kind = DBObject(kind_version=0)
            tx.set_root_object("kind", kind)
            tx.set_root_object("base", DBObject(parents=(kind,), version=0))
        db.run(start)
        for v in range(1, depth + 1):
            def supersede(tx, v=v):
                kind = tx.get_root_object("kind")
                kind = kind.with_(kind_version=v, previous=kind)
                tx.set_root_object("kind", kind)
                previous = tx.get_root_object("base")
                base = previous.with_(version=v, previous=previous).with_parents(kind)
                tx.set_root_object("base", base)
            db.run(supersede)

        def child(tx):
            tx.set_root_object("child", DBObject(parents=(tx.get_root_object("base"),)))
        db.run(child)
        counter = _CountingLookups(monkeypatch)
        out = {}

        def read(tx):
            c = tx.get_root_object("child")
            assert c.version == depth and c.kind_version == depth and c.missing is None
            out["frames"] = len(counter.visited)
            out["lin"] = len(c._linearization())
        db.run(read)
        space.close()
        return out
    shallow, deep = frames(100), frames(1000)
    # version: 1 frame; kind_version: 2; missing: 2 (base, kind) -- o's own probes are apart.
    assert shallow == deep == {"frames": 5, "lin": 3}


# -- rulings R159, R160, R161 ---------------------------------------------------------------------

class _Records:
    """A storage double whose records are written by hand, at pointers chosen by the test (so
    that two records may reference each other)."""

    def __init__(self):
        self.records = {}
        self.segment = uuid.uuid4()

    def ptr(self, offset):
        from proto_db.core.atom import AtomPointer
        return AtomPointer(self.segment, offset)

    def ref(self, offset, type_name=DBOBJECT):
        return {"@ref": [self.segment.bytes, offset, type_tag(type_name)]}

    def put(self, offset, oid, state, parents=None, type_name=DBOBJECT):
        payload = {"t": type_tag(type_name), "oid": uuid.UUID(int=oid).bytes, "s": state}
        if parents is not None:
            payload["p"] = {"@tup": parents}
        self.records[self.ptr(offset)] = payload

    def get_atom(self, ptr):
        return self.records[ptr]


def test_two_stubs_of_one_oid_are_one_node_the_first_reachable():
    """R159: the node met first in a left-to-right, depth-first walk of the parents is used."""
    rec = _Records()
    rec.put(1, 0xD, {"who": "d1", "only1": 1})
    rec.put(2, 0xD, {"who": "d2", "only2": 2})          # another record of the same oid
    rec.put(3, 0xB, {"b": 1}, [rec.ref(1)])
    rec.put(4, 0xC, {"c": 1}, [rec.ref(2)])
    rec.put(5, 0x0, {}, [rec.ref(3), rec.ref(4)])
    o = DirectContext(rec).load(rec.ptr(5), DBOBJECT)
    lin = o._linearization()
    assert [n.oid.int for n in lin] == [0x0, 0xB, 0xC, 0xD]
    assert lin[3].atom_pointer == rec.ptr(1)
    assert o.who == "d1" and o.only1 == 1 and o.only2 is None and "only2" not in o


def test_one_parent_listing_two_records_of_one_oid_is_one_node():
    """R159, one parent: records written by another writer may list two records of one oid
    (different pointers pass the load check); the linearization holds that node once, the
    first one listed (Task 9 plant ``c3_drops_duplicate_check``)."""
    rec = _Records()
    rec.put(1, 0xD, {"who": "d1", "only1": 1})
    rec.put(2, 0xD, {"who": "d2", "only2": 2})          # another record of the same oid
    rec.put(5, 0x0, {}, [rec.ref(1), rec.ref(2)])
    o = DirectContext(rec).load(rec.ptr(5), DBOBJECT)
    lin = o._linearization()
    assert [n.oid.int for n in lin] == [0x0, 0xD]
    assert lin[1].atom_pointer == rec.ptr(1)
    assert o.who == "d1" and o.only2 is None and "only2" not in o


def test_a_parent_cycle_in_records_is_corruption():
    """R160: possible only in another writer's records; raised naming the oids."""
    rec = _Records()
    rec.put(1, 0xA, {"a": 1}, [rec.ref(2)])
    rec.put(2, 0xB, {"b": 1}, [rec.ref(1)])
    rec.put(3, 0x1, {"own": 1}, [rec.ref(1)])
    o = DirectContext(rec).load(rec.ptr(3), DBOBJECT)
    assert o.own == 1                                   # loading and own hits are unaffected
    with pytest.raises(ProtoCorruptionError) as e:
        o.missing
    assert str(uuid.UUID(int=0xA)) in str(e.value) and str(uuid.UUID(int=0xB)) in str(e.value)
    with pytest.raises(ProtoCorruptionError):
        "missing" in o


def test_an_opaque_parent_contributes_its_fields_and_its_parents():
    """R161: an atom of another language's type gives its own fields as data and its persisted
    parents; no method, and no ProtoNotSupportedError for a name it lacks."""
    rec = _Records()
    rec.put(1, 0xE, {"deep": "from dbobject"})
    rec.put(2, 0xF, {"st_field": 7}, [rec.ref(1)], type_name="polyglot.st.Order")
    rec.put(3, 0x1, {}, [rec.ref(2, "polyglot.st.Order")])
    o = DirectContext(rec).load(rec.ptr(3), DBOBJECT)
    assert o.st_field == 7 and o["st_field"] == 7 and "st_field" in o
    assert o.deep == "from dbobject"
    assert o.missing is None and "missing" not in o
    assert type(o._linearization()[1]).__type_name__ == "polyglot.st.Order"


# -- registry names without a registry --------------------------------------------------------

def test_a_registry_name_without_a_registry_raises_only_when_reached():
    base = DBObject(early="hit")
    o = DBObject(parents=(base, "acme.Base"), own=1)
    assert o.own == 1 and o.early == "hit" and o["early"] == "hit" and "early" in o
    with pytest.raises(ProtoNotSupportedError, match="registry name 'acme.Base' needs a registry"):
        o.late
    with pytest.raises(ProtoNotSupportedError):
        o["late"]
    with pytest.raises(ProtoNotSupportedError):
        "late" in o
    assert [n if type(n) is str else n.oid for n in linearize(o)][1:] == [base.oid, "acme.Base"]


def test_a_registry_name_parent_never_raises_at_load():
    storage = MemoryStorage()
    context = DirectContext(storage)
    o = DBObject(parents=("acme.Base",), n=1)
    o._bind(context)
    loaded = context.load(o._save(), DBOBJECT)
    assert loaded.n == 1 and loaded.parents == ("acme.Base",) and loaded._own == {"n": 1}


# -- own attributes, legacy fields (R157), indexes ---------------------------------------------------

def test_own_is_a_frozen_dict_of_own_attributes_only():
    o, *_ = _hierarchy()
    assert type(o._own) is FrozenDict and o._own == {"own": "O"}
    assert not o.has_defined_attr("a") and o.has_defined_attr("own")
    assert DBObject()._own == {}


def _legacy(storage):
    payload = {"t": _T, "oid": uuid.UUID(int=0x1e9).bytes,
               "s": {"name": "a", "parents": "old", "with_parents": {"@tup": [1, 2]}}}
    return DirectContext(storage).load(storage.push_atom(payload), DBOBJECT)


def test_legacy_fields_read_through_the_item_form():
    o = _legacy(MemoryStorage())
    assert o["parents"] == "old" and o["with_parents"] == (1, 2)
    assert o.parents == () and callable(o.with_parents)
    assert "parents" in o and "with_parents" in o
    assert o._own == {"name": "a", "parents": "old", "with_parents": (1, 2)}
    child = DBObject(parents=(o,))
    assert child["parents"] == "old" and "parents" in child     # inherited like any field
    assert child.parents == (o,)
    assert DBObject(n=1)["parents"] is None and "parents" not in DBObject(n=1)


def test_indexes_read_own_attributes_only():
    parent = DBObject(x=1)
    child = DBObject(parents=(parent,), y=2)
    assert KeySpec.path("x").extract(child) is MISSING
    assert KeySpec.path("y").extract(child) == 2
    assert KeySpec.path("x").extract(parent) == 1
    legacy = _legacy(MemoryStorage())
    assert KeySpec.path("parents").extract(legacy) == "old"


def test_item_names_are_validated():
    o = DBObject(n=1)
    with pytest.raises(TypeError):
        o[1]
    for bad in ("", "_n", "__class__", "oid", "context", "with_", "has_defined_attr"):
        with pytest.raises(ProtoValidationError):
            o[bad]
    with pytest.raises(TypeError):
        iter(o)


# -- review fix round 1 ---------------------------------------------------------------------------

def test_unsaved_parent_entries_never_reach_the_space_cache():
    """I1: a version of a loaded object (its context is the space's shared atom cache) with an
    unsaved parent is not cached there, and keeps no reference to that parent once it ends."""
    import gc
    import weakref

    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    db.run(lambda tx: tx.set_root_object("o", DBObject(n=1)))
    ref = {}

    def use(tx):
        loaded = tx.get_root_object("o")
        unsaved = DBObject(k="unsaved")
        ref["parent"] = weakref.ref(unsaved)
        version = loaded.with_parents(unsaved)
        assert version.context is space._atoms
        assert version.k == "unsaved" and version.with_(m=2).k == "unsaved"
    db.run(use)
    assert len(space._atoms._linearizations) == 0
    gc.collect()
    assert ref["parent"]() is None
    space.close()


def test_unsaved_parent_entries_are_cached_only_on_a_transaction(monkeypatch):
    calls = []
    real = resolution._compute
    monkeypatch.setattr(resolution, "_compute", lambda p, r: calls.append(p) or real(p, r))
    unsaved = DBObject(k=1)
    o = DBObject(context=DirectContext(MemoryStorage()), parents=(unsaved,))
    assert o.k == 1 and o.k == 1
    assert len(calls) == 2                          # no cache outside a transaction


def _unreachable_registry_object():
    """A DBObject whose lookups past its own attributes reach a registry name (no registry), with
    a parent holding data under the names of library protocols."""
    decoy = DBObject(elements="data", as_query_plan="data", plan="data", startswith="data",
                     endswith="data", lower="data", upper="data")
    return DBObject(parents=(decoy, "acme.Base"), n=1), DBObject(parents=("acme.Base",), n=1)


def test_library_protocol_probes_never_go_through_resolution():
    """I2: library code asks the Python type, never resolution, whether a value has a protocol
    member: no ProtoNotSupportedError from the registry name, and inherited data is ignored."""
    from proto_db.query.cache import as_plan
    from proto_db.query.linq import F, from_collection
    from proto_db.query.plans import Unnest
    from proto_db.collections.lists import List

    for obj in _unreachable_registry_object():
        with pytest.raises(ProtoValidationError, match="Expected a QueryPlan"):
            as_plan(obj)
        plan = Unnest(List.from_iterable([DBObject(tags=obj)]).as_query_plan(), "tags")
        with pytest.raises(ProtoValidationError, match="Unnest"):
            list(plan.execute())
        rows = [{"s": obj}]
        for predicate in (F.s.startswith("A"), F.s.endswith("A"), F.s.lower() == "a", F.s.upper() == "A"):
            with pytest.raises(AttributeError):
                from_collection(rows).where(predicate).to_list()
    assert from_collection([{"s": "Abc"}]).where(F.s.startswith("A")).count() == 1
    assert from_collection([{"s": "Abc"}]).where(F.s.lower() == "abc").count() == 1
