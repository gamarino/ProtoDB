"""Resolution through the runtime registry: chains, binding and ``_super`` (Task 6).

Catches: a method found unbound or bound to the wrong receiver; a field and a method resolved by
different rules; ``_super`` skipping, repeating or reordering a definition, or failing to cross
from one registry name into the next; a method that sees an early binding of ``self.x``; a catalog
object searched after its registry name (spec Q1); a data default that wins over a later module
or over the instance; ``_super`` resolving silently from the wrong place when called outside a
method, from a closure, a generator expression or a generator method, or on an object whose
linearization lacks the running extension; a proxy that can be stored as a value; a registry
definition hidden by a Python member of the object's class going unreported; a registry name that
no module extends treated as a miss; and a per-name cache that walks the frames again on a
repeated hit or miss.
"""
from __future__ import annotations

import asyncio
import threading
import types

import pytest

from proto_db import resolution
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.registry import BASE, Registry, current_registry, using
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

_PACKAGE = "tests.unit.registry_modules"
_FIXTURES = ("reg6_base", "reg6_mid", "reg6_top", "reg6_shadow", "reg6_other", "reg6_wrapped", "reg6_wrapped_top",
             "reg6_unwrapped", "reg6_unwrapped_super")
MAPPING = {name: f"{_PACKAGE}.{name}" for name in _FIXTURES}


def build(*names):
    """The registry of ``names``, after ``base`` (ruling R205: every non-empty list starts with it)."""
    return Registry.build((BASE,) + names, mapping=MAPPING)


@pytest.fixture(scope="module")
def registry():
    return build("reg6_base", "reg6_mid", "reg6_top")


@pytest.fixture
def active(registry):
    with using(registry):
        yield registry


class _Counting:
    """Counts the frames a lookup walks (``_lookup``) and the direct reads of a cached position
    (``_read``), by node."""

    def __init__(self, monkeypatch):
        self.walked, self.read = [], []
        walk, read = resolution._lookup, resolution._read

        def walking(node, name):
            self.walked.append(node)
            return walk(node, name)

        def reading(node, name):
            self.read.append(node)
            return read(node, name)
        monkeypatch.setattr(resolution, "_lookup", walking)
        monkeypatch.setattr(resolution, "_read", reading)

    def clear(self):
        self.walked.clear()
        self.read.clear()


# -- methods and fields ---------------------------------------------------------------------------

def test_method_and_field_resolve_alike(active):
    o = DBObject(parents=("reg6.Opp",))
    assert o.stage == "mid-default" and o["stage"] == "mid-default"
    won = o.promote(o.stage)                                    # o.win(o.stage), in the brief
    assert won.stage == "mid-default->won" and won.parents == ("reg6.Opp",)
    assert isinstance(o.promote, types.MethodType) and isinstance(o["promote"], types.MethodType)
    assert "promote" in o and "stage" in o and "absent" not in o
    assert o.absent is None and o["absent"] is None             # a miss is None (R150)
    assert o._resolve("stage") == ("mid-default", (1, 1))       # node 1, extension 1 (reg6_mid)
    assert o._resolve("level") == ("top", (1, 0))
    assert o._resolve("channel") == ("thread", (2, 0))          # the parent name reg6.Thread
    assert o._resolve("absent") == (None, None)
    assert o._linearization() == (o, "reg6.Opp", "reg6.Thread", BASE)       # base last (R204)


def test_self_is_o(active):
    o = DBObject(parents=("reg6.Opp",), n=1)
    assert o.me() is o
    assert o.promote.__self__ is o and o["me"].__self__ is o
    assert o.promote.__func__ is active.chain("reg6.Opp")[1].definitions["promote"][1]


def test_super_walks_three_modules_once_in_load_order(active):
    o = DBObject(parents=("reg6.Opp",))
    # Newest first: top, mid, base; then base's _super crosses into the parent name reg6.Thread,
    # whose own _super is a miss.
    assert o.win([]) == ["top", "mid", "base", "thread"]
    assert o.super_stage() == "mid-default"                     # from top: mid's default first
    assert o.super_items() == ("mid-default", True, False, None)
    # A registry built in another installation order with the same dependencies loads the same way.
    with using(build("reg6_top", "reg6_mid", "reg6_base")):
        assert DBObject(parents=("reg6.Opp",)).win([]) == ["top", "mid", "base", "thread"]


def test_late_binding_inside_methods(active):
    o = DBObject(parents=("reg6.Opp",))
    # describe is defined by reg6_base; it reads self.title and self.level from the start of the
    # linearization: mid's title and top's level win over base's.
    assert o.describe() == "mid title (top)"
    assert o.with_(level="own").describe() == "mid title (own)"
    with using(build("reg6_base")):
        assert o.describe() == "base title (base)"


def test_catalog_object_before_its_registry_name(active):
    catalog = DBObject(parents=("reg6.Opp",), stage="catalog", label="Opportunity")
    o = DBObject(parents=(catalog,))
    assert o._linearization() == (o, catalog, "reg6.Opp", "reg6.Thread", BASE)
    assert o.stage == "catalog" and o._resolve("stage") == ("catalog", 1)
    assert o.level == "top" and o._resolve("level") == ("top", (2, 0))
    assert o.win([]) == ["top", "mid", "base", "thread"]         # methods still bind to o
    assert o.me() is o


def test_data_default_overridden_by_later_module_and_by_instance(active):
    o = DBObject(parents=("reg6.Opp",))
    assert o.stage == "mid-default"                             # reg6_mid loads after reg6_base
    with using(build("reg6_base")):
        assert o.stage == "base-default"
    own = o.with_(stage="own")
    assert own.stage == "own" and own._resolve("stage") == ("own", 0)
    assert own.super_stage() == "mid-default"                   # _super never sees own attributes


# -- _super misuse (Review Focus 3) ---------------------------------------------------------------

def test_super_outside_a_method_raises(active):
    o = DBObject(parents=("reg6.Opp",))
    with pytest.raises(ProtoUserError, match=r"_super\(\).*test_super_outside_a_method_raises"):
        o._super()

    def helper(obj):
        return obj._super()
    with pytest.raises(ProtoUserError, match=r"helper"):
        helper(o)
    with using(None):
        with pytest.raises(ProtoUserError, match=r"no registry"):
            o._super()


def test_super_from_closure_raises(active):
    o = DBObject(parents=("reg6.Opp",))
    with pytest.raises(ProtoUserError, match=r"inner"):
        o.from_closure()


def test_super_from_generator_raises(active):
    o = DBObject(parents=("reg6.Opp",))
    with pytest.raises(ProtoUserError, match=r"genexpr"):
        o.from_genexpr()
    # A generator method is a registry method, and _super() is refused in it by design (R172, R229);
    # coroutine methods are allowed (R176).
    with pytest.raises(ProtoUserError, match=r"a generator: _super\(\) is not available in generators"):
        next(o.generator_method())


def test_super_in_a_coroutine_method_works(active):
    o = DBObject(parents=("reg6.Opp",))
    assert asyncio.run(o.async_win([])) == ["async top", "mid", "base", "thread"]


def test_super_from_a_method_bound_under_another_registry_is_named(active):
    with using(build("reg6_base", "reg6_mid", "reg6_other")):
        o = DBObject(parents=("reg6.Opp",))
        method = o.other_super
        assert method() == "mid-default"
    with pytest.raises(ProtoUserError, match=r"module 'reg6_other'.*bound under another registry"):
        method()


# -- final review I5 (ruling R239): decorated methods -----------------------------------------------

def test_methods_wrapped_with_functools_wraps_are_indexed_by_their_own_code():
    """One ``functools.wraps`` decorator on several methods shares one wrapper code object: the
    registry indexes each definition by the code of ``inspect.unwrap(f)``, so they are accepted,
    and ``_super()`` inside a wrapped body (also under a stack of decorators) resolves."""
    from tests.unit.registry_modules import reg6_wrapped
    registry = build("reg6_wrapped", "reg6_wrapped_top")
    with using(registry):
        o = DBObject(parents=("reg6.Wrapped",))
        del reg6_wrapped.CALLS[:]
        assert (o.first(), o.second()) == ("first", "second")
        assert o.twice() == ["top", "wrapped"]
        assert reg6_wrapped.CALLS == ["first", "second", "twice", "twice", "twice"]
        assert o.first.__self__ is o


def test_a_decorator_without_wraps_on_two_methods_is_refused_naming_functools_wraps():
    with pytest.raises(ProtoValidationError, match=r"code object.*functools\.wraps"):
        build("reg6_unwrapped")


def test_super_inside_a_method_wrapped_without_wraps_names_functools_wraps():
    with using(build("reg6_base", "reg6_unwrapped_super")):
        o = DBObject(parents=("reg6.Opp",))
        with pytest.raises(ProtoUserError, match=r"functools\.wraps"):
            o.title()


def test_super_on_foreign_object_raises(active):
    o = DBObject(parents=("reg6.Opp",))
    for other in (DBObject(n=1), DBObject(parents=("reg6.Thread",)), DBObject(parents=(DBObject(),))):
        with pytest.raises(ProtoUserError, match=r"reg6\.Opp"):
            o.on_other(other)
    # An object that does contain the running extension is not foreign.
    assert o.on_other(DBObject(parents=(DBObject(parents=("reg6.Opp",)),))) == "mid-default"


def test_super_proxy_is_not_a_value(active):
    o = DBObject(parents=("reg6.Opp",))
    with pytest.raises(ProtoValidationError, match=r"Unsupported value type: SuperProxy"):
        o.store_super()


# -- Python members -----------------------------------------------------------------------------

class _Labelled(DBObject):
    __type_name__ = "tests.registry_resolution.Labelled"

    def label(self):
        return "python label"


class _Plain(DBObject):
    __type_name__ = "tests.registry_resolution.Plain"


def test_registry_cannot_shadow_a_python_member():
    with using(build("reg6_shadow")):
        o = _Labelled(parents=("reg6.Shadowed",))
        assert o.label() == "python label"          # Python members come first, no resolution
        with pytest.raises(ProtoValidationError, match=r"_Labelled.*'label'.*reg6_shadow.*reg6\.Shadowed"):
            o.note
        with pytest.raises(ProtoValidationError):
            o["note"]
        # A subclass that defines no such member, and a plain DBObject, resolve normally.
        assert _Plain(parents=("reg6.Shadowed",)).note == "registry"
        assert DBObject(parents=("reg6.Shadowed",)).label() == "registry label"


def test_python_member_check_runs_once_per_class_and_registry(monkeypatch):
    """Without a cached entry (no context) the check is still memoized per (class, registry,
    registry name), so a repeated lookup does not scan the chains again."""
    calls = []
    real = resolution.class_defines
    monkeypatch.setattr(resolution, "class_defines", lambda cls, name: calls.append(name) or real(cls, name))
    with using(build("reg6_shadow")):
        unsaved = DBObject()                    # an unsaved parent and no transaction: no cached entry
        o = _Plain(parents=(unsaved, "reg6.Shadowed"))
        assert o.note == "registry" and calls
        calls.clear()
        again = _Plain(parents=(unsaved, "reg6.Shadowed"))
        assert o.note == "registry" and again.label() == "registry label"
        assert calls == []
    with using(build("reg6_shadow")):          # another registry: checked again
        assert o.note == "registry" and calls


# -- registry names ---------------------------------------------------------------------------------

def test_a_registry_name_no_module_extends_is_an_empty_node(active):
    """A registry name no module extends contributes nothing and has no parents but ``base``
    (ruling R204): the lookup goes on past it, and a miss is None (controller ruling overturning
    R171)."""
    o = DBObject(parents=("reg6.Nobody", "reg6.Opp"))
    assert o._linearization() == (o, "reg6.Nobody", "reg6.Opp", "reg6.Thread", BASE)
    assert o.stage == "mid-default" and o._resolve("stage") == ("mid-default", (2, 1))
    assert o.win([]) == ["top", "mid", "base", "thread"]
    lone = DBObject(parents=("reg6.Nobody",))
    assert lone.late is None and lone["late"] is None and "late" not in lone
    assert lone._linearization() == (lone, "reg6.Nobody", BASE)


def test_unknown_names_flags_names_no_module_extends(active):
    assert active.unknown_names(("reg6.Opp", "reg6.Nobdy", "reg6.Thread", "reg6.Opq")) == (
        "reg6.Nobdy", "reg6.Opq")
    assert active.unknown_names(()) == ()


def test_explicit_registry_and_the_current_one(registry):
    o = DBObject(parents=("reg6.Opp",))
    assert current_registry() is None
    assert resolution.resolve(o, "stage", registry) == ("mid-default", (1, 1))
    with pytest.raises(ProtoNotSupportedError, match=r"needs a registry"):
        o.stage
    with using(registry):
        assert current_registry() is registry and o.stage == "mid-default"
        with using(None):
            assert current_registry() is None
        assert current_registry() is registry
    assert current_registry() is None


def test_using_is_per_context(registry):
    """The current registry is a context variable: a thread started outside ``using`` does not see
    it unless the interpreter copies the context into new threads (``thread_inherit_context``)."""
    import sys
    seen = []
    with using(registry):
        thread = threading.Thread(target=lambda: seen.append(current_registry()))
        thread.start()
        thread.join()
    inherits = bool(getattr(sys.flags, "thread_inherit_context", 0))
    assert seen == [registry if inherits else None]


# -- the per-name position cache (spec section 9) -------------------------------------------------

def test_repeated_hit_and_miss_walk_no_frames(registry, monkeypatch):
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    tx = db.new_transaction()
    try:
        with using(registry):
            catalog = DBObject(label="catalog label")
            o = DBObject(context=tx, parents=(catalog, "reg6.Opp"))
            counter = _Counting(monkeypatch)
            for name, value in (("label", "catalog label"), ("stage", "mid-default"), ("absent", None)):
                counter.clear()
                assert getattr(o, name) == value
                assert counter.walked == [catalog]              # the first lookup walks the frames
                counter.clear()
                assert getattr(o, name) == value and o[name] == value
                assert counter.walked == []                     # the repeat walks none
                # A hit on an object node reads that one node directly; others read nothing.
                assert counter.read == ([catalog, catalog] if name == "label" else [])
            # Another object with the same parents tuple shares the entry, methods included.
            assert o.promote("x").stage == "x->won"
            other = o.with_(n=2)
            counter.clear()
            assert other.stage == "mid-default" and other.absent is None and counter.walked == []
            assert other.promote("y").stage == "y->won" and counter.walked == []
            assert other.promote.__self__ is other
            # The entry is charged its worst case when stored: its nodes plus the position map's cap.
            entry = tx._linearizations.get((id(o.parents), registry))
            assert entry is not None and len(entry.positions) <= resolution.NAMES_PER_ENTRY
            assert tx._linearizations.size_bytes == len(entry.tail) + resolution.NAMES_PER_ENTRY
    finally:
        tx.abort()
        space.close()


def test_positions_are_per_registry(registry, monkeypatch):
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    tx = db.new_transaction()
    try:
        o = DBObject(context=tx, parents=("reg6.Opp",))
        with using(registry):
            assert o.stage == "mid-default"
        with using(build("reg6_base")):
            assert o.stage == "base-default"
        with using(registry):
            assert o.stage == "mid-default"
    finally:
        tx.abort()
        space.close()


def test_saved_parents_resolve_through_the_registry(registry):
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    try:
        def fill(tx):
            catalog = DBObject(parents=("reg6.Opp",), label="Opportunity")
            tx.set_root_object("o", DBObject(parents=(catalog,), n=1))
        db.run(fill)

        def read(tx):
            o = tx.get_root_object("o")
            assert o.label == "Opportunity" and o.stage == "mid-default"
            assert o.win([]) == ["top", "mid", "base", "thread"] and o.me() is o
        with using(registry):
            db.run(read)
            db.run(read)
    finally:
        space.close()
