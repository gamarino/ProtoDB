"""Semantic classes on the rc4 registry (spec section 4, rulings R259, R280; Task 4).

Catches: a class without semantic elements changed in any way (its namespace, its records); a semantic
class whose records gain parents or lose their type name; semantic elements left in the Python class
(where they would shadow later modules) or missing from the registry chain; a stored field read through
its descriptor; an absent field that resolves to anything but its declared default (or None); an intent
that runs off a session, or whose context leaks out of it; ``_super()`` from a subtype's intent not
reaching the supertype's implementation; subtype instances that do not see the supertypes' chains (an
extension of the supertype by another module included); class-creation refusals that are silent or that
leave the type name taken.
"""
from __future__ import annotations

import datetime
import hashlib
import os
import uuid
from decimal import Decimal
from unittest import mock

import msgpack
import pytest

from proto_db.core import codec
from proto_db.core.atom import type_tag
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.registry import DESCRIPTOR, METHOD, Registry, using
from proto_db.semantic import IntentContext, Relationship, intent_context
from proto_db.semantic.classes import _invoke
from proto_db.semantic.descriptors import Field
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.persistence import DirectContext
from tests.unit.semantic_modules import MAPPING

pytestmark = [pytest.mark.timeout(120)]

from docs.kit.semantic.acme_sales import Order, OrderLine  # noqa: E402  (after the mapping's package)
from tests.unit.semantic_modules.acme_express import ExpressOrder  # noqa: E402
from tests.unit.semantic_modules.acme_rush import RushOrder  # noqa: E402


def build(*names):
    return Registry.build(("base", "acme_sales") + names, mapping=MAPPING)


# -- a class without semantic elements is untouched (S5; preflight F37) ------------------------------

class PlainNote(DBObject):
    """The non-semantic workload's class: fields, a data attribute and a method, no semantic element."""

    __type_name__ = "tests.semantic.PlainNote"
    kind = "note"

    def title(self):
        return f"note {self.number}"


#: The class namespace of ``PlainNote`` on the base commit b99285b, without the names the interpreter's
#: version adds (``__firstlineno__``, ``__static_attributes__``) or that differ by place (``__doc__``).
_PLAIN_KEYS_ON_THE_BASE = ["__module__", "__type_name__", "_custom_restore", "_legacy_attrs", "_legacy_held",
                           "_type_name_declared", "kind", "title"]
#: The SHA-256 of the segment and root files :func:`_plain_workload` writes, computed once on the base
#: commit b99285b (rc4 with the plan's rulings, before any semantic code) by the same workload.
_PLAIN_DIGEST_ON_THE_BASE = "3bd9bfd95645996ec94bbe108a5391d9060da55c461bdd9bb7c2b07ab14420d4"


def _plain_workload(directory) -> str:
    state = {"n": 0, "clock": datetime.datetime(2026, 11, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)}

    def uuid4():
        state["n"] += 1
        return uuid.UUID(int=(0x5EED << 112) | state["n"], version=4)

    def now():
        state["clock"] += datetime.timedelta(seconds=1)
        return state["clock"]

    with mock.patch("uuid.uuid4", uuid4), mock.patch("proto_db.transactions._utc_now", now):
        space = ObjectSpace(FileStorage(str(directory)))
        db = space.new_database("db")

        def fill(tx):
            tx.set_root_object("notes", PlainNote(number=1, text="first"))
            tx.set_root_object("plain", DBObject(n=2, tags=["a", "b"]))

        def update(tx):
            tx.set_root_object("notes", tx.get_root_object("notes").with_(number=2), replace=True)

        db.run(fill)
        db.run(update)
        space.close()
    digest = hashlib.sha256()
    for name in sorted(os.listdir(directory)):
        if name == "space.root" or name.endswith(".seg"):
            digest.update(name.encode())
            digest.update((directory / name).read_bytes())
    return digest.hexdigest()


def test_a_class_without_semantic_elements_is_untouched(tmp_path):
    keys = sorted(k for k in PlainNote.__dict__ if k not in ("__firstlineno__", "__static_attributes__", "__doc__"))
    assert keys == _PLAIN_KEYS_ON_THE_BASE
    assert PlainNote._implicit_parent is None and not hasattr(PlainNote, "describe")
    assert PlainNote(number=3).title() == "note 3" and PlainNote(number=3).missing is None
    assert _plain_workload(tmp_path / "space") == _PLAIN_DIGEST_ON_THE_BASE


class PlainLink(Relationship):
    """A subclass of ``Relationship`` with no semantic element: a plain class, untouched."""

    __type_name__ = "tests.semantic.PlainLink"
    weight = 1

    def heavier(self):
        return self.weight + 1


def test_a_plain_subclass_of_relationship_is_untouched():
    keys = sorted(k for k in PlainLink.__dict__ if k not in ("__firstlineno__", "__static_attributes__", "__doc__"))
    assert keys == ["__module__", "__type_name__", "_custom_restore", "_legacy_attrs", "_legacy_held",
                    "_type_name_declared", "heavier", "weight"]
    assert PlainLink._implicit_parent is None and "describe" not in PlainLink.__dict__
    link = PlainLink(a=1)
    assert link.weight == 1 and link.heavier() == 2 and link.b is None and link.a == 1


# -- records ------------------------------------------------------------------------------------------

class _Recording(MemoryStorage):
    def __init__(self):
        super().__init__()
        self.pushed = []

    def push_atom(self, payload, *, features=frozenset()):
        ptr = super().push_atom(payload, features=features)
        self.pushed.append((ptr, payload, frozenset(features)))
        return ptr


def test_semantic_class_records_keep_their_type_name_and_no_p():
    order = Order(number=7, state="open")
    storage = _Recording()
    order._bind(DirectContext(storage))
    ptr = order._save()
    (_, payload, features), = storage.pushed
    expected = {"t": type_tag("acme.sales.Order"), "oid": order.oid.bytes,
                "s": codec.encode_state({"number": 7, "state": "open"}, lambda a: a.atom_pointer)}
    assert list(payload) == ["t", "oid", "s"] and features == frozenset()       # the format-4 envelope
    assert msgpack.packb(payload, use_bin_type=True) == msgpack.packb(expected, use_bin_type=True)
    assert storage._segment_versions[ptr.segment] == 4
    assert type_tag("acme.sales.Order") == "acme.sales.Order"
    assert order.parents == () and order._persisted_parents() == ()


# -- the elements move into the registry chain ------------------------------------------------------

def test_semantic_elements_move_into_the_registry_chain():
    for name in ("number", "state", "date", "company", "customer", "lines", "total", "add_line", "confirm",
                 "__meaning__", "__label__", "__synonyms__", "__key__"):
        assert name not in Order.__dict__, name
    assert Order._implicit_parent == "acme.sales.Order" and "describe" in Order.__dict__
    chain = build().chain("acme.sales.Order")
    assert [e.module for e in chain] == ["acme_sales"]
    (extension,) = chain
    assert extension.definitions["number"][0] == DESCRIPTOR and isinstance(extension.definitions["number"][1], Field)
    assert extension.definitions["add_line"][0] == METHOD
    assert extension.semantic["__meaning__"].startswith("A customer's order")
    assert extension.semantic["__key__"] == ("company", "number") and extension.semantic["kind"] == "entity"
    assert build().chain("acme.sales.OrderLine")[0].semantic["kind"] == "relationship"
    assert build("acme_rush").parents_of("acme.sales.RushOrder") == ("acme.sales.Order", "base")


def test_stored_field_read_does_not_call_the_descriptor():
    registry = build()
    field = registry.chain("acme.sales.Order")[0].definitions["number"][1]
    with using(registry), mock.patch.object(type(field), "resolve", autospec=True,
                                            side_effect=AssertionError("resolve called")) as resolve:
        order = Order(number=12, state="confirmed")
        assert order.number == 12 and order["number"] == 12 and order.state == "confirmed"
        assert "number" in order
    assert resolve.call_count == 0


def test_absent_field_resolves_to_its_default_or_none():
    with using(build("acme_delivery")):
        order = Order(number=1)
        assert order.state == "open" and order["state"] == "open"
        assert order.date is None and order.delivery_date is None
        assert "delivery_date" in order and "state" in order and "nothing" not in order
        assert order.nothing is None
        assert order.total == 0                         # a sum over no lines
        line = OrderLine(quantity=Decimal("2"), price=Decimal("1.25"))
        assert line.amount == Decimal("2.50")


# -- the expression reader follows F15 (review I1, I2) -------------------------------------------------

def _reads():
    from tests.unit.semantic_modules import acme_reads
    return acme_reads, Registry.build(("base", "acme_reads"), mapping=MAPPING)


def test_a_computed_reads_an_rc4_data_default():
    module, registry = _reads()
    with using(registry):
        thing = module.Thing(a=1)
        assert thing.rate == 3
        assert thing.plus_rate == 4                     # the expression reads what resolution reads
        assert thing.plus_b == 11                       # a Field default, as before
        assert module.Thing(a=1, rate=5).plus_rate == 6     # the stored value first


def test_a_computed_reading_another_computed_sees_missing():
    from proto_db.semantic.descriptors import RegistryReader
    from proto_db.values import MISSING
    module, registry = _reads()
    with using(registry):
        empty = module.Thing()
        reader = RegistryReader()
        assert reader.default(empty, "twice") is MISSING        # never the public read's None
        assert reader.default(empty, "a") is MISSING
        assert reader.default(empty, "b") == 10 and reader.default(empty, "rate") == 3
        assert empty.chained is None and empty.direct is None and empty.twice is None   # None only at o.name
        assert module.Thing(a=2).chained == 5


def test_a_sum_over_members_whose_computed_term_is_missing_skips_them():
    from proto_db.collections.lists import List
    module, registry = _reads()
    with using(registry):
        holder = module.Holder(things=List.from_iterable([module.Thing(a=1), module.Thing(), module.Thing(a=4)]))
        assert holder.total == 10 and holder.count == 3


def test_an_absent_field_needs_the_registry():
    # A semantic instance resolves through its type's chain: without a registry it is an explicit error,
    # never a silent None in place of a declared default.
    with pytest.raises(ProtoNotSupportedError, match="needs a registry"):
        Order(number=1).state


def _python_module(name: str, source: str):
    """A real Python module ``name`` (in ``sys.modules``, so a registry can import it) running ``source``."""
    import sys
    import types
    found = types.ModuleType(name)
    sys.modules[name] = found
    exec(compile(source, f"<{name}>", "exec"), found.__dict__)
    return found


def test_a_redefinition_without_a_default_keeps_the_earlier_default():
    # A module that redefines state's presentation states no default: the lookup passes it and finds
    # acme_sales's "open", as composition keeps the default (spec section 5).
    _python_module("tests_semantic_state_label", _HEADER + "m = module('sem_state_label', depends=('acme_sales',))\n"
                   "class StateLabel(Extension):\n    __extends__ = 'acme.sales.Order'\n"
                   "    state = Field(Enum('open', 'confirmed', 'cancelled'), label='State')\n")
    registry = Registry.build(("base", "acme_sales", "sem_state_label"),
                              mapping={**MAPPING, "sem_state_label": "tests_semantic_state_label"})
    assert [e.module for e in registry.chain("acme.sales.Order")] == ["sem_state_label", "acme_sales"]
    with using(registry):
        order = Order(number=1)
        assert order.state == "open" and order["state"] == "open" and order.state == "open"   # cached position too
        assert "state" in order


# -- intents (R278, preflight F21) --------------------------------------------------------------------

class _Recorder(IntentContext):
    def __init__(self, target):
        super().__init__(target)
        self.calls = []

    def relate(self, type_name, **roles_and_fields):
        self.calls.append(("relate", type_name, roles_and_fields))

    def update(self, handle, **changes):
        self.calls.append(("update", handle, changes))


def test_an_intent_off_a_session_is_refused():
    with using(build()):
        order = Order(number=1)
        with pytest.raises(ProtoUserError, match="SemanticSession"):
            order.confirm()
        with pytest.raises(ProtoUserError, match="SemanticSession"):
            _invoke(order, "confirm", {}, None)
    with pytest.raises(ProtoUserError, match="only while an intent runs"):
        intent_context()


def test_invoke_runs_the_intent_with_its_context():
    context = _Recorder("acme.sales.Order#" + "0" * 32)
    with using(build()):
        order = Order(number=1)
        _invoke(order, "add_line", {"product": "p", "quantity": Decimal(2), "price": Decimal(3)}, context)
        assert context.calls == [("relate", "acme.sales.OrderLine",
                                  {"order": order, "product": "p", "quantity": Decimal(2), "price": Decimal(3)})]
        result = _invoke(order, "confirm", {}, context)
    assert result.state == "confirmed"
    assert context.calls[-1] == ("update", context.target, {"state": "confirmed"})     # a new version updates
    with pytest.raises(ProtoUserError):
        intent_context()                                # the context does not outlive the call


def test_invoke_refuses_an_unknown_intent_and_bad_arguments():
    context = _Recorder("t")
    with using(build()):
        order = Order(number=1)
        with pytest.raises(ProtoValidationError, match="no intent named 'number'"):
            _invoke(order, "number", {}, context)
        with pytest.raises(ProtoValidationError, match="add_line"):
            _invoke(order, "add_line", {"product": "p"}, context)


def test_subclass_intent_super_reaches_the_supertype_implementation():
    context = _Recorder("acme.sales.ExpressOrder#" + "1" * 32)
    with using(build("acme_express")):
        express = ExpressOrder(number=5)
        result = _invoke(express, "confirm", {}, context)
    assert result.state == "confirmed" and result.expedited is True
    assert context.calls == [("update", context.target, {"state": "confirmed", "expedited": True})]


# -- subtypes (R280) ----------------------------------------------------------------------------------

def test_supertype_extension_reaches_the_subtype():
    from proto_db import resolution
    registry = build("acme_delivery", "acme_rush")
    with using(registry):
        rush = RushOrder(number=9, priority=2)
        assert rush._linearization()[1:] == ("acme.sales.RushOrder", "acme.sales.Order", "base")
        assert "delivery_date" in rush and rush.delivery_date is None and rush.state == "open"
        number = next(resolution.definitions(rush, "number"))
        assert number[0] == DESCRIPTOR and number[1].element()["label"] == {"und": "Order no."}
        assert rush.priority == 2 and isinstance(rush, Order)


def test_subclass_presentation_wins_over_supertype_extensions_in_resolution():
    from proto_db import resolution
    with using(build("acme_express", "acme_zlate")):
        express = ExpressOrder(number=1)
        first = next(resolution.definitions(express, "number"))
        assert first[1].element()["label"] == {"und": "Express no."}
        plain = Order(number=1)
        assert next(resolution.definitions(plain, "number"))[1].element()["label"] == {"und": "Late no."}


def test_multiple_semantic_bases_follow_c3_in_resolution():
    from tests.unit.semantic_modules.acme_diamond import D
    registry = Registry.build(("base", "acme_diamond"), mapping=MAPPING)
    with using(registry):
        d = D(z=True)
        assert d._linearization()[1:] == ("acme.diamond.D", "acme.diamond.B", "acme.diamond.C", "acme.diamond.A",
                                          "base")
        assert [c.__name__ for c in D.__mro__[:4]] == ["D", "B", "C", "A"]


# -- describe (R259) ------------------------------------------------------------------------------------

def test_describe_class_method_needs_a_snapshot():
    with pytest.raises(ProtoUserError, match="read snapshot"):
        Order.describe()
    with pytest.raises(ProtoUserError, match="read snapshot"):
        Order.describe(build())
    assert not hasattr(DBObject, "describe")


def test_describe_reads_the_registry_of_the_snapshot():
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    try:
        db = space.new_database("db")
        db.run(lambda tx: tx.install_module("acme_delivery"))
        described = db.run(lambda tx: Order.describe(tx))
        assert [a["name"] for a in described["attributes"]][-1] == "delivery_date"
        snapshot = db.snapshot_at(db.run(lambda tx: tx.snapshot_ref))
        assert Order.describe(snapshot) == described
        empty = space.new_database("empty")
        with pytest.raises(ProtoNotSupportedError, match="no modules"):
            empty.run(lambda tx: Order.describe(tx))
    finally:
        space.close()


# -- class-creation refusals --------------------------------------------------------------------------

def _declare(module_name: str, source: str) -> dict:
    """Run ``source`` as the Python module ``module_name`` (a fresh namespace); return the namespace."""
    namespace = {"__name__": module_name}
    exec(compile(source, f"<{module_name}>", "exec"), namespace)
    return namespace


_HEADER = "from proto_db import DBObject\nfrom proto_db.registry import module\nfrom proto_db.semantic import *\n"


def test_a_semantic_class_needs_a_registry_module():
    source = "class Lonely(DBObject):\n    __type_name__ = 'acme.refused.Lonely'\n    n = Field(int)\n"
    with pytest.raises(ProtoValidationError, match="declares no registry module"):
        _declare("tests_semantic_refused_lonely", _HEADER + source)
    # The refused class left its type name free: a corrected declaration is accepted.
    namespace = _declare("tests_semantic_refused_lonely", _HEADER + "lonely = module('sem_lonely')\n" + source)
    assert namespace["Lonely"]._implicit_parent == "acme.refused.Lonely"


def test_a_semantic_class_chooses_among_several_registry_modules():
    two = _HEADER + "a = module('sem_two_a')\nb = module('sem_two_b')\n"
    with pytest.raises(ProtoValidationError, match="__registry_module__"):
        _declare("tests_semantic_two", two + "class T(DBObject):\n    __type_name__ = 'acme.two.T'\n    n = Field(int)\n")
    with pytest.raises(ProtoValidationError, match="declares no registry module of that name"):
        _declare("tests_semantic_two", two + "class T(DBObject):\n    __type_name__ = 'acme.two.T'\n"
                                             "    __registry_module__ = 'nope'\n    n = Field(int)\n")
    namespace = _declare("tests_semantic_two", two + "class T(DBObject):\n    __type_name__ = 'acme.two.T'\n"
                                                     "    __registry_module__ = 'sem_two_b'\n    n = Field(int)\n")
    assert namespace["b"]._declared[0].registry_name == "acme.two.T" and not namespace["a"]._declared


def test_semantic_elements_need_a_declared_type_name():
    with pytest.raises(ProtoValidationError, match="declares its __type_name__"):
        _declare("tests_semantic_unnamed", _HEADER + "m = module('sem_unnamed')\n"
                                                     "class Unnamed(DBObject):\n    n = Field(int)\n")
    with pytest.raises(ProtoValidationError, match="declares its __type_name__"):
        _declare("tests_semantic_unnamed_sub", _HEADER + "m = module('sem_unnamed_sub')\n"
                 "from docs.kit.semantic.acme_sales import Order\nclass Sub(Order):\n    pass\n")


def test_validity_is_not_supported():
    with pytest.raises(ProtoNotSupportedError, match="D72"):
        _declare("tests_semantic_validity", _HEADER + "m = module('sem_validity')\n"
                 "class Priced(DBObject):\n    __type_name__ = 'acme.refused.Priced'\n    __validity__ = 'x'\n")
    with pytest.raises(ProtoNotSupportedError, match="D72"):
        _declare("tests_semantic_validity_ext", _HEADER + "m = module('sem_validity_ext')\n"
                 "class P(Extension):\n    __extends__ = 'acme.refused.Priced'\n    __validity__ = 'x'\n")
    # The decorator spelling refuses it alike (review M1).
    with pytest.raises(ProtoNotSupportedError, match="D72"):
        _declare("tests_semantic_validity_dec", _HEADER + "m = module('sem_validity_dec')\n"
                 "@m.extends('acme.refused.Priced')\nclass P:\n    __validity__ = 'x'\n")


def test_a_role_outside_a_relationship_is_refused():
    with pytest.raises(ProtoValidationError, match="Relationship"):
        _declare("tests_semantic_role", _HEADER + "m = module('sem_role')\n"
                 "class R(DBObject):\n    __type_name__ = 'acme.refused.R'\n    other = Role('acme.refused.S')\n")


def test_malformed_semantic_dunders_are_refused():
    for body, match in (("__label__ = 3", "__label__"), ("__key__ = 'name'", "__key__"),
                        ("__constraints__ = ['x']", "Invariant"), ("__meaning__ = ''", "__meaning__")):
        with pytest.raises(ProtoValidationError, match=match):
            _declare("tests_semantic_dunders", _HEADER + "m = module('sem_dunders')\n"
                     f"class Bad(DBObject):\n    __type_name__ = 'acme.refused.Bad'\n    {body}\n")


def test_an_extension_has_extension_as_its_only_base_and_names_its_type():
    with pytest.raises(ProtoValidationError, match="__extends__"):
        _declare("tests_semantic_ext1", _HEADER + "m = module('sem_ext1')\nclass E(Extension):\n    n = Field(int)\n")
    with pytest.raises(ProtoValidationError, match="only base"):
        _declare("tests_semantic_ext2", _HEADER + "m = module('sem_ext2')\nclass Mixin:\n    pass\n"
                 "class E(Extension, Mixin):\n    __extends__ = 'acme.x.T'\n")
    with pytest.raises(ProtoValidationError, match="a name starting with '_' is not a definition"):
        _declare("tests_semantic_ext3", _HEADER + "m = module('sem_ext3')\n"
                 "class E(Extension):\n    __extends__ = 'acme.x.T'\n    _private = 1\n")
    with pytest.raises(ProtoValidationError, match="declares no registry module of that name"):
        _declare("tests_semantic_ext4", _HEADER + "from tests.unit.semantic_modules.acme_zlate import late\n"
                 "class E(Extension):\n    __extends__ = 'acme.x.T'\n    __registry_module__ = 'acme_zlate'\n")
    namespace = _declare("tests_semantic_ext5", _HEADER + "m = module('sem_ext5')\n"
                         "class E(Extension):\n    __extends__ = 'acme.x.T'\n    __parents__ = ('acme.x.P',)\n"
                         "    n = Field(int)\n    kind = 'plain data'\n    def helper(self):\n        return 1\n")
    (declared,) = namespace["m"]._declared
    assert declared.parents == ("acme.x.P",)
    assert [(name, kind) for name, (kind, _) in declared.definitions.items()] == [
        ("n", DESCRIPTOR), ("kind", "data"), ("helper", METHOD)]
    with pytest.raises(ProtoUserError, match="never instantiated"):
        namespace["E"]()


def test_the_decorator_keeps_its_refusals():
    # Preflight F34: only descriptors and the semantic dunders became accepted; every other refusal
    # keeps its message.
    from proto_db.registry import module
    handle = module("sem_decorator_refusals")
    for name in ("_private", "__init__", "__extends__"):
        with pytest.raises(ProtoValidationError, match="a name starting with '_' is not a definition"):
            handle.extends("acme.x.T")(type("C", (), {name: 1}))
    accepted = handle.extends("acme.x.T")(type("C", (), {"__meaning__": "A thing", "n": Field(int)}))
    assert accepted.__name__ == "C"


# -- a refused declaration leaves nothing behind (review M4, M5) ------------------------------------

_SHARED_INTENT = ("from proto_db.semantic import intent\n"
                  "@intent(meaning='Close it')\ndef close(self):\n    return self.with_(closed=True)\n")


def test_a_refused_semantic_class_changes_nothing_and_is_never_registered():
    from proto_db.core import atom
    registered = []
    real = atom.register_atom_class

    def recording(cls):
        registered.append(cls.__type_name__)
        return real(cls)

    with mock.patch.object(atom, "register_atom_class", recording):
        with pytest.raises(ProtoValidationError, match="Relationship"):
            _declare("tests_semantic_refused_m4", _HEADER + _SHARED_INTENT + "m = module('sem_refused_m4')\n"
                     "class R(DBObject):\n    __type_name__ = 'acme.refused.M4'\n    finish = close\n"
                     "    other = Role('acme.refused.S')\n")
    assert "acme.refused.M4" not in registered          # checked before registration: no window
    with pytest.raises(Exception):
        atom.Atom._class_for("acme.refused.M4")


def test_a_refused_declaration_does_not_name_its_intents():
    # The intent function of a refused declaration is not marked with the refused name: a later, correct
    # declaration may use it under another name.
    for refused in ("class R(DBObject):\n    __type_name__ = 'acme.refused.M4b'\n    finish = close\n"
                    "    other = Role('acme.refused.S')\n",
                    "class E(Extension):\n    __extends__ = 'base'\n    __parents__ = ('acme.x.P',)\n"
                    "    finish = close\n"):         # refused by the registry, after the semantic checks
        namespace = {"__name__": "tests_semantic_m4b"}
        exec(compile(_HEADER + _SHARED_INTENT + "m = module('sem_m4b')\n", "<m4b>", "exec"), namespace)
        with pytest.raises(ProtoValidationError):
            exec(compile(refused, "<m4b>", "exec"), namespace)
        assert namespace["close"].__proto_intent__.name is None
        assert not namespace["m"]._declared
        exec(compile("class Ok(Extension):\n    __extends__ = 'acme.refused.M4b'\n    close = close\n",
                     "<m4b>", "exec"), namespace)
        assert namespace["close"].__proto_intent__.name == "close"
